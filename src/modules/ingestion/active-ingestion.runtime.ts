import config from '../../config';
import {
  closeCandlePersistence,
  startCandlePersistence,
} from '../candle/candle.persistence';
import { KafkaRawTickPublisher } from '../kafka/kafka-raw-tick.publisher';
import type { RawTickPublisher } from '../kafka/raw-tick.publisher';
import { runHistoricalBackfill } from '../market-data/historical-backfill.service';
import { TwelveDataStream } from '../market-data/twelvedata.provider';
import { createRedisPubSubService } from '../messaging/pubsub.factory';
import type { MarketEventPublisher } from '../messaging/pubsub.interface';
import { getErrorMessage, logger } from '../../shared/utils/logger';

const runtimeLogger = logger.child({
  subsystem: 'active-ingestion-runtime',
});

/**
 * Owns resources used only by the active ingestor. It deliberately has no
 * knowledge of leader election; src/ingestor.ts composes both lifecycles.
 */
export class ActiveIngestionRuntime {
  private publisher: MarketEventPublisher | null = null;

  private rawTickPublisher: RawTickPublisher | null = null;

  private twelveDataStream: TwelveDataStream | null = null;

  private backfillOperation: Promise<void> = Promise.resolve();

  private running = false;

  isRunning(): boolean {
    return this.running;
  }

  async start(): Promise<void> {
    if (this.running) {
      return;
    }

    runtimeLogger.info('Active Ingestion Runtime을 시작합니다.', {
      event: 'active_ingestion_starting',
    });

    const publisher = createRedisPubSubService('publisher');

    this.publisher = publisher;

    try {
      /*
       * 외부 tick을 받기 전에 durable storage가 반드시 준비되어야 한다.
       *
       * TwelveDataStream 내부의 handlePriceUpdate()는
       * 완성 candle을 enqueueCandle() 할 수 있기 때문에,
       * WebSocket을 먼저 열면 RocksDB가 준비되기 전에
       * tick이 도착할 가능성이 생긴다.
       */
      startCandlePersistence();

      /*
       * WebSocket보다 먼저 Kafka producer를 준비한다.
       * TwelveDataStream은 Kafka ACK 전에는 CandleMaker를 전진시키지
       * 않으므로, producer connection 실패 시 upstream을 열지 않는다.
       */
      const rawTickPublisher = new KafkaRawTickPublisher(
        [...config.kafka.brokers],
        config.kafka.rawTicksTopic,
        config.kafka.clientId,
      );

      this.rawTickPublisher = rawTickPublisher;

      await rawTickPublisher.start();

      /*
       * 하나의 Active Leadership Epoch마다
       * 새로운 TwelveDataStream instance를 생성한다.
       *
       * 따라서 CandleMaker, WebSocket, reconnect timer,
       * heartbeat, active message handlers 모두
       * 이전 Epoch와 공유되지 않는다.
       */
      const twelveDataStream = new TwelveDataStream(
        publisher,
        rawTickPublisher,
        {
          symbols: config.market.streamSymbols,
        },
      );

      /*
       * start() 전에 field에 저장한다.
       *
       * start() 도중 예외가 발생하더라도
       * cleanupAfterStartFailure()가 이 instance를 찾아
       * stop()할 수 있게 하기 위함이다.
       */
      this.twelveDataStream = twelveDataStream;

      twelveDataStream.start();

      if (config.market.historicalBackfillEnabled) {
        this.backfillOperation = runHistoricalBackfill().catch(
          (error) => {
            runtimeLogger.error('과거 데이터 백필에 실패했습니다.', {
              event: 'historical_backfill_failed',
              error: getErrorMessage(error),
            });
          },
        );
      } else {
        this.backfillOperation = Promise.resolve();
      }

      this.running = true;

      runtimeLogger.info('Active Ingestion Runtime 시작을 완료했습니다.', {
        event: 'active_ingestion_started',
      });
    } catch (error) {
      await this.cleanupAfterStartFailure(publisher);

      throw error;
    }
  }

  async stop(): Promise<void> {
    if (
      !this.running &&
      !this.publisher &&
      !this.rawTickPublisher &&
      !this.twelveDataStream
    ) {
      return;
    }

    runtimeLogger.info('Active Ingestion Runtime을 중지합니다.', {
      event: 'active_ingestion_stopping',
    });

    this.running = false;

    /*
     * 먼저 현재 Epoch의 resource references를 떼어낸다.
     *
     * 이후 cleanup 중 오류가 발생하더라도
     * runtime object가 이미 종료된 resource를
     * 다시 정상 resource처럼 보유하지 않도록 한다.
     */
    const twelveDataStream = this.twelveDataStream;
    const rawTickPublisher = this.rawTickPublisher;
    const publisher = this.publisher;

    this.twelveDataStream = null;
    this.rawTickPublisher = null;
    this.publisher = null;

    let firstError: unknown;

    /*
     * 1. upstream ingestion을 가장 먼저 중지한다.
     *
     * TwelveDataStream.stop():
     * - reconnect 금지
     * - heartbeat 정지
     * - WebSocket 정리
     * - 이미 실행 중인 message handler settle 대기
     *
     * 이 작업이 끝난 뒤에는 새로운 enqueueCandle() 호출이
     * 발생하지 않아야 한다.
     */
    if (twelveDataStream) {
      try {
        await twelveDataStream.stop();
      } catch (error) {
        firstError ??= error;
      }
    }

    /*
     * 2. 실행 중이던 historical backfill을 기다린다.
     *
     * 아직 cancellation 기능은 없으므로
     * 현재 behavior를 그대로 유지한다.
     */
    try {
      await this.backfillOperation;
    } catch (error) {
      firstError ??= error;
    } finally {
      this.backfillOperation = Promise.resolve();
    }

    /*
     * 3. 모든 active message handler가 settle된 뒤 Kafka producer를
     * 종료한다. send() 중 disconnect되는 race를 방지한다.
     */
    if (rawTickPublisher) {
      try {
        await rawTickPublisher.stop();
      } catch (error) {
        firstError ??= error;
      }
    }

    /*
     * 4. 더 이상 새로운 tick handler가 RocksDB에 접근하지 않는
     * 상태가 된 후 persistence를 닫는다.
     */
    try {
      await closeCandlePersistence();
    } catch (error) {
      firstError ??= error;
    }

    /*
     * 5. 마지막으로 realtime publisher를 종료한다.
     */
    if (publisher) {
      try {
        await publisher.disconnect();
      } catch (error) {
        firstError ??= error;
      }
    }

    if (firstError !== undefined) {
      runtimeLogger.error(
        'Active Ingestion Runtime 종료 중 오류가 발생했습니다.',
        {
          event: 'active_ingestion_stop_failed',
          error: getErrorMessage(firstError),
        },
      );

      throw firstError;
    }

    runtimeLogger.info('Active Ingestion Runtime을 중지했습니다.', {
      event: 'active_ingestion_stopped',
    });
  }

  private async cleanupAfterStartFailure(
    publisher: MarketEventPublisher,
  ): Promise<void> {
    this.running = false;

    const twelveDataStream = this.twelveDataStream;
    const rawTickPublisher = this.rawTickPublisher;

    this.twelveDataStream = null;
    this.rawTickPublisher = null;
    this.publisher = null;
    this.backfillOperation = Promise.resolve();

    /*
     * Stream이 일부라도 시작된 상태라면
     * WebSocket / timer / handler부터 정리한다.
     */
    if (twelveDataStream) {
      await twelveDataStream.stop().catch(() => undefined);
    }

    /*
     * Stream handler가 모두 정리된 뒤 producer를 종료한다.
     * connect()가 실패한 publisher도 best-effort disconnect한다.
     */
    if (rawTickPublisher) {
      await rawTickPublisher.stop().catch(() => undefined);
    }

    /*
     * 그 다음 durable storage를 닫는다.
     */
    await closeCandlePersistence().catch(() => undefined);

    /*
     * 마지막으로 publisher를 종료한다.
     */
    await publisher.disconnect().catch(() => undefined);

    runtimeLogger.error(
      'Active Ingestion Runtime 시작에 실패해 부분 시작 상태를 정리했습니다.',
      {
        event: 'active_ingestion_start_failed',
      },
    );
  }
}
