import config from '../../config';
import {
  closeCandlePersistence,
  startCandlePersistence,
} from '../candle/candle.persistence';
import { runHistoricalBackfill } from '../market-data/historical-backfill.service';
import {
  connectToTwelveData,
  disconnectFromTwelveData,
} from '../market-data/twelvedata.provider';
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
  private backfillOperation: Promise<void> = Promise.resolve();
  private running = false;

  isRunning(): boolean {
    return this.running;
  }

  async start(): Promise<void> {
    if (this.running) return;

    runtimeLogger.info('Active Ingestion Runtime을 시작합니다.', {
      event: 'active_ingestion_starting',
    });

    const publisher = createRedisPubSubService('publisher');
    this.publisher = publisher;

    try {
      // Durable storage must be ready before an upstream tick can arrive.
      startCandlePersistence();
      connectToTwelveData(publisher);

      if (config.market.historicalBackfillEnabled) {
        this.backfillOperation = runHistoricalBackfill().catch((error) => {
          runtimeLogger.error('과거 데이터 백필에 실패했습니다.', {
            event: 'historical_backfill_failed',
            error: getErrorMessage(error),
          });
        });
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
    if (!this.running && !this.publisher) return;

    runtimeLogger.info('Active Ingestion Runtime을 중지합니다.', {
      event: 'active_ingestion_stopping',
    });

    this.running = false;
    const publisher = this.publisher;
    this.publisher = null;
    let firstError: unknown;

    // Prevent new ticks, then wait for existing handlers before closing RocksDB.
    try {
      await disconnectFromTwelveData();
    } catch (error) {
      firstError ??= error;
    }

    try {
      await this.backfillOperation;
    } catch (error) {
      firstError ??= error;
    } finally {
      this.backfillOperation = Promise.resolve();
    }

    try {
      await closeCandlePersistence();
    } catch (error) {
      firstError ??= error;
    }

    if (publisher) {
      try {
        await publisher.disconnect();
      } catch (error) {
        firstError ??= error;
      }
    }

    if (firstError !== undefined) {
      runtimeLogger.error('Active Ingestion Runtime 종료 중 오류가 발생했습니다.', {
        event: 'active_ingestion_stop_failed',
        error: getErrorMessage(firstError),
      });
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
    this.publisher = null;
    this.backfillOperation = Promise.resolve();

    await disconnectFromTwelveData().catch(() => undefined);
    await closeCandlePersistence().catch(() => undefined);
    await publisher.disconnect().catch(() => undefined);

    runtimeLogger.error('Active Ingestion Runtime 시작에 실패해 부분 시작 상태를 정리했습니다.', {
      event: 'active_ingestion_start_failed',
    });
  }
}
