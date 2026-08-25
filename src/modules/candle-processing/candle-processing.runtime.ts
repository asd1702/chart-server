import type {
  ConsumedRawTick,
  RawTickConsumer,
} from '../kafka/raw-tick.consumer';
import { parseRawMarketTick } from '../market-data/raw-market-tick.parser';
import type { MarketEventPublisher } from '../messaging/pubsub.interface';
import { getErrorMessage, logger } from '../../shared/utils/logger';
import type { Candle } from '../candle/candle.types';
import type { CandleProcessingResult } from './candle-processor';

export interface CandleMessageProcessor {
  process(message: ConsumedRawTick): Promise<CandleProcessingResult>;
}

const runtimeLogger = logger.child({
  subsystem: 'candle-processing-runtime',
});

export class CandleProcessingRuntime {
  constructor(
    private readonly consumer: RawTickConsumer,
    private readonly processor: CandleMessageProcessor,
    private readonly publisher: MarketEventPublisher,
    private readonly onFatalError: (error: Error) => Promise<void>,
  ) {}

  async start(): Promise<void> {
    await this.consumer.start(
      async (record) => {
        try {
          const tick = parseRawMarketTick(record.value);
          const message: ConsumedRawTick = {
            topic: record.topic,
            partition: record.partition,
            offset: record.offset,
            tick,
          };

          const result = await this.processor.process(message);

          if (result.completedCandle) {
            await this.publishCompletedCandleSafely(
              result.completedCandle,
            );
          }
        } catch (error) {
          const normalized = normalizeError(error);

          await this.onFatalError(normalized);

          /*
           * Tests may use a non-terminating fatal callback. Keep rejecting so
           * the consumer never treats this message as successfully handled.
           */
          throw normalized;
        }
      },
    );
  }

  async stop(): Promise<void> {
    await this.consumer.stop();
  }

  private async publishCompletedCandleSafely(
    candle: Candle,
  ): Promise<void> {
    try {
      await this.publisher.publish({
        type: 'candle',
        timeframe: '1m',
        candle,
      });
    } catch (error) {
      runtimeLogger.warn('완료 candle Redis 발행에 실패했습니다.', {
        event: 'completed_candle_publish_failed',
        symbol: candle.symbol,
        time: candle.startTime,
        error: getErrorMessage(error),
      });
    }
  }
}

function normalizeError(error: unknown): Error {
  return error instanceof Error
    ? error
    : new Error(String(error));
}
