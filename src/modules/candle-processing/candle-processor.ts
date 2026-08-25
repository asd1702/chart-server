import { CandleMaker } from '../candle/candle.maker';
import type { CandleRepository } from '../candle/candle.repository';
import type { Candle } from '../candle/candle.types';
import type {
  ConsumedRawTick,
  RawTickOffsetCommitter,
} from '../kafka/raw-tick.consumer';
import { logger } from '../../shared/utils/logger';

const processorLogger = logger.child({
  subsystem: 'candle-processor',
});

type CandleWrite = {
  symbol: string;
  time: Date;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
};

type CandleWriter = Pick<CandleRepository, 'bulkSave1mCandles'>;

export interface CandleProcessingResult {
  completedCandle: Candle | null;
}

/**
 * Builds one symbol's candles from the Kafka raw log without a local state
 * checkpoint. A failure after CandleMaker mutation must be handled by the
 * composition root with a fresh process and replay.
 */
export class CandleProcessor {
  private readonly symbols: ReadonlySet<string>;

  private readonly candleMakers = new Map<string, CandleMaker>();

  /*
   * Each value is the first Kafka offset needed to rebuild that symbol's
   * in-progress candle after a restart. Kafka has one partition-wide cursor,
   * so we commit the oldest of these offsets rather than a boundary message's
   * offset unconditionally.
   */
  private readonly replayStartOffsets = new Map<string, bigint>();

  constructor(
    symbols: readonly string[],
    private readonly repository: CandleWriter,
    private readonly offsetCommitter: RawTickOffsetCommitter,
  ) {
    this.symbols = new Set(symbols);

    if (this.symbols.size === 0) {
      throw new Error('Candle processor requires at least one symbol');
    }
  }

  async process(
    message: ConsumedRawTick,
  ): Promise<CandleProcessingResult> {
    const {
      tick,
      topic,
      partition,
      offset,
    } = message;

    if (!this.symbols.has(tick.symbol)) {
      throw new Error(
        `Unexpected symbol for candle processor: ${tick.symbol}`,
      );
    }

    const replayStartOffset = BigInt(offset);

    if (!this.replayStartOffsets.has(tick.symbol)) {
      this.replayStartOffsets.set(tick.symbol, replayStartOffset);
    }

    const candleMaker = this.getCandleMaker(tick.symbol);
    const completed = candleMaker.update(
      tick.symbol,
      tick.price,
      0,
      tick.providerTimestampSec,
    );

    if (!completed) {
      /*
       * No checkpoint while constructing a minute. Replaying from the last
       * completed-minute boundary reconstructs this in-memory candle.
       */
      return {
        completedCandle: null,
      };
    }

    const candle: CandleWrite = {
      symbol: completed.symbol,
      time: new Date(completed.startTime * 1000),
      open: completed.open,
      high: completed.high,
      low: completed.low,
      close: completed.close,
      volume: completed.volume,
    };

    await this.repository.bulkSave1mCandles([candle]);

    /*
     * The boundary tick completed the previous candle and started this
     * symbol's new current candle, so replay must include this message.
     */
    this.replayStartOffsets.set(tick.symbol, replayStartOffset);

    const safeReplayOffset = this.getSafeReplayOffset();

    await this.offsetCommitter.commitOffset({
      topic,
      partition,
      offset: safeReplayOffset.toString(),
    });

    processorLogger.info('완성된 candle의 replay checkpoint를 기록했습니다.', {
      event: 'candle_offset_committed',
      topic,
      partition,
      offset: safeReplayOffset.toString(),
      boundaryOffset: offset,
      candleStartTime: completed.startTime,
      symbol: completed.symbol,
    });

    return {
      completedCandle: completed,
    };
  }

  private getCandleMaker(symbol: string): CandleMaker {
    const existing = this.candleMakers.get(symbol);

    if (existing) {
      return existing;
    }

    const created = new CandleMaker();
    this.candleMakers.set(symbol, created);

    return created;
  }

  private getSafeReplayOffset(): bigint {
    let safeOffset: bigint | null = null;

    for (const replayStartOffset of this.replayStartOffsets.values()) {
      if (safeOffset === null || replayStartOffset < safeOffset) {
        safeOffset = replayStartOffset;
      }
    }

    if (safeOffset === null) {
      throw new Error('Cannot commit without an initialized replay offset');
    }

    return safeOffset;
  }
}
