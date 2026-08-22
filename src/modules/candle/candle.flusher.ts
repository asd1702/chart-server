import { prisma } from '../../shared';
import { getErrorMessage, logger } from '../../shared/utils/logger';
import type { PendingCandleStore } from './storage/pending-candle.store';

interface CandleBatchWriter {
  createMany(args: {
    data: Array<{
      symbol: string;
      time: Date;
      open: number;
      high: number;
      low: number;
      close: number;
      volume: number;
    }>;
    skipDuplicates: boolean;
  }): Promise<unknown>;
}

export interface CandleFlusherOptions {
  batchSize?: number;
  flushInterval?: number;
  writer?: CandleBatchWriter;
}

type FlushFailureState = 'read' | 'write' | 'ack' | null;

export class CandleFlusher {
  private readonly batchSize: number;
  private readonly flushInterval: number;
  private readonly writer: CandleBatchWriter;
  private flushTimer: NodeJS.Timeout | null = null;
  private inFlight: Promise<number> | null = null;
  private enqueuedSinceFlush = 0;
  private stopped = false;
  private failureState: FlushFailureState = null;

  constructor(
    private readonly store: PendingCandleStore,
    options: CandleFlusherOptions = {},
  ) {
    this.batchSize = options.batchSize ?? 500;
    this.flushInterval = options.flushInterval ?? 3000;
    this.writer = options.writer ?? prisma.candle1m;
  }

  start(): void {
    if (this.flushTimer || this.stopped) return;

    this.flushTimer = setInterval(() => void this.flush(), this.flushInterval);
    this.flushTimer.unref();

    // Recover pending data promptly after a restart.
    void this.flush();
    logger.info('캔들 DB 플러셔를 시작했습니다.', {
      subsystem: 'candle-persistence',
      event: 'candle_flusher_started',
      batchSize: this.batchSize,
      flushInterval: this.flushInterval,
    });
  }

  notifyEnqueued(): void {
    this.enqueuedSinceFlush += 1;
    if (this.enqueuedSinceFlush >= this.batchSize) {
      this.enqueuedSinceFlush = 0;
      void this.flush();
    }
  }

  flush(): Promise<number> {
    if (this.stopped || this.inFlight) return Promise.resolve(0);

    const operation = this.flushBatch();
    this.inFlight = operation;
    void operation.finally(() => {
      if (this.inFlight === operation) this.inFlight = null;
    });
    return operation;
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    await this.inFlight;
    logger.info('캔들 DB 플러셔를 중지했습니다.', {
      subsystem: 'candle-persistence',
      event: 'candle_flusher_stopped',
    });
  }

  private async flushBatch(): Promise<number> {
    const startedAt = Date.now();
    let pending;

    try {
      pending = await this.store.peek(this.batchSize);
    } catch (error) {
      this.logFailure('read', 'RocksDB pending 캔들 조회에 실패했습니다.', {
        subsystem: 'candle-persistence',
        event: 'candle_flush_read_failed',
        elapsedMs: Date.now() - startedAt,
        error: getErrorMessage(error),
      });
      return 0;
    }
    if (pending.length === 0) {
      this.logRecovery('read');
      return 0;
    }

    try {
      await this.writer.createMany({
        data: pending.map(({ candle }) => ({
          symbol: candle.symbol,
          time: new Date(candle.startTime * 1000),
          open: candle.open,
          high: candle.high,
          low: candle.low,
          close: candle.close,
          volume: candle.volume,
        })),
        skipDuplicates: true,
      });
    } catch (error) {
      this.logFailure('write', '캔들 DB 플러시에 실패했습니다. RocksDB pending 데이터를 유지합니다.', {
        subsystem: 'candle-persistence',
        event: 'candle_flush_write_failed',
        count: pending.length,
        elapsedMs: Date.now() - startedAt,
        error: getErrorMessage(error),
      });
      return 0;
    }

    try {
      // ACK only after TimescaleDB has accepted the idempotent batch.
      await this.store.ack(pending.map(({ key }) => key));
    } catch (error) {
      this.logFailure('ack', 'RocksDB 캔들 ACK에 실패했습니다. 멱등 재시도를 위해 pending 데이터를 유지합니다.', {
        subsystem: 'candle-persistence',
        event: 'candle_flush_ack_failed',
        count: pending.length,
        elapsedMs: Date.now() - startedAt,
        error: getErrorMessage(error),
      });
      return 0;
    }

    this.logRecovery();

    logger.info('캔들 DB 플러시를 완료했습니다.', {
      subsystem: 'candle-persistence',
      event: 'candle_flush_succeeded',
      count: pending.length,
      elapsedMs: Date.now() - startedAt,
    });
    return pending.length;
  }

  private logFailure(
    failure: Exclude<FlushFailureState, null>,
    message: string,
    metadata: Record<string, unknown>,
  ): void {
    if (this.failureState === failure) return;

    this.failureState = failure;
    logger.error(message, metadata);
  }

  private logRecovery(
    onlyFrom?: Exclude<FlushFailureState, null>,
  ): void {
    const recoveredFrom = this.failureState;
    if (
      recoveredFrom === null
      || (onlyFrom !== undefined && recoveredFrom !== onlyFrom)
    ) {
      return;
    }

    this.failureState = null;
    logger.info('캔들 플러시 파이프라인이 복구되었습니다.', {
      subsystem: 'candle-persistence',
      event: 'candle_flush_recovered',
      recoveredFrom,
    });
  }
}
