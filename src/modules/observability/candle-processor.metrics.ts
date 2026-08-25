import {
  collectDefaultMetrics,
  Counter,
  Gauge,
  Histogram,
  Registry,
} from 'prom-client';

export class CandleProcessorMetrics {
  readonly registry: Registry;

  private readonly ticksProcessed: Counter<'symbol'>;
  private readonly candlesCompleted: Counter<'symbol'>;
  private readonly dbWriteDuration: Histogram<'symbol'>;
  private readonly dbWriteFailures: Counter<'symbol'>;
  private readonly offsetCommitFailures: Counter;
  private readonly processedOffset: Gauge<'topic' | 'partition'>;
  private readonly safeReplayOffset: Gauge<'topic' | 'partition'>;
  private readonly replayExposure: Gauge<'topic' | 'partition'>;
  private readonly symbolReplayStartOffset: Gauge<
    'symbol' | 'topic' | 'partition'
  >;

  /*
   * Kafka commit is the only durable replay checkpoint. This in-memory value
   * lets current-minute ticks report exposure without advancing that checkpoint.
   */
  private readonly committedSafeOffsets = new Map<string, bigint>();

  constructor(registry = new Registry()) {
    this.registry = registry;
    collectDefaultMetrics({ register: registry });

    this.ticksProcessed = new Counter({
      name: 'candle_ticks_processed_total',
      help: 'Kafka raw ticks successfully processed by the candle processor',
      labelNames: ['symbol'],
      registers: [registry],
    });
    this.candlesCompleted = new Counter({
      name: 'candle_completed_total',
      help: 'Completed candles with durable DB write and safe offset commit',
      labelNames: ['symbol'],
      registers: [registry],
    });
    this.dbWriteDuration = new Histogram({
      name: 'candle_db_write_duration_seconds',
      help: 'TimescaleDB completed-candle write duration',
      labelNames: ['symbol'],
      buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
      registers: [registry],
    });
    this.dbWriteFailures = new Counter({
      name: 'candle_db_write_failures_total',
      help: 'Completed candle TimescaleDB write failures',
      labelNames: ['symbol'],
      registers: [registry],
    });
    this.offsetCommitFailures = new Counter({
      name: 'candle_offset_commit_failures_total',
      help: 'Kafka safe replay offset commit failures',
      registers: [registry],
    });
    this.processedOffset = this.createOffsetGauge(
      'candle_processed_offset',
      'Most recently application-processed Kafka offset; observational only',
    );
    this.safeReplayOffset = this.createOffsetGauge(
      'candle_safe_replay_offset',
      'Oldest replay offset needed to rebuild all initialized symbol state',
    );
    this.replayExposure = this.createOffsetGauge(
      'candle_replay_exposure',
      'Records that may replay after a process replacement',
    );
    this.symbolReplayStartOffset = new Gauge({
      name: 'candle_symbol_replay_start_offset',
      help: 'Per-symbol current candle replay start offset; observational only',
      labelNames: ['symbol', 'topic', 'partition'],
      registers: [registry],
    });
  }

  startDbWrite(symbol: string): () => void {
    try {
      const end = this.dbWriteDuration.startTimer({ symbol });

      return () => safely(() => {
        end();
      });
    } catch {
      return () => undefined;
    }
  }

  recordDbWriteFailure(symbol: string): void {
    safely(() => this.dbWriteFailures.inc({ symbol }));
  }

  recordOffsetCommitFailure(): void {
    safely(() => this.offsetCommitFailures.inc());
  }

  recordProcessedTick(
    symbol: string,
    topic: string,
    partition: number,
    offset: bigint,
  ): void {
    safely(() => {
      this.ticksProcessed.inc({ symbol });
      this.processedOffset.set(
        { topic, partition: String(partition) },
        observableOffset(offset),
      );

      const safeReplayOffset = this.committedSafeOffsets.get(
        offsetKey(topic, partition),
      );

      if (safeReplayOffset !== undefined) {
        this.replayExposure.set(
          { topic, partition: String(partition) },
          observableOffset(
            offset > safeReplayOffset
              ? offset - safeReplayOffset
              : 0n,
          ),
        );
      }
    });
  }

  recordCommittedBoundary(input: {
    symbol: string;
    topic: string;
    partition: number;
    processedOffset: bigint;
    safeReplayOffset: bigint;
    replayStartOffsets: ReadonlyMap<string, bigint>;
  }): void {
    this.committedSafeOffsets.set(
      offsetKey(input.topic, input.partition),
      input.safeReplayOffset,
    );

    safely(() => {
      const labels = {
        topic: input.topic,
        partition: String(input.partition),
      };

      this.ticksProcessed.inc({ symbol: input.symbol });
      this.candlesCompleted.inc({ symbol: input.symbol });
      this.processedOffset.set(labels, observableOffset(input.processedOffset));
      this.safeReplayOffset.set(labels, observableOffset(input.safeReplayOffset));
      this.replayExposure.set(
        labels,
        observableOffset(
          input.processedOffset > input.safeReplayOffset
            ? input.processedOffset - input.safeReplayOffset
            : 0n,
        ),
      );

      for (const [symbol, replayStartOffset] of input.replayStartOffsets) {
        this.symbolReplayStartOffset.set(
          { ...labels, symbol },
          observableOffset(replayStartOffset),
        );
      }
    });
  }

  recordCurrentReplayStart(
    symbol: string,
    topic: string,
    partition: number,
    offset: bigint,
  ): void {
    safely(() => this.symbolReplayStartOffset.set(
      { symbol, topic, partition: String(partition) },
      observableOffset(offset),
    ));
  }

  private createOffsetGauge(name: string, help: string): Gauge<'topic' | 'partition'> {
    return new Gauge({
      name,
      help,
      labelNames: ['topic', 'partition'],
      registers: [this.registry],
    });
  }
}

function offsetKey(topic: string, partition: number): string {
  return `${topic}:${partition}`;
}

/**
 * Prometheus gauges are IEEE-754 numbers, unlike the bigint values that own
 * replay correctness. They are observability only, not commit checkpoints.
 */
function observableOffset(offset: bigint): number {
  return Number(offset);
}

function safely(action: () => void): void {
  try {
    action();
  } catch {
    // Metrics are strictly best-effort instrumentation.
  }
}
