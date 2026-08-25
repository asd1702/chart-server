import type { RawMarketTick } from '../market-data/raw-market-tick';

export interface ConsumedRawTickRecord {
  topic: string;
  partition: number;
  offset: string;
  value: Buffer | null;
}

export interface ConsumedRawTick {
  topic: string;
  partition: number;
  offset: string;
  tick: RawMarketTick;
}

export interface RawTickOffsetCommitter {
  commitOffset(input: {
    topic: string;
    partition: number;
    offset: string;
  }): Promise<void>;
}

export interface RawTickConsumer extends RawTickOffsetCommitter {
  start(
    handler: (record: ConsumedRawTickRecord) => Promise<void>,
  ): Promise<void>;
  stop(): Promise<void>;
}
