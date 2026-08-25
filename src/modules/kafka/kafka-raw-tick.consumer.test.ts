import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  commitOffsets: vi.fn().mockResolvedValue(undefined),
  connect: vi.fn().mockResolvedValue(undefined),
  consumer: vi.fn(),
  disconnect: vi.fn().mockResolvedValue(undefined),
  kafka: vi.fn(),
  run: vi.fn().mockResolvedValue(undefined),
  subscribe: vi.fn().mockResolvedValue(undefined),
}));

vi.mock('@confluentinc/kafka-javascript', () => ({
  KafkaJS: {
    Kafka: class {
      constructor(config: unknown) {
        mocks.kafka(config);
      }

      consumer(config: unknown) {
        return mocks.consumer(config);
      }
    },
  },
}));

import {
  KafkaRawTickConsumer,
} from './kafka-raw-tick.consumer';
import { parseRawMarketTick } from '../market-data/raw-market-tick.parser';

const validTick = {
  schemaVersion: 1,
  symbol: 'BTC/USD',
  price: 100_000,
  providerTimestampSec: 1_700_000_000,
  receivedAtMs: 1_700_000_001_000,
  source: 'twelvedata',
};

describe('KafkaRawTickConsumer', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.consumer.mockReturnValue({
      connect: mocks.connect,
      subscribe: mocks.subscribe,
      run: mocks.run,
      commitOffsets: mocks.commitOffsets,
      disconnect: mocks.disconnect,
    });
  });

  it('uses a single-partition manual-commit consumer and passes raw transport metadata to the handler', async () => {
    const handler = vi.fn().mockResolvedValue(undefined);
    const consumer = new KafkaRawTickConsumer(
      ['kafka:19092'],
      'market.raw-ticks',
      'candle-processor-v1',
      'candle-processor',
    );

    await consumer.start(handler);

    expect(mocks.kafka).toHaveBeenCalledWith({
      kafkaJS: {
        brokers: ['kafka:19092'],
        clientId: 'candle-processor',
      },
    });
    expect(mocks.consumer).toHaveBeenCalledWith({
      kafkaJS: {
        groupId: 'candle-processor-v1',
        autoCommit: false,
        allowAutoTopicCreation: false,
        fromBeginning: true,
      },
    });
    expect(mocks.connect).toHaveBeenCalledOnce();
    expect(mocks.subscribe).toHaveBeenCalledWith({
      topics: ['market.raw-ticks'],
    });

    const runConfig = mocks.run.mock.calls[0]?.[0] as {
      partitionsConsumedConcurrently: number;
      eachMessage: (payload: {
        topic: string;
        partition: number;
        message: {
          offset: string;
          value: Buffer | null;
        };
      }) => Promise<void>;
    };

    expect(runConfig.partitionsConsumedConcurrently).toBe(1);

    await runConfig.eachMessage({
      topic: 'market.raw-ticks',
      partition: 0,
      message: {
        offset: '150',
        value: Buffer.from(JSON.stringify(validTick)),
      },
    });

    expect(handler).toHaveBeenCalledWith({
      topic: 'market.raw-ticks',
      partition: 0,
      offset: '150',
      value: Buffer.from(JSON.stringify(validTick)),
    });
  });

  it('commits the caller-selected replay offset unchanged and disconnects cleanly', async () => {
    const consumer = new KafkaRawTickConsumer(
      ['kafka:19092'],
      'market.raw-ticks',
      'candle-processor-v1',
      'candle-processor',
    );

    await consumer.commitOffset({
      topic: 'market.raw-ticks',
      partition: 0,
      offset: '150',
    });
    await consumer.stop();

    expect(mocks.commitOffsets).toHaveBeenCalledWith([
      {
        topic: 'market.raw-ticks',
        partition: 0,
        offset: '150',
      },
    ]);
    expect(mocks.disconnect).toHaveBeenCalledOnce();
  });
});

describe('parseRawMarketTick', () => {
  it('rejects malformed raw log records instead of skipping them', () => {
    const invalidValues: Array<Buffer | null> = [
      null,
      Buffer.from('{invalid json'),
      Buffer.from(JSON.stringify({ ...validTick, schemaVersion: 2 })),
      Buffer.from(JSON.stringify({ ...validTick, price: '100' })),
      Buffer.from(JSON.stringify({ ...validTick, providerTimestampSec: null })),
      Buffer.from(JSON.stringify({ ...validTick, source: 'other' })),
    ];

    for (const value of invalidValues) {
      expect(() => parseRawMarketTick(value)).toThrow();
    }
  });
});
