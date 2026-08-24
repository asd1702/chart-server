import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  calls: [] as string[],
  startCandlePersistence: vi.fn(),
  closeCandlePersistence: vi.fn(),
  createRedisPublisher: vi.fn(),
  kafkaStart: vi.fn(),
  kafkaStop: vi.fn(),
  streamStart: vi.fn(),
  streamStop: vi.fn(),
  runHistoricalBackfill: vi.fn(),
  logger: {
    info: vi.fn(),
    error: vi.fn(),
  },
}));

vi.mock('../../config', () => ({
  default: {
    kafka: {
      brokers: ['kafka:19092'],
      rawTicksTopic: 'market.raw-ticks',
      clientId: 'market-feed-ingestor',
    },
    market: {
      streamSymbols: ['BTC/USD'],
      historicalBackfillEnabled: false,
    },
  },
}));

vi.mock('../candle/candle.persistence', () => ({
  startCandlePersistence: mocks.startCandlePersistence,
  closeCandlePersistence: mocks.closeCandlePersistence,
}));

vi.mock('../messaging/pubsub.factory', () => ({
  createRedisPubSubService: mocks.createRedisPublisher,
}));

vi.mock('../kafka/kafka-raw-tick.publisher', () => ({
  KafkaRawTickPublisher: class {
    constructor() {
      mocks.calls.push('kafka-created');
    }

    start = mocks.kafkaStart;
    stop = mocks.kafkaStop;
    publish = vi.fn();
  },
}));

vi.mock('../market-data/twelvedata.provider', () => ({
  TwelveDataStream: class {
    constructor() {
      mocks.calls.push('stream-created');
    }

    start = mocks.streamStart;
    stop = mocks.streamStop;
  },
}));

vi.mock('../market-data/historical-backfill.service', () => ({
  runHistoricalBackfill: mocks.runHistoricalBackfill,
}));

vi.mock('../../shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
  logger: { child: vi.fn(() => mocks.logger) },
}));

import { ActiveIngestionRuntime } from './active-ingestion.runtime';

describe('ActiveIngestionRuntime Kafka lifecycle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.calls.length = 0;

    mocks.startCandlePersistence.mockImplementation(() => {
      mocks.calls.push('candle-persistence-start');
    });
    mocks.closeCandlePersistence.mockImplementation(async () => {
      mocks.calls.push('candle-persistence-stop');
    });
    mocks.kafkaStart.mockImplementation(async () => {
      mocks.calls.push('kafka-start');
    });
    mocks.kafkaStop.mockImplementation(async () => {
      mocks.calls.push('kafka-stop');
    });
    mocks.streamStart.mockImplementation(() => {
      mocks.calls.push('stream-start');
    });
    mocks.streamStop.mockImplementation(async () => {
      mocks.calls.push('stream-stop');
    });
    mocks.runHistoricalBackfill.mockResolvedValue(undefined);
    mocks.createRedisPublisher.mockImplementation(() => {
      mocks.calls.push('redis-created');
      return {
        publish: vi.fn(),
        disconnect: vi.fn(async () => {
          mocks.calls.push('redis-stop');
        }),
      };
    });
  });

  it('connects Kafka before opening TwelveData and disconnects it after stream handlers settle', async () => {
    const runtime = new ActiveIngestionRuntime();

    await runtime.start();
    await runtime.stop();

    expect(mocks.calls).toEqual([
      'redis-created',
      'candle-persistence-start',
      'kafka-created',
      'kafka-start',
      'stream-created',
      'stream-start',
      'stream-stop',
      'kafka-stop',
      'candle-persistence-stop',
      'redis-stop',
    ]);
  });

  it('cleans Kafka, persistence, and Redis when Kafka connection fails', async () => {
    mocks.kafkaStart.mockRejectedValueOnce(new Error('Kafka unavailable'));
    const runtime = new ActiveIngestionRuntime();

    await expect(runtime.start()).rejects.toThrow('Kafka unavailable');

    expect(mocks.calls).toEqual([
      'redis-created',
      'candle-persistence-start',
      'kafka-created',
      'kafka-stop',
      'candle-persistence-stop',
      'redis-stop',
    ]);
    expect(mocks.streamStart).not.toHaveBeenCalled();
  });

});
