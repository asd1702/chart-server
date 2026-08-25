import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  calls: [] as string[],
  createRedisPublisher: vi.fn(),
  kafkaStart: vi.fn(),
  kafkaStop: vi.fn(),
  streamStart: vi.fn(),
  streamStop: vi.fn(),
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

vi.mock('../../shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
  logger: { child: vi.fn(() => mocks.logger) },
}));

import { ActiveIngestionRuntime } from './active-ingestion.runtime';

describe('ActiveIngestionRuntime Kafka lifecycle', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.calls.length = 0;

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

  it('starts Redis, Kafka, and TwelveData then stops them in reverse dependency order', async () => {
    const runtime = new ActiveIngestionRuntime();

    await runtime.start();
    await runtime.stop();

    expect(mocks.calls).toEqual([
      'redis-created',
      'kafka-created',
      'kafka-start',
      'stream-created',
      'stream-start',
      'stream-stop',
      'kafka-stop',
      'redis-stop',
    ]);
  });

  it('cleans Kafka and Redis when Kafka connection fails', async () => {
    mocks.kafkaStart.mockRejectedValueOnce(new Error('Kafka unavailable'));
    const runtime = new ActiveIngestionRuntime();

    await expect(runtime.start()).rejects.toThrow('Kafka unavailable');

    expect(mocks.calls).toEqual([
      'redis-created',
      'kafka-created',
      'kafka-stop',
      'redis-stop',
    ]);
    expect(mocks.streamStart).not.toHaveBeenCalled();
  });

  it('cleans the partially started stream before Kafka when TwelveData startup fails', async () => {
    mocks.streamStart.mockImplementationOnce(() => {
      mocks.calls.push('stream-start');
      throw new Error('TwelveData start failed');
    });
    const runtime = new ActiveIngestionRuntime();

    await expect(runtime.start()).rejects.toThrow('TwelveData start failed');

    expect(mocks.calls).toEqual([
      'redis-created',
      'kafka-created',
      'kafka-start',
      'stream-created',
      'stream-start',
      'stream-stop',
      'kafka-stop',
      'redis-stop',
    ]);
  });

});
