import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  enqueueCandle: vi.fn(),
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock('../../config', () => ({
  default: {
    market: {
      streamSymbols: ['FAIL/USD', 'LATENCY/USD', 'DURABLE/USD', 'OK/USD'],
    },
    TWELVE_DATA_API_KEY: 'test-key',
  },
}));

vi.mock('../candle/candle.persistence', () => ({
  enqueueCandle: mocks.enqueueCandle,
}));

vi.mock('../../shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
  logger: mocks.logger,
}));

import {
  buildTwelveDataSubscription,
  TwelveDataStream,
} from './twelvedata.provider';

function createRawTickPublisher() {
  return {
    start: vi.fn().mockResolvedValue(undefined),
    publish: vi.fn().mockResolvedValue(undefined),
    stop: vi.fn().mockResolvedValue(undefined),
  };
}

function createDeferred() {
  let resolve!: () => void;
  let reject!: (error: unknown) => void;

  const promise = new Promise<void>((res, rej) => {
    resolve = res;
    reject = rej;
  });

  return { promise, reject, resolve };
}

describe('TwelveData price handling', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.enqueueCandle.mockResolvedValue(undefined);
  });

  it('persists a completed candle even when Redis publish rejects', async () => {
    const calls: string[] = [];
    mocks.enqueueCandle.mockImplementation(async () => {
      calls.push('enqueue');
    });
    const publisher = {
      publish: vi.fn(async (message: { type: string }) => {
        calls.push(`publish:${message.type}`);
        throw new Error('Redis unavailable');
      }),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const rawTickPublisher = createRawTickPublisher();
    rawTickPublisher.publish.mockImplementation(async () => {
      calls.push('raw-tick-publish');
    });
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['FAIL/USD'],
    });

    await stream.handlePriceUpdate('FAIL/USD', 100, 60);
    await stream.handlePriceUpdate('FAIL/USD', 101, 120);

    expect(mocks.enqueueCandle).toHaveBeenCalledWith(expect.objectContaining({
      symbol: 'FAIL/USD',
      startTime: 60,
      close: 100,
    }));
    expect(calls).toEqual([
      'raw-tick-publish',
      'publish:tick',
      'raw-tick-publish',
      'enqueue',
      'publish:tick',
      'publish:candle',
    ]);
    expect(mocks.logger.error).not.toHaveBeenCalled();
  });

  it('updates and persists candle state without waiting for a slow Redis publish', async () => {
    let releaseFirstPublish: (() => void) | undefined;
    const firstPublish = new Promise<void>((resolve) => {
      releaseFirstPublish = resolve;
    });
    const publisher = {
      publish: vi.fn()
        .mockImplementationOnce(() => firstPublish)
        .mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const stream = new TwelveDataStream(
      publisher,
      createRawTickPublisher(),
      {
      symbols: ['LATENCY/USD'],
      },
    );

    let firstTickSettled = false;
    const firstTick = stream.handlePriceUpdate('LATENCY/USD', 200, 60)
      .finally(() => {
        firstTickSettled = true;
      });
    const secondTick = stream.handlePriceUpdate('LATENCY/USD', 201, 120);

    await vi.waitFor(() => expect(mocks.enqueueCandle).toHaveBeenCalledOnce());
    expect(firstTickSettled).toBe(false);

    releaseFirstPublish?.();
    await Promise.all([firstTick, secondTick]);
  });

  it('serializes the Kafka-to-RocksDB durable phase for the same symbol', async () => {
    const firstKafkaAck = createDeferred();
    const calls: string[] = [];
    const rawTickPublisher = createRawTickPublisher();
    rawTickPublisher.publish
      .mockImplementationOnce(async () => {
        calls.push('kafka:A:start');
        await firstKafkaAck.promise;
        calls.push('kafka:A:ack');
      })
      .mockImplementationOnce(async () => {
        calls.push('kafka:B:start');
      });
    mocks.enqueueCandle.mockImplementation(async (candle) => {
      calls.push(`enqueue:${candle.startTime}`);
    });
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['BTC/USD'],
    });

    const first = stream.handlePriceUpdate('BTC/USD', 100, 60);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledTimes(1);
    });

    const second = stream.handlePriceUpdate('BTC/USD', 101, 120);

    await Promise.resolve();

    expect(rawTickPublisher.publish).toHaveBeenCalledTimes(1);
    expect(mocks.enqueueCandle).not.toHaveBeenCalled();

    firstKafkaAck.resolve();

    await Promise.all([first, second]);

    expect(rawTickPublisher.publish).toHaveBeenCalledTimes(2);
    expect(mocks.enqueueCandle).toHaveBeenCalledWith(expect.objectContaining({
      symbol: 'BTC/USD',
      startTime: 60,
      open: 100,
      close: 100,
    }));
    expect(calls.indexOf('kafka:A:ack')).toBeLessThan(
      calls.indexOf('kafka:B:start'),
    );
  });

  it('does not serialize durable processing across different symbols', async () => {
    const btcKafkaAck = createDeferred();
    const rawTickPublisher = createRawTickPublisher();
    rawTickPublisher.publish.mockImplementation(async (tick) => {
      if (tick.symbol === 'BTC/USD') {
        await btcKafkaAck.promise;
      }
    });
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['BTC/USD', 'ETH/USD'],
    });

    const btc = stream.handlePriceUpdate('BTC/USD', 100, 60);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledTimes(1);
    });

    const eth = stream.handlePriceUpdate('ETH/USD', 200, 60);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledTimes(2);
    });

    expect(rawTickPublisher.publish).toHaveBeenCalledWith(
      expect.objectContaining({ symbol: 'ETH/USD' }),
    );

    btcKafkaAck.resolve();

    await Promise.all([btc, eth]);
  });

  it('does not treat a durability failure as a best-effort publish failure', async () => {
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const stream = new TwelveDataStream(
      publisher,
      createRawTickPublisher(),
      {
      symbols: ['DURABLE/USD'],
      },
    );

    await stream.handlePriceUpdate('DURABLE/USD', 250, 60);
    mocks.enqueueCandle.mockRejectedValueOnce(new Error('RocksDB unavailable'));

    await expect(
      stream.handlePriceUpdate('DURABLE/USD', 251, 120)
    ).rejects.toThrow('RocksDB unavailable');
    expect(publisher.publish).toHaveBeenCalledTimes(1);
  });

  it('keeps the existing tick and candle realtime message formats', async () => {
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const rawTickPublisher = createRawTickPublisher();
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['OK/USD'],
    });

    await stream.handlePriceUpdate('OK/USD', 300, 60);
    await stream.handlePriceUpdate('OK/USD', 301, 120);

    expect(publisher.publish).toHaveBeenNthCalledWith(2, {
      type: 'tick',
      symbol: 'OK/USD',
      price: 301,
      timestamp: 120,
    });
    expect(publisher.publish).toHaveBeenNthCalledWith(3, {
      type: 'candle',
      timeframe: '1m',
      candle: expect.objectContaining({
        symbol: 'OK/USD',
        startTime: 60,
        open: 300,
        close: 300,
      }),
    });
    expect(rawTickPublisher.publish).toHaveBeenNthCalledWith(1, {
      schemaVersion: 1,
      symbol: 'OK/USD',
      price: 300,
      providerTimestampSec: 60,
      receivedAtMs: expect.any(Number),
      source: 'twelvedata',
    });
  });

  it('does not advance CandleMaker or publish realtime events when Kafka rejects a tick', async () => {
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const rawTickPublisher = createRawTickPublisher();
    rawTickPublisher.publish.mockRejectedValueOnce(
      new Error('Kafka unavailable'),
    );
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['BTC/USD'],
    });

    await expect(
      stream.handlePriceUpdate('BTC/USD', 100, 60),
    ).rejects.toThrow('Kafka unavailable');

    expect(mocks.enqueueCandle).not.toHaveBeenCalled();
    expect(publisher.publish).not.toHaveBeenCalled();

    await stream.handlePriceUpdate('BTC/USD', 101, 120);
    await stream.handlePriceUpdate('BTC/USD', 102, 180);

    expect(mocks.enqueueCandle).toHaveBeenCalledWith(expect.objectContaining({
      symbol: 'BTC/USD',
      startTime: 120,
      open: 101,
    }));
    expect(rawTickPublisher.publish).toHaveBeenCalledTimes(3);
  });

  it('isolates CandleMaker OHLC state between stream instances', async () => {
    const publisherA = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const publisherB = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const streamA = new TwelveDataStream(
      publisherA,
      createRawTickPublisher(),
      {
      symbols: ['OK/USD'],
      },
    );
    const streamB = new TwelveDataStream(
      publisherB,
      createRawTickPublisher(),
      {
      symbols: ['OK/USD'],
      },
    );

    // Stream A has a wide, incomplete candle range.
    await streamA.handlePriceUpdate('OK/USD', 100, 60);
    await streamA.handlePriceUpdate('OK/USD', 120, 70);
    await streamA.handlePriceUpdate('OK/USD', 90, 80);
    await streamA.handlePriceUpdate('OK/USD', 110, 90);

    // Stream B starts independently and completes its own candle.
    await streamB.handlePriceUpdate('OK/USD', 105, 120);
    await streamB.handlePriceUpdate('OK/USD', 106, 130);
    await streamB.handlePriceUpdate('OK/USD', 104, 140);
    await streamB.handlePriceUpdate('OK/USD', 105, 150);
    await streamB.handlePriceUpdate('OK/USD', 107, 180);

    expect(mocks.enqueueCandle).toHaveBeenCalledOnce();
    expect(mocks.enqueueCandle).toHaveBeenCalledWith(expect.objectContaining({
      symbol: 'OK/USD',
      startTime: 120,
      open: 105,
      high: 106,
      low: 104,
      close: 105,
    }));
    expect(publisherA.publish).toHaveBeenCalledTimes(4);
    expect(publisherB.publish).toHaveBeenCalledTimes(6);
  });
});

describe('TwelveData subscription configuration', () => {
  it('subscribes only to BTC/USD for the default lab workload', () => {
    expect(buildTwelveDataSubscription(['BTC/USD'])).toEqual({
      action: 'subscribe',
      params: { symbols: 'BTC/USD' },
    });
  });

  it('keeps multi-symbol subscription capability', () => {
    expect(buildTwelveDataSubscription(['BTC/USD', 'ETH/USD'])).toEqual({
      action: 'subscribe',
      params: { symbols: 'BTC/USD,ETH/USD' },
    });
  });
});
