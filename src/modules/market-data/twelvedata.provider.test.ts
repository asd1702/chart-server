import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock('../../config', () => ({
  default: {
    market: { streamSymbols: ['BTC/USD'] },
    TWELVE_DATA_API_KEY: 'test-key',
  },
}));

vi.mock('../../shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) =>
    error instanceof Error ? error.message : 'Unknown error',
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

function createPublisher() {
  return {
    publish: vi.fn().mockResolvedValue(undefined),
    disconnect: vi.fn().mockResolvedValue(undefined),
  };
}

describe('TwelveData feed ingestion', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('does not publish a realtime tick before Kafka durable admission succeeds', async () => {
    const kafkaAck = createDeferred();
    const rawTickPublisher = createRawTickPublisher();
    rawTickPublisher.publish.mockImplementationOnce(async () => {
      await kafkaAck.promise;
    });
    const publisher = createPublisher();
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['BTC/USD'],
    });

    const handling = stream.handlePriceUpdate('BTC/USD', 100, 60);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledOnce();
    });
    expect(publisher.publish).not.toHaveBeenCalled();

    kafkaAck.resolve();
    await handling;

    expect(publisher.publish).toHaveBeenCalledWith({
      type: 'tick',
      symbol: 'BTC/USD',
      price: 100,
      timestamp: 60,
    });
  });

  it('does not publish a realtime tick when Kafka admission fails', async () => {
    const rawTickPublisher = createRawTickPublisher();
    rawTickPublisher.publish.mockRejectedValueOnce(
      new Error('Kafka unavailable'),
    );
    const publisher = createPublisher();
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['BTC/USD'],
    });

    await expect(
      stream.handlePriceUpdate('BTC/USD', 100, 60),
    ).rejects.toThrow('Kafka unavailable');

    expect(publisher.publish).not.toHaveBeenCalled();

    await stream.handlePriceUpdate('BTC/USD', 101, 120);

    expect(publisher.publish).toHaveBeenCalledWith({
      type: 'tick',
      symbol: 'BTC/USD',
      price: 101,
      timestamp: 120,
    });
  });

  it('does not let a slow Redis tick publish block the next same-symbol Kafka admission', async () => {
    const redisPublish = createDeferred();
    const publisher = createPublisher();
    publisher.publish
      .mockImplementationOnce(() => redisPublish.promise)
      .mockResolvedValue(undefined);
    const rawTickPublisher = createRawTickPublisher();
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['BTC/USD'],
    });

    const first = stream.handlePriceUpdate('BTC/USD', 100, 60);

    await vi.waitFor(() => {
      expect(publisher.publish).toHaveBeenCalledOnce();
    });

    const second = stream.handlePriceUpdate('BTC/USD', 101, 120);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledTimes(2);
    });

    redisPublish.resolve();
    await Promise.all([first, second]);
  });

  it('serializes Kafka durable admission for the same symbol', async () => {
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
    const stream = new TwelveDataStream(
      createPublisher(),
      rawTickPublisher,
      { symbols: ['BTC/USD'] },
    );

    const first = stream.handlePriceUpdate('BTC/USD', 100, 60);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledOnce();
    });

    const second = stream.handlePriceUpdate('BTC/USD', 101, 120);
    await Promise.resolve();

    expect(rawTickPublisher.publish).toHaveBeenCalledOnce();

    firstKafkaAck.resolve();
    await Promise.all([first, second]);

    expect(calls.indexOf('kafka:A:ack')).toBeLessThan(
      calls.indexOf('kafka:B:start'),
    );
  });

  it('does not serialize Kafka durable admission across different symbols', async () => {
    const btcKafkaAck = createDeferred();
    const rawTickPublisher = createRawTickPublisher();
    rawTickPublisher.publish.mockImplementation(async (tick) => {
      if (tick.symbol === 'BTC/USD') {
        await btcKafkaAck.promise;
      }
    });
    const stream = new TwelveDataStream(
      createPublisher(),
      rawTickPublisher,
      { symbols: ['BTC/USD', 'ETH/USD'] },
    );

    const btc = stream.handlePriceUpdate('BTC/USD', 100, 60);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledOnce();
    });

    const eth = stream.handlePriceUpdate('ETH/USD', 200, 60);

    await vi.waitFor(() => {
      expect(rawTickPublisher.publish).toHaveBeenCalledTimes(2);
    });

    btcKafkaAck.resolve();
    await Promise.all([btc, eth]);
  });

  it('never creates a completed candle event at a minute boundary', async () => {
    const publisher = createPublisher();
    const rawTickPublisher = createRawTickPublisher();
    const stream = new TwelveDataStream(publisher, rawTickPublisher, {
      symbols: ['BTC/USD'],
    });

    await stream.handlePriceUpdate('BTC/USD', 100, 60);
    await stream.handlePriceUpdate('BTC/USD', 101, 120);

    expect(publisher.publish).toHaveBeenCalledTimes(2);
    expect(publisher.publish).toHaveBeenNthCalledWith(2, {
      type: 'tick',
      symbol: 'BTC/USD',
      price: 101,
      timestamp: 120,
    });
  });

  it('publishes the normalized raw tick schema to Kafka', async () => {
    const rawTickPublisher = createRawTickPublisher();
    const stream = new TwelveDataStream(
      createPublisher(),
      rawTickPublisher,
      { symbols: ['BTC/USD'] },
    );

    await stream.handlePriceUpdate('BTC/USD', 100, 60);

    expect(rawTickPublisher.publish).toHaveBeenCalledWith({
      schemaVersion: 1,
      symbol: 'BTC/USD',
      price: 100,
      providerTimestampSec: 60,
      receivedAtMs: expect.any(Number),
      source: 'twelvedata',
    });
  });

  it('drops a tick for a symbol that is not configured for this feed', async () => {
    const rawTickPublisher = createRawTickPublisher();
    const publisher = createPublisher();
    const stream = new TwelveDataStream(
      publisher,
      rawTickPublisher,
      { symbols: ['BTC/USD'] },
    );

    await expect(
      stream.handlePriceUpdate('ETH/USD', 200, 60),
    ).resolves.toBeUndefined();

    expect(rawTickPublisher.publish).not.toHaveBeenCalled();
    expect(publisher.publish).not.toHaveBeenCalled();
    expect(mocks.logger.warn).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        event: 'twelvedata_unconfigured_symbol',
        symbol: 'ETH/USD',
      }),
    );
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
