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
  handlePriceUpdate,
  resetTwelveDataCandleState,
} from './twelvedata.provider';

describe('TwelveData price handling', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.enqueueCandle.mockResolvedValue(undefined);
    resetTwelveDataCandleState();
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

    await handlePriceUpdate('FAIL/USD', 100, 60, publisher);
    await handlePriceUpdate('FAIL/USD', 101, 120, publisher);

    expect(mocks.enqueueCandle).toHaveBeenCalledWith(expect.objectContaining({
      symbol: 'FAIL/USD',
      startTime: 60,
      close: 100,
    }));
    expect(calls).toEqual([
      'publish:tick',
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

    let firstTickSettled = false;
    const firstTick = handlePriceUpdate('LATENCY/USD', 200, 60, publisher)
      .finally(() => {
        firstTickSettled = true;
      });
    const secondTick = handlePriceUpdate('LATENCY/USD', 201, 120, publisher);

    await vi.waitFor(() => expect(mocks.enqueueCandle).toHaveBeenCalledOnce());
    expect(firstTickSettled).toBe(false);

    releaseFirstPublish?.();
    await Promise.all([firstTick, secondTick]);
  });

  it('does not treat a durability failure as a best-effort publish failure', async () => {
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    await handlePriceUpdate('DURABLE/USD', 250, 60, publisher);
    mocks.enqueueCandle.mockRejectedValueOnce(new Error('RocksDB unavailable'));

    await expect(
      handlePriceUpdate('DURABLE/USD', 251, 120, publisher)
    ).rejects.toThrow('RocksDB unavailable');
    expect(publisher.publish).toHaveBeenCalledTimes(1);
  });

  it('keeps the existing tick and candle realtime message formats', async () => {
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    await handlePriceUpdate('OK/USD', 300, 60, publisher);
    await handlePriceUpdate('OK/USD', 301, 120, publisher);

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
  });

  it('does not carry CandleMaker OHLC state across a leadership reset', async () => {
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };

    // Epoch 1 has a wide price range but does not complete its minute candle.
    await handlePriceUpdate('OK/USD', 100, 60, publisher);
    await handlePriceUpdate('OK/USD', 120, 70, publisher);
    await handlePriceUpdate('OK/USD', 90, 80, publisher);
    await handlePriceUpdate('OK/USD', 110, 90, publisher);

    resetTwelveDataCandleState();

    // Epoch 2 must begin from a fresh CandleMaker, not the 120/90 range.
    await handlePriceUpdate('OK/USD', 105, 120, publisher);
    await handlePriceUpdate('OK/USD', 106, 130, publisher);
    await handlePriceUpdate('OK/USD', 104, 140, publisher);
    await handlePriceUpdate('OK/USD', 105, 150, publisher);
    await handlePriceUpdate('OK/USD', 107, 180, publisher);

    expect(mocks.enqueueCandle).toHaveBeenCalledOnce();
    expect(mocks.enqueueCandle).toHaveBeenCalledWith(expect.objectContaining({
      symbol: 'OK/USD',
      startTime: 120,
      open: 105,
      high: 106,
      low: 104,
      close: 105,
    }));
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
