import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  commitOffset: vi.fn(),
  bulkSave1mCandles: vi.fn(),
  logger: {
    info: vi.fn(),
  },
}));

vi.mock('../../shared/utils/logger', () => ({
  logger: { child: vi.fn(() => mocks.logger) },
}));

import { CandleProcessor } from './candle-processor';

function rawMessage(
  offset: string,
  timestamp: number,
  price: number,
  symbol = 'BTC/USD',
) {
  return {
    topic: 'market.raw-ticks',
    partition: 0,
    offset,
    tick: {
      schemaVersion: 1 as const,
      symbol,
      price,
      providerTimestampSec: timestamp,
      receivedAtMs: timestamp * 1000,
      source: 'twelvedata' as const,
    },
  };
}

describe('CandleProcessor pure replay checkpointing', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.bulkSave1mCandles.mockResolvedValue(1);
    mocks.commitOffset.mockResolvedValue(undefined);
  });

  function createProcessor(
    symbols: readonly string[] = ['BTC/USD'],
  ): CandleProcessor {
    return new CandleProcessor(
      symbols,
      { bulkSave1mCandles: mocks.bulkSave1mCandles },
      { commitOffset: mocks.commitOffset },
    );
  }

  it('does not write or checkpoint while it only builds the current minute', async () => {
    const processor = createProcessor();

    await expect(processor.process(rawMessage('100', 60, 100))).resolves.toEqual({
      completedCandle: null,
    });
    await expect(processor.process(rawMessage('101', 80, 110))).resolves.toEqual({
      completedCandle: null,
    });

    expect(mocks.bulkSave1mCandles).not.toHaveBeenCalled();
    expect(mocks.commitOffset).not.toHaveBeenCalled();
  });

  it('writes the completed candle before committing the boundary offset itself', async () => {
    const calls: string[] = [];
    mocks.bulkSave1mCandles.mockImplementation(async () => {
      calls.push('db-save');
      return 1;
    });
    mocks.commitOffset.mockImplementation(async () => {
      calls.push('commit');
    });
    const processor = createProcessor();

    await processor.process(rawMessage('100', 60, 100));
    await processor.process(rawMessage('101', 90, 110));
    const result = await processor.process(rawMessage('150', 120, 105));

    expect(result.completedCandle).toEqual(expect.objectContaining({
      symbol: 'BTC/USD',
      startTime: 60,
      open: 100,
      high: 110,
      low: 100,
      close: 110,
    }));

    expect(mocks.bulkSave1mCandles).toHaveBeenCalledWith([
      {
        symbol: 'BTC/USD',
        time: new Date(60_000),
        open: 100,
        high: 110,
        low: 100,
        close: 110,
        volume: 0,
      },
    ]);
    expect(mocks.commitOffset).toHaveBeenCalledWith({
      topic: 'market.raw-ticks',
      partition: 0,
      offset: '150',
    });
    expect(calls).toEqual(['db-save', 'commit']);
  });

  it('keeps OHLC state independent and commits the oldest symbol replay start', async () => {
    const processor = createProcessor(['BTC/USD', 'ETH/USD']);

    await processor.process(rawMessage('10', 60, 100, 'BTC/USD'));
    await processor.process(rawMessage('11', 60, 200, 'ETH/USD'));
    await processor.process(rawMessage('12', 90, 120, 'BTC/USD'));
    await processor.process(rawMessage('13', 90, 180, 'ETH/USD'));

    await processor.process(rawMessage('20', 120, 110, 'BTC/USD'));

    expect(mocks.bulkSave1mCandles).toHaveBeenLastCalledWith([
      {
        symbol: 'BTC/USD',
        time: new Date(60_000),
        open: 100,
        high: 120,
        low: 100,
        close: 120,
        volume: 0,
      },
    ]);
    expect(mocks.commitOffset).toHaveBeenLastCalledWith({
      topic: 'market.raw-ticks',
      partition: 0,
      offset: '11',
    });

    await processor.process(rawMessage('21', 120, 190, 'ETH/USD'));

    expect(mocks.bulkSave1mCandles).toHaveBeenLastCalledWith([
      {
        symbol: 'ETH/USD',
        time: new Date(60_000),
        open: 200,
        high: 200,
        low: 180,
        close: 180,
        volume: 0,
      },
    ]);
    expect(mocks.commitOffset).toHaveBeenLastCalledWith({
      topic: 'market.raw-ticks',
      partition: 0,
      offset: '20',
    });
  });

  it('rebuilds every symbol current candle when replay starts at the global watermark', async () => {
    const firstWrites: unknown[][] = [];
    const firstCommits: string[] = [];
    const firstProcessor = new CandleProcessor(
      ['BTC/USD', 'ETH/USD'],
      {
        bulkSave1mCandles: vi.fn(async (candles) => {
          firstWrites.push(candles);
          return 1;
        }),
      },
      {
        commitOffset: vi.fn(async ({ offset }) => {
          firstCommits.push(offset);
        }),
      },
    );

    await firstProcessor.process(rawMessage('10', 60, 100, 'BTC/USD'));
    await firstProcessor.process(rawMessage('11', 60, 200, 'ETH/USD'));
    await firstProcessor.process(rawMessage('12', 90, 120, 'BTC/USD'));
    await firstProcessor.process(rawMessage('13', 90, 180, 'ETH/USD'));
    await firstProcessor.process(rawMessage('20', 120, 110, 'BTC/USD'));
    await firstProcessor.process(rawMessage('21', 120, 190, 'ETH/USD'));

    expect(firstCommits).toEqual(['11', '20']);
    expect(firstWrites).toHaveLength(2);

    const replayWrites: unknown[][] = [];
    const replayCommits: string[] = [];
    const replayProcessor = new CandleProcessor(
      ['BTC/USD', 'ETH/USD'],
      {
        bulkSave1mCandles: vi.fn(async (candles) => {
          replayWrites.push(candles);
          return 1;
        }),
      },
      {
        commitOffset: vi.fn(async ({ offset }) => {
          replayCommits.push(offset);
        }),
      },
    );

    /* The prior global checkpoint was 20, so both current candles replay. */
    await replayProcessor.process(rawMessage('20', 120, 110, 'BTC/USD'));
    await replayProcessor.process(rawMessage('21', 120, 190, 'ETH/USD'));
    await replayProcessor.process(rawMessage('22', 130, 115, 'BTC/USD'));
    await replayProcessor.process(rawMessage('23', 130, 195, 'ETH/USD'));
    await replayProcessor.process(rawMessage('30', 180, 120, 'BTC/USD'));
    await replayProcessor.process(rawMessage('31', 180, 200, 'ETH/USD'));

    expect(replayWrites).toEqual([
      [
        {
          symbol: 'BTC/USD',
          time: new Date(120_000),
          open: 110,
          high: 115,
          low: 110,
          close: 115,
          volume: 0,
        },
      ],
      [
        {
          symbol: 'ETH/USD',
          time: new Date(120_000),
          open: 190,
          high: 195,
          low: 190,
          close: 195,
          volume: 0,
        },
      ],
    ]);
    expect(replayCommits).toEqual(['21', '30']);
  });

  it('rejects without committing when the completed candle cannot be saved', async () => {
    mocks.bulkSave1mCandles.mockRejectedValueOnce(
      new Error('database unavailable'),
    );
    const processor = createProcessor();

    await processor.process(rawMessage('100', 60, 100));

    await expect(
      processor.process(rawMessage('150', 120, 105)),
    ).rejects.toThrow('database unavailable');

    expect(mocks.commitOffset).not.toHaveBeenCalled();
  });

  it('rejects after a commit failure so the caller can discard mutated memory state', async () => {
    mocks.commitOffset.mockRejectedValueOnce(
      new Error('Kafka commit failure'),
    );
    const processor = createProcessor();

    await processor.process(rawMessage('100', 60, 100));

    await expect(
      processor.process(rawMessage('150', 120, 105)),
    ).rejects.toThrow('Kafka commit failure');

    expect(mocks.bulkSave1mCandles).toHaveBeenCalledOnce();
    expect(mocks.commitOffset).toHaveBeenCalledOnce();
  });

  it('fails fast for a symbol outside the configured symbol set', async () => {
    const processor = createProcessor(['BTC/USD', 'ETH/USD']);

    await expect(
      processor.process(rawMessage('100', 60, 100, 'SOL/USD')),
    ).rejects.toThrow('Unexpected symbol for candle processor: SOL/USD');

    expect(mocks.bulkSave1mCandles).not.toHaveBeenCalled();
    expect(mocks.commitOffset).not.toHaveBeenCalled();
  });
});
