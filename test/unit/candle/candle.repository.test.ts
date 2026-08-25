import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  createMany: vi.fn(),
  logger: {
    error: vi.fn(),
    info: vi.fn(),
  },
}));

vi.mock('../../../src/shared', () => ({
  prisma: {
    candle1m: {
      createMany: mocks.createMany,
    },
  },
}));

vi.mock('../../../src/shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) =>
    error instanceof Error ? error.message : 'Unknown error',
  logger: mocks.logger,
}));

import { CandleRepository } from '../../../src/modules/candle/candle.repository';

describe('CandleRepository Continuous Aggregate refresh result', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.createMany.mockResolvedValue({ count: 0 });
  });

  it('개별 view 실패 후에도 다음 view를 처리하고 결과를 집계한다', async () => {
    const repository = new CandleRepository();
    const refresh = vi.spyOn(repository, 'refreshContinuousAggregate')
      .mockResolvedValue(undefined);
    refresh.mockRejectedValueOnce(new Error('1h refresh failed'));

    await expect(repository.refreshAllContinuousAggregates()).resolves.toEqual({
      succeeded: 6,
      failed: 1,
    });
    expect(refresh).toHaveBeenCalledTimes(7);
    expect(mocks.logger.error).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        event: 'continuous_aggregate_view_refresh_failed',
      }),
    );
  });

  it('replay된 candle을 skipDuplicates로 멱등 저장한다', async () => {
    const repository = new CandleRepository();
    const candle = {
      symbol: 'BTC/USD',
      time: new Date(60_000),
      open: 100,
      high: 110,
      low: 100,
      close: 110,
      volume: 0,
    };

    await repository.bulkSave1mCandles([candle]);

    expect(mocks.createMany).toHaveBeenCalledWith({
      data: [candle],
      skipDuplicates: true,
    });
  });
});
