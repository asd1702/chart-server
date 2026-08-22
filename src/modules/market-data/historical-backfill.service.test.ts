import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  getLastCandles: vi.fn(),
  bulkSave1mCandles: vi.fn(),
  refreshAllContinuousAggregates: vi.fn(),
  axiosGet: vi.fn(),
  logger: {
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock('../../config', () => ({
  default: {
    market: { streamSymbols: ['OK/USD', 'FAIL/USD'] },
    TWELVE_DATA_API_KEY: 'test-key',
  },
}));

vi.mock('../candle', () => ({
  candleRepository: {
    getLastCandles: mocks.getLastCandles,
    bulkSave1mCandles: mocks.bulkSave1mCandles,
  },
  candleService: {
    refreshAllContinuousAggregates: mocks.refreshAllContinuousAggregates,
  },
}));

vi.mock('axios', () => ({
  default: { get: mocks.axiosGet },
}));

vi.mock('../../shared/utils/rate-limiter', () => ({
  isTwelveDataRateLimitError: vi.fn(() => false),
  scheduleTwelveDataRequest: vi.fn((request: () => Promise<unknown>) => request()),
}));

vi.mock('../../shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) =>
    error instanceof Error ? error.message : 'Unknown error',
  logger: mocks.logger,
}));

import { runHistoricalBackfill } from './historical-backfill.service';

describe('historical backfill result logging', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.getLastCandles.mockImplementation(() => Promise.resolve([
      { time: new Date(Date.now() - 60_000) },
    ]));
    mocks.bulkSave1mCandles.mockResolvedValue(1);
    mocks.refreshAllContinuousAggregates.mockResolvedValue({
      succeeded: 7,
      failed: 0,
    });
  });

  it('모든 symbol 확인 성공 시 completed와 정확한 집계를 기록한다', async () => {
    await runHistoricalBackfill();

    expect(logEvents(mocks.logger.info, 'historical_backfill_completed')).toEqual([
      expect.arrayContaining([
        expect.any(String),
        expect.objectContaining({ succeeded: 2, failed: 0, skipped: 0 }),
      ]),
    ]);
    expect(logEvents(
      mocks.logger.warn,
      'historical_backfill_completed_with_errors',
    )).toHaveLength(0);
  });

  it('일부 symbol 실패를 no-gap과 구분해 WARN 집계에 반영한다', async () => {
    mocks.getLastCandles.mockImplementation((symbol: string) => {
      if (symbol === 'FAIL/USD') {
        return Promise.reject(new Error('database unavailable'));
      }
      return Promise.resolve([{ time: new Date(Date.now() - 60_000) }]);
    });

    await runHistoricalBackfill();

    expect(logEvents(
      mocks.logger.warn,
      'historical_backfill_completed_with_errors',
    )).toEqual([
      expect.arrayContaining([
        expect.any(String),
        expect.objectContaining({ succeeded: 1, failed: 1, skipped: 0 }),
      ]),
    ]);
    expect(logEvents(
      mocks.logger.info,
      'historical_backfill_gap_not_found',
    )).toHaveLength(0);
  });

  it('모든 symbol 실패를 성공 또는 gap 없음으로 계산하지 않는다', async () => {
    mocks.getLastCandles.mockRejectedValue(new Error('database unavailable'));

    await runHistoricalBackfill();

    const completion = logEvents(
      mocks.logger.warn,
      'historical_backfill_completed_with_errors',
    );
    expect(completion[0]?.[1]).toEqual(expect.objectContaining({
      succeeded: 0,
      failed: 2,
    }));
    expect(logEvents(
      mocks.logger.info,
      'historical_backfill_gap_not_found',
    )).toHaveLength(0);
  });

  it('CA refresh 부분 실패를 별도 WARN과 최종 결과에 반영한다', async () => {
    mocks.getLastCandles.mockImplementation((symbol: string) => Promise.resolve([
      {
        time: new Date(Date.now() - (symbol === 'OK/USD' ? 10 : 1) * 60_000),
      },
    ]));
    mocks.axiosGet.mockResolvedValue({
      data: {
        values: [{
          datetime: '2026-08-20T00:01:00.000Z',
          open: '100',
          high: '101',
          low: '99',
          close: '100',
          volume: '1',
        }],
      },
    });
    mocks.refreshAllContinuousAggregates.mockResolvedValue({
      succeeded: 6,
      failed: 1,
    });

    await runHistoricalBackfill();

    expect(logEvents(
      mocks.logger.warn,
      'continuous_aggregate_refresh_completed_with_errors',
    )[0]?.[1]).toEqual(expect.objectContaining({ succeeded: 6, failed: 1 }));
    expect(logEvents(
      mocks.logger.warn,
      'historical_backfill_completed_with_errors',
    )[0]?.[1]).toEqual(expect.objectContaining({
      succeeded: 2,
      failed: 0,
      aggregateRefreshFailed: 1,
    }));
  });
});

function logEvents(
  logMethod: ReturnType<typeof vi.fn>,
  event: string,
): unknown[][] {
  return logMethod.mock.calls.filter(([, metadata]) =>
    metadata && typeof metadata === 'object' && metadata.event === event
  );
}
