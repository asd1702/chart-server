import { beforeEach, describe, expect, it, vi } from 'vitest';
import request from 'supertest';

const mocks = vi.hoisted(() => ({
  queryRaw: vi.fn(),
  getCandles: vi.fn(),
  refreshContinuousAggregate: vi.fn(),
  logger: {
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock('./config', () => ({ default: {} }));
vi.mock('./shared/db/prisma', () => ({
  prisma: { $queryRaw: mocks.queryRaw },
}));
vi.mock('./shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
  logger: mocks.logger,
}));
vi.mock('./modules/candle/candle.service', () => ({
  candleService: {
    getCandles: mocks.getCandles,
    refreshContinuousAggregate: mocks.refreshContinuousAggregate,
  },
  CandleService: class {},
}));

import { createApp } from './app';

describe('Chart Server API', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.queryRaw.mockResolvedValue([{ '?column?': 1 }]);
    mocks.getCandles.mockResolvedValue({
      symbol: 'AAPL',
      timeframe: '1m',
      data: [
        {
          time: 1_700_000_000,
          open: 100,
          high: 105,
          low: 98,
          close: 102,
          volume: 10,
        },
      ],
    });
    mocks.refreshContinuousAggregate.mockResolvedValue(undefined);
  });

  it('returns liveness from GET /', async () => {
    const response = await request(createApp()).get('/').expect(200);

    expect(response.body.status).toBe('ok');
  });

  it('returns readiness from GET /health without a real database', async () => {
    const response = await request(createApp()).get('/health').expect(200);

    expect(response.body.status).toBe('ok');
    expect(response.body.services.database.status).toBe('healthy');
    expect(mocks.queryRaw).toHaveBeenCalledOnce();
  });

  it('reports Redis subscriber loss as degraded while REST remains ready', async () => {
    const response = await request(createApp({
      getRedisSubscriberStatus: () => 'unavailable',
    })).get('/health').expect(200);

    expect(response.body).toMatchObject({
      status: 'degraded',
      services: {
        database: { status: 'healthy' },
        redis: { status: 'unavailable', role: 'subscriber' },
      },
    });
  });

  it('logs a database outage and recovery only on health state transitions', async () => {
    mocks.queryRaw
      .mockRejectedValueOnce(new Error('database unavailable'))
      .mockRejectedValueOnce(new Error('database unavailable'))
      .mockResolvedValueOnce([{ '?column?': 1 }]);
    const app = createApp();

    await request(app).get('/health').expect(503);
    await request(app).get('/health').expect(503);
    await request(app).get('/health').expect(200);

    expect(mocks.logger.warn).toHaveBeenCalledOnce();
    expect(mocks.logger.warn).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ event: 'health_database_unavailable' }),
    );
    expect(mocks.logger.info).toHaveBeenCalledOnce();
    expect(mocks.logger.info).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ event: 'health_database_recovered' }),
    );
  });

  it('returns candles from GET /api/candles/:symbol/:timeframe', async () => {
    const response = await request(createApp())
      .get('/api/candles/AAPL/1m?limit=10')
      .expect(200);

    expect(mocks.getCandles).toHaveBeenCalledWith('AAPL', '1m', {
      limit: 10,
      from: undefined,
      to: undefined,
    });
    expect(response.body).toMatchObject({
      symbol: 'AAPL',
      timeframe: '1m',
    });
    expect(response.body.data).toBeInstanceOf(Array);
  });

  it('decodes an encoded slash in a candle symbol', async () => {
    await request(createApp())
      .get('/api/candles/BTC%2FUSD/1m?limit=10')
      .expect(200);

    expect(mocks.getCandles).toHaveBeenCalledWith('BTC/USD', '1m', {
      limit: 10,
      from: undefined,
      to: undefined,
    });
  });

  it('rejects an unsupported candle timeframe', async () => {
    const response = await request(createApp())
      .get('/api/candles/AAPL/3m')
      .expect(400);

    expect(response.body).toEqual({
      success: false,
      errorCode: 'INVALID_TIMEFRAME',
      message: 'Unsupported timeframe.',
    });
    expect(response.body).not.toHaveProperty('error');
    expect(mocks.getCandles).not.toHaveBeenCalled();
  });

  it.each(['abc', '0', '5001', '1.5'])('rejects invalid limit %s', async (limit) => {
    const response = await request(createApp())
      .get(`/api/candles/AAPL/1m?limit=${limit}`)
      .expect(400);

    expect(response.body.errorCode).toBe('INVALID_LIMIT');
    expect(response.body.message).toBeTruthy();
    expect(response.body).not.toHaveProperty('error');
    expect(mocks.getCandles).not.toHaveBeenCalled();
  });

  it('rejects an inverted candle time range', async () => {
    const response = await request(createApp())
      .get('/api/candles/AAPL/1m?from=2000&to=1000')
      .expect(400);

    expect(response.body.errorCode).toBe('INVALID_TIME_RANGE');
    expect(mocks.getCandles).not.toHaveBeenCalled();
  });

  it('rejects an invalid candle time value', async () => {
    const response = await request(createApp())
      .get('/api/candles/AAPL/1m?from=not-a-date')
      .expect(400);

    expect(response.body.errorCode).toBe('INVALID_TIME_RANGE');
    expect(mocks.getCandles).not.toHaveBeenCalled();
  });

  it('rejects an invalid symbol', async () => {
    const response = await request(createApp())
      .get('/api/candles/%20/1m')
      .expect(400);

    expect(response.body.errorCode).toBe('INVALID_SYMBOL');
    expect(mocks.getCandles).not.toHaveBeenCalled();
  });

  it('returns the common error shape for an unknown route', async () => {
    const response = await request(createApp()).get('/unknown').expect(404);

    expect(response.body).toEqual({
      success: false,
      errorCode: 'NOT_FOUND',
      message: 'Cannot GET /unknown',
    });
  });

  it('returns the common error shape for an unknown error', async () => {
    mocks.getCandles.mockRejectedValueOnce(new Error('unexpected failure'));

    const response = await request(createApp())
      .get('/api/candles/AAPL/1m')
      .expect(500);

    expect(response.body).toEqual({
      success: false,
      errorCode: 'INTERNAL_SERVER_ERROR',
      message: 'unexpected failure',
    });
  });

  it('refreshes a supported aggregate timeframe', async () => {
    const response = await request(createApp())
      .post('/api/aggregate/refresh')
      .send({ timeframe: '5m', from: 1000, to: 2000 })
      .expect(200);

    expect(mocks.refreshContinuousAggregate).toHaveBeenCalledWith(
      '5m',
      1000,
      2000
    );
    expect(response.body).toMatchObject({
      success: true,
      timeframe: '5m',
    });
  });

  it.each([
    [{}, 'INVALID_TIMEFRAME'],
    [{ timeframe: '1m' }, 'INVALID_TIMEFRAME'],
    [
      { timeframe: '5m', from: 2000, to: 1000 },
      'INVALID_TIME_RANGE',
    ],
  ])('rejects invalid aggregate refresh body %#', async (body, errorCode) => {
    const response = await request(createApp())
      .post('/api/aggregate/refresh')
      .send(body)
      .expect(400);

    expect(response.body.errorCode).toBe(errorCode);
    expect(mocks.refreshContinuousAggregate).not.toHaveBeenCalled();
  });
});
