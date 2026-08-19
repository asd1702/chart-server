import { afterEach, describe, expect, it, vi } from 'vitest';

const logger = {
  info: vi.fn(),
  error: vi.fn(),
};

afterEach(() => {
  vi.restoreAllMocks();
  vi.doUnmock('http');
  vi.doUnmock('../../../src/config');
  vi.doUnmock('../../../src/app');
  vi.doUnmock('../../../src/modules/realtime');
  vi.doUnmock('../../../src/modules/messaging/pubsub.factory');
  vi.doUnmock('../../../src/modules/market-data/twelvedata.provider');
  vi.doUnmock('../../../src/modules/market-data/historical-backfill.service');
  vi.doUnmock('../../../src/modules/candle/candle.persistence');
  vi.doUnmock('../../../src/shared/db/prisma');
  vi.doUnmock('../../../src/shared/utils/logger');
  vi.resetModules();
  vi.clearAllMocks();
});

describe('process separation startup', () => {
  it('starts only serving responsibilities in the Chart Server', async () => {
    const subscriber = {
      subscribe: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const httpServer = {
      once: vi.fn(),
      off: vi.fn(),
      listen: vi.fn((_port: number, callback: () => void) => callback()),
      close: vi.fn(),
    };
    const initWebSocketServer = vi.fn().mockResolvedValue(undefined);
    const twelveDataConnect = vi.fn();
    const candleFlusherStart = vi.fn();

    vi.doMock('http', () => ({
      default: { createServer: vi.fn(() => httpServer) },
    }));
    vi.doMock('../../../src/config', () => ({
      default: { port: 8080 },
    }));
    vi.doMock('../../../src/app', () => ({ createApp: vi.fn(() => ({})) }));
    vi.doMock('../../../src/modules/realtime', () => ({
      initWebSocketServer,
      closeWebSocketServer: vi.fn().mockResolvedValue(undefined),
    }));
    vi.doMock('../../../src/modules/messaging/pubsub.factory', () => ({
      createRedisPubSubService: vi.fn(() => subscriber),
    }));
    vi.doMock('../../../src/modules/market-data/twelvedata.provider', () => ({
      connectToTwelveData: twelveDataConnect,
    }));
    vi.doMock('../../../src/modules/candle/candle.persistence', () => ({
      candleFlusher: { start: candleFlusherStart },
    }));
    vi.doMock('../../../src/shared/db/prisma', () => ({
      prisma: { $disconnect: vi.fn().mockResolvedValue(undefined) },
    }));
    vi.doMock('../../../src/shared/utils/logger', () => ({
      logger: { child: vi.fn(() => logger) },
    }));
    vi.spyOn(process, 'once').mockImplementation(() => process);

    const { startChartServer } = await import('../../../src/server.js');
    await startChartServer();

    expect(initWebSocketServer).toHaveBeenCalledWith(httpServer, subscriber);
    expect(httpServer.listen).toHaveBeenCalledWith(8080, expect.any(Function));
    expect(twelveDataConnect).not.toHaveBeenCalled();
    expect(candleFlusherStart).not.toHaveBeenCalled();
  });

  it('starts ingestion responsibilities in the Market Ingestor', async () => {
    const publisher = {
      publish: vi.fn().mockResolvedValue(undefined),
      disconnect: vi.fn().mockResolvedValue(undefined),
    };
    const candleFlusherStart = vi.fn();
    const twelveDataConnect = vi.fn();
    const runHistoricalBackfill = vi.fn().mockResolvedValue(undefined);

    vi.doMock('../../../src/config', () => ({
      default: {
        TWELVE_DATA_API_KEY: 'test-key',
        market: {
          streamSymbols: ['BTC/USD'],
          historicalBackfillEnabled: false,
        },
      },
    }));
    vi.doMock('../../../src/modules/candle/candle.persistence', () => ({
      candleFlusher: { start: candleFlusherStart },
      closeCandlePersistence: vi.fn().mockResolvedValue(undefined),
    }));
    vi.doMock('../../../src/modules/messaging/pubsub.factory', () => ({
      createRedisPubSubService: vi.fn(() => publisher),
    }));
    vi.doMock('../../../src/modules/market-data/twelvedata.provider', () => ({
      connectToTwelveData: twelveDataConnect,
      disconnectFromTwelveData: vi.fn().mockResolvedValue(undefined),
    }));
    vi.doMock('../../../src/modules/market-data/historical-backfill.service', () => ({
      runHistoricalBackfill,
    }));
    vi.doMock('../../../src/shared/db/prisma', () => ({
      prisma: { $disconnect: vi.fn().mockResolvedValue(undefined) },
    }));
    vi.doMock('../../../src/shared/utils/logger', () => ({
      logger: { child: vi.fn(() => logger) },
    }));
    vi.spyOn(process, 'once').mockImplementation(() => process);

    const { startMarketIngestor } = await import('../../../src/ingestor.js');
    await startMarketIngestor();

    expect(candleFlusherStart).toHaveBeenCalledOnce();
    expect(twelveDataConnect).toHaveBeenCalledWith(publisher);
    expect(runHistoricalBackfill).not.toHaveBeenCalled();
    expect(logger.info).toHaveBeenCalledWith(
      'Market Ingestor configuration loaded',
      {
        event: 'ingestor_configuration',
        streamSymbols: ['BTC/USD'],
        historicalBackfillEnabled: false,
      },
    );
  });
});
