import { afterEach, describe, expect, it, vi } from 'vitest';

const logger = {
  info: vi.fn(),
  error: vi.fn(),
  warn: vi.fn(),
};
const setLogComponent = vi.fn();

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
  vi.doUnmock('../../../src/modules/coordination/postgres-advisory-leader-election');
  vi.doUnmock('../../../src/modules/ingestion/active-ingestion.runtime');
  vi.doUnmock('../../../src/modules/observability/ingestor.metrics');
  vi.doUnmock('../../../src/modules/observability/metrics-server');
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
    const startCandlePersistence = vi.fn();

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
      startCandlePersistence,
    }));
    vi.doMock('../../../src/shared/db/prisma', () => ({
      prisma: { $disconnect: vi.fn().mockResolvedValue(undefined) },
    }));
    vi.doMock('../../../src/shared/utils/logger', () => ({
      getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
      logger: { child: vi.fn(() => logger) },
      setLogComponent,
    }));
    vi.spyOn(process, 'once').mockImplementation(() => process);

    const { startChartServer } = await import('../../../src/server.js');
    await startChartServer();

    expect(initWebSocketServer).toHaveBeenCalledWith(httpServer, subscriber);
    expect(httpServer.listen).toHaveBeenCalledWith(8080, expect.any(Function));
    expect(twelveDataConnect).not.toHaveBeenCalled();
    expect(startCandlePersistence).not.toHaveBeenCalled();
    expect(setLogComponent).toHaveBeenCalledWith('chart-server');
  });

  it('starts ingestion responsibilities in the Market Ingestor', async () => {
    const activeRuntimeStart = vi.fn().mockResolvedValue(undefined);
    const activeRuntimeStop = vi.fn().mockResolvedValue(undefined);
    const electionStart = vi.fn();
    const electionStop = vi.fn().mockResolvedValue(undefined);
    let electionOptions: {
      onLeadershipAcquired?: () => Promise<void>;
      onLeadershipLost?: (reason: string) => Promise<void>;
      onFatalError?: (error: Error) => Promise<void>;
    } | undefined;

    vi.doMock('../../../src/config', () => ({
      default: {
        TWELVE_DATA_API_KEY: 'test-key',
        market: {
          streamSymbols: ['BTC/USD'],
          historicalBackfillEnabled: false,
        },
        DATABASE_URL: 'postgresql://user:password@localhost:5432/lab',
        leaderElection: {
          lockKey: 424242,
          retryIntervalMs: 1000,
        },
        observability: {
          ingestorMetricsPort: 9464,
        },
      },
    }));
    vi.doMock('../../../src/modules/ingestion/active-ingestion.runtime', () => ({
      ActiveIngestionRuntime: class {
        start = activeRuntimeStart;
        stop = activeRuntimeStop;
      },
    }));
    vi.doMock('../../../src/modules/observability/ingestor.metrics', () => ({
      IngestorMetrics: class {
        registry = {};
      },
    }));
    vi.doMock('../../../src/modules/observability/metrics-server', () => ({
      MetricsServer: class {
        start = vi.fn().mockResolvedValue(undefined);
        stop = vi.fn().mockResolvedValue(undefined);
      },
    }));
    vi.doMock('../../../src/modules/coordination/postgres-advisory-leader-election', () => ({
      LeaderElectionService: class {
        constructor(options: typeof electionOptions) {
          electionOptions = options;
        }

        start = async () => {
          electionStart();
          await electionOptions?.onLeadershipAcquired?.();
        };

        stop = electionStop;
      },
    }));
    vi.doMock('../../../src/shared/db/prisma', () => ({
      prisma: { $disconnect: vi.fn().mockResolvedValue(undefined) },
    }));
    vi.doMock('../../../src/shared/utils/logger', () => ({
      getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
      logger: { child: vi.fn(() => logger) },
      setLogComponent,
    }));
    vi.spyOn(process, 'once').mockImplementation(() => process);

    const { startMarketIngestor } = await import('../../../src/ingestor.js');
    await startMarketIngestor();

    expect(setLogComponent).toHaveBeenCalledWith('market-ingestor');
    expect(electionStart).toHaveBeenCalledOnce();
    expect(activeRuntimeStart).toHaveBeenCalledOnce();
    expect(electionOptions).toEqual(expect.objectContaining({
      databaseUrl: 'postgresql://user:password@localhost:5432/lab',
      lockKey: 424242,
      retryIntervalMs: 1000,
    }));
    expect(logger.info).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        event: 'ingestor_configuration',
        streamSymbols: ['BTC/USD'],
        historicalBackfillEnabled: false,
      }),
    );
    expect(JSON.stringify(logger.info.mock.calls)).not.toContain('test-key');
  });

  it('terminates the Market Ingestor when leader cleanup reaches a fatal failure', async () => {
    const activeRuntimeStart =
      vi.fn().mockResolvedValue(undefined);

    const activeRuntimeStop =
      vi.fn().mockResolvedValue(undefined);

    let electionOptions: {
      onLeadershipAcquired?: () => Promise<void>;
      onLeadershipLost?: (reason: string) => Promise<void>;
      onFatalError?: (error: Error) => Promise<void>;
    } | undefined;

    vi.doMock('../../../src/config', () => ({
      default: {
        TWELVE_DATA_API_KEY: 'test-key',

        market: {
          streamSymbols: ['BTC/USD'],
          historicalBackfillEnabled: false,
        },

        DATABASE_URL:
          'postgresql://user:password@localhost:5432/lab',

        leaderElection: {
          lockKey: 424242,
          retryIntervalMs: 1000,
        },
        observability: {
          ingestorMetricsPort: 9464,
        },
      },
    }));

    vi.doMock(
      '../../../src/modules/ingestion/active-ingestion.runtime',
      () => ({
        ActiveIngestionRuntime: class {
          start = activeRuntimeStart;
          stop = activeRuntimeStop;
        },
      }),
    );
    vi.doMock('../../../src/modules/observability/ingestor.metrics', () => ({
      IngestorMetrics: class {
        registry = {};
      },
    }));
    vi.doMock('../../../src/modules/observability/metrics-server', () => ({
      MetricsServer: class {
        start = vi.fn().mockResolvedValue(undefined);
        stop = vi.fn().mockResolvedValue(undefined);
      },
    }));

    vi.doMock(
      '../../../src/modules/coordination/postgres-advisory-leader-election',
      () => ({
        LeaderElectionService: class {
          constructor(
            options: typeof electionOptions,
          ) {
            electionOptions = options;
          }

          start = vi.fn().mockResolvedValue(undefined);

          stop = vi.fn().mockResolvedValue(undefined);
        },
      }),
    );

    vi.doMock('../../../src/shared/db/prisma', () => ({
      prisma: {
        $disconnect:
          vi.fn().mockResolvedValue(undefined),
      },
    }));

    vi.doMock('../../../src/shared/utils/logger', () => ({
      getErrorMessage: (error: unknown) =>
        error instanceof Error
          ? error.message
          : 'Unknown error',

      logger: {
        child: vi.fn(() => logger),
      },

      setLogComponent,
    }));

    /*
    * installShutdownHandlers()가 signal handler를
    * 실제 process에 등록하지 않도록 막는다.
    */
    vi.spyOn(process, 'once')
      .mockImplementation(() => process);

    /*
    * 실제 테스트 프로세스가 종료되면 안 되므로
    * process.exit()만 spy로 대체한다.
    */
    const exitSpy = vi
      .spyOn(process, 'exit')
      .mockImplementation(
        (() => undefined) as () => never,
      );

    const { startMarketIngestor } =
      await import('../../../src/ingestor.js');

    await startMarketIngestor();

    expect(electionOptions?.onFatalError).toEqual(
      expect.any(Function),
    );

    const fatalError =
      new Error('RocksDB close failed');

    await electionOptions?.onFatalError?.(
      fatalError,
    );

    expect(logger.error).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        event:
          'ingestor_fatal_leadership_cleanup_failure',
        error: 'RocksDB close failed',
      }),
    );

    expect(exitSpy).toHaveBeenCalledOnce();

    expect(exitSpy).toHaveBeenCalledWith(1);

    /*
    * Fatal policy는 graceful shutdown을 다시 실행하는
    * 경로가 아니다.
    *
    * ActiveIngestionRuntime.stop()은 이미
    * LeaderElectionService의 onLeadershipLost 경로에서
    * 실패한 뒤 fatal callback까지 도달했다고 가정한다.
    */
    expect(activeRuntimeStop).not.toHaveBeenCalled();
  });
});
