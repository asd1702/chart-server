import config from './config';
import { LeaderElectionService } from './modules/coordination/postgres-advisory-leader-election';
import { ActiveIngestionRuntime } from './modules/ingestion/active-ingestion.runtime';
import { prisma } from './shared/db/prisma';
import {
  getErrorMessage,
  logger,
  setLogComponent,
} from './shared/utils/logger';

const ingestorLogger = logger.child({ component: 'market-ingestor' });

export async function startMarketIngestor(): Promise<void> {
  setLogComponent('market-ingestor');

  if (!config.TWELVE_DATA_API_KEY.trim()) {
    throw new Error('Market Ingestor requires TWELVE_DATA_API_KEY');
  }

  ingestorLogger.info('Market Ingestor 설정을 불러왔습니다.', {
    event: 'ingestor_configuration',
    streamSymbols: config.market.streamSymbols,
    historicalBackfillEnabled: config.market.historicalBackfillEnabled,
  });

  const activeRuntime = new ActiveIngestionRuntime();
  const leaderElection = new LeaderElectionService({
    databaseUrl: config.DATABASE_URL,
    lockKey: config.leaderElection.lockKey,
    retryIntervalMs: config.leaderElection.retryIntervalMs,
    onLeadershipAcquired: async () => {
      ingestorLogger.info('Leadership 획득으로 Active Ingestion을 시작합니다.', {
        event: 'ingestor_leadership_acquired',
      });
      await activeRuntime.start();
    },
    onLeadershipLost: async (reason) => {
      ingestorLogger.warn('Leadership 상실로 Active Ingestion을 중지합니다.', {
        event: 'ingestor_leadership_lost',
        reason,
      });
      await activeRuntime.stop();
    },
  });

  installShutdownHandlers(activeRuntime, leaderElection);
  await leaderElection.start();

  ingestorLogger.info('Market Ingestor leader election을 시작했습니다.', {
    event: 'ingestor_started',
  });
}

function installShutdownHandlers(
  activeRuntime: ActiveIngestionRuntime,
  leaderElection: LeaderElectionService,
): void {
  let isShuttingDown = false;

  const shutdown = async (signal: string): Promise<void> => {
    if (isShuttingDown) return;
    isShuttingDown = true;

    ingestorLogger.info('Market Ingestor 종료 요청을 처리합니다.', {
      event: 'shutdown_requested',
      signal,
    });

    const forceExitTimer = setTimeout(() => {
      ingestorLogger.error('Market Ingestor 정상 종료 제한 시간을 초과했습니다.', {
        event: 'shutdown_timed_out',
      });
      process.exit(1);
    }, 15_000);
    forceExitTimer.unref();

    let firstError: unknown;

    // Release workload before ownership. This prevents a promoted standby from
    // opening the shared RocksDB path while this process still uses it.
    try {
      await activeRuntime.stop();
    } catch (error) {
      firstError ??= error;
    }

    try {
      await leaderElection.stop();
    } catch (error) {
      firstError ??= error;
    }

    try {
      await prisma.$disconnect();
    } catch (error) {
      firstError ??= error;
    } finally {
      clearTimeout(forceExitTimer);
    }

    if (firstError !== undefined) {
      ingestorLogger.error('Market Ingestor 정상 종료에 실패했습니다.', {
        event: 'shutdown_failed',
        error: getErrorMessage(firstError),
      });
      process.exit(1);
      return;
    }

    ingestorLogger.info('Market Ingestor 종료를 완료했습니다.', {
      event: 'ingestor_stopped',
    });
    process.exit(0);
  };

  process.once('SIGTERM', () => void shutdown('SIGTERM'));
  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGUSR2', () => void shutdown('SIGUSR2'));
}

if (require.main === module) {
  startMarketIngestor().catch(async (error) => {
    ingestorLogger.error('Market Ingestor 시작에 실패했습니다.', {
      event: 'ingestor_start_failed',
      error: getErrorMessage(error),
    });
    await prisma.$disconnect();
    process.exit(1);
  });
}
