import config from './config';
import { LeaderElectionService } from './modules/coordination/postgres-advisory-leader-election';
import { ActiveIngestionRuntime } from './modules/ingestion/active-ingestion.runtime';
import { prisma } from './shared/db/prisma';
import {
  getErrorMessage,
  logger,
  setLogComponent,
} from './shared/utils/logger';

const ingestorLogger = logger.child({
  component: 'market-ingestor',
});

export async function startMarketIngestor(): Promise<void> {
  setLogComponent('market-ingestor');

  if (!config.TWELVE_DATA_API_KEY.trim()) {
    throw new Error(
      'Market Ingestor requires TWELVE_DATA_API_KEY',
    );
  }

  ingestorLogger.info(
    'Market Ingestor 설정을 불러왔습니다.',
    {
      event: 'ingestor_configuration',
      streamSymbols: config.market.streamSymbols,
      historicalBackfillEnabled:
        config.market.historicalBackfillEnabled,
    },
  );

  const activeRuntime =
    new ActiveIngestionRuntime();

  const leaderElection =
    new LeaderElectionService({
      databaseUrl: config.DATABASE_URL,
      lockKey:
        config.leaderElection.lockKey,
      retryIntervalMs:
        config.leaderElection.retryIntervalMs,

      onLeadershipAcquired: async () => {
        ingestorLogger.info(
          'Leadership 획득으로 Active Ingestion을 시작합니다.',
          {
            event:
              'ingestor_leadership_acquired',
          },
        );

        await activeRuntime.start();
      },

      onLeadershipLost: async (
        reason,
      ) => {
        ingestorLogger.warn(
          'Leadership 상실로 Active Ingestion을 중지합니다.',
          {
            event:
              'ingestor_leadership_lost',
            reason,
          },
        );

        await activeRuntime.stop();
      },

      /*
       * Leadership을 잃은 뒤 Active workload cleanup까지
       * 실패했다면 이 process의 runtime state를 더 이상
       * 신뢰할 수 없다.
       *
       * LeaderElectionService는 이 fatal condition을
       * 판단하기만 하고, 실제 process termination 정책은
       * composition root인 ingestor.ts가 결정한다.
       *
       * 이 경로에서는 graceful shutdown을 다시 시도하지 않는다.
       * ActiveIngestionRuntime.stop()은 이미 best-effort cleanup을
       * 모두 수행한 뒤 실패를 반환한 상태이기 때문이다.
       *
       * exit code 1로 process를 종료해 Docker/Kubernetes 같은
       * supervisor가 fresh process로 복구할 수 있게 한다.
       */
      onFatalError: async (error) => {
        ingestorLogger.error(
          'Market Ingestor가 복구 불가능한 Leadership cleanup 오류를 감지했습니다.',
          {
            event:
              'ingestor_fatal_leadership_cleanup_failure',
            error:
              getErrorMessage(error),
          },
        );

        process.exit(1);
      },
    });

  installShutdownHandlers(
    activeRuntime,
    leaderElection,
  );

  await leaderElection.start();

  ingestorLogger.info(
    'Market Ingestor leader election을 시작했습니다.',
    {
      event: 'ingestor_started',
    },
  );
}

function installShutdownHandlers(
  activeRuntime: ActiveIngestionRuntime,
  leaderElection: LeaderElectionService,
): void {
  let isShuttingDown = false;

  const shutdown = async (
    signal: string,
  ): Promise<void> => {
    if (isShuttingDown) {
      return;
    }

    isShuttingDown = true;

    ingestorLogger.info(
      'Market Ingestor 종료 요청을 처리합니다.',
      {
        event: 'shutdown_requested',
        signal,
      },
    );

    const forceExitTimer = setTimeout(
      () => {
        ingestorLogger.error(
          'Market Ingestor 정상 종료 제한 시간을 초과했습니다.',
          {
            event: 'shutdown_timed_out',
          },
        );

        process.exit(1);
      },
      15_000,
    );

    forceExitTimer.unref();

    let firstError: unknown;

    /*
     * Release workload before ownership.
     *
     * Active workload를 먼저 완전히 정리해야
     * standby가 승격된 뒤 shared RocksDB path를
     * 안전하게 열 수 있다.
     */
    try {
      await activeRuntime.stop();
    } catch (error) {
      firstError ??= error;
    }

    /*
     * Active workload가 내려간 뒤에야
     * advisory lock ownership을 반환한다.
     */
    try {
      await leaderElection.stop();
    } catch (error) {
      firstError ??= error;
    }

    /*
     * 마지막으로 business-query Prisma pool을 닫는다.
     */
    try {
      await prisma.$disconnect();
    } catch (error) {
      firstError ??= error;
    } finally {
      clearTimeout(forceExitTimer);
    }

    if (firstError !== undefined) {
      ingestorLogger.error(
        'Market Ingestor 정상 종료에 실패했습니다.',
        {
          event: 'shutdown_failed',
          error:
            getErrorMessage(firstError),
        },
      );

      process.exit(1);
      return;
    }

    ingestorLogger.info(
      'Market Ingestor 종료를 완료했습니다.',
      {
        event: 'ingestor_stopped',
      },
    );

    process.exit(0);
  };

  process.once(
    'SIGTERM',
    () => void shutdown('SIGTERM'),
  );

  process.once(
    'SIGINT',
    () => void shutdown('SIGINT'),
  );

  process.once(
    'SIGUSR2',
    () => void shutdown('SIGUSR2'),
  );
}

if (require.main === module) {
  startMarketIngestor().catch(
    async (error) => {
      ingestorLogger.error(
        'Market Ingestor 시작에 실패했습니다.',
        {
          event: 'ingestor_start_failed',
          error:
            getErrorMessage(error),
        },
      );

      await prisma.$disconnect();

      process.exit(1);
    },
  );
}