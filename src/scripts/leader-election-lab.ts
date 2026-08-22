import config from '../config';
import { LeaderElectionService } from '../modules/coordination/postgres-advisory-leader-election';
import {
  getErrorMessage,
  logger,
  setLogComponent,
} from '../shared/utils/logger';

setLogComponent('leader-election-lab');

const labLogger = logger.child({
  component: 'leader-election-lab',
});

async function main(): Promise<void> {
  labLogger.info('Leader Election Lab 프로세스를 시작합니다.', {
    event: 'leader_election_lab_started',
    pid: process.pid,
  });

  const election = new LeaderElectionService({
    databaseUrl: config.DATABASE_URL,
    lockKey: config.leaderElection.lockKey,
    retryIntervalMs: config.leaderElection.retryIntervalMs,
  });

  await election.start();

  let shuttingDown = false;

  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;

    labLogger.info('Leader Election Lab 종료 요청을 처리합니다.', {
      event: 'leader_election_lab_shutdown',
      signal,
      pid: process.pid,
    });

    await election.stop();

    process.exit(0);
  };

  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGTERM', () => void shutdown('SIGTERM'));
}

main().catch((error) => {
  labLogger.error('Leader Election Lab 실행에 실패했습니다.', {
    event: 'leader_election_lab_failed',
    error: getErrorMessage(error),
  });

  process.exit(1);
});
