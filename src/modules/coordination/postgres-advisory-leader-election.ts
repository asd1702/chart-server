import { Client } from 'pg';
import { getErrorMessage, logger } from '../../shared/utils/logger';

export type LeaderElectionState = 'stopped' | 'standby' | 'leader';

export interface LeaderElectionOptions {
  databaseUrl: string;
  lockKey: number;
  retryIntervalMs?: number;
  onLeadershipAcquired?: () => Promise<void>;
  onLeadershipLost?: (reason: string) => Promise<void>;
}

const electionLogger = logger.child({
  subsystem: 'leader-election',
});

/**
 * Keeps a dedicated PostgreSQL session open while this process owns the
 * advisory lock. It intentionally knows nothing about ingestion resources;
 * the composition root supplies the active/standby lifecycle callbacks.
 */
export class LeaderElectionService {
  private readonly databaseUrl: string;
  private readonly lockKey: number;
  private readonly retryIntervalMs: number;
  private readonly onLeadershipAcquired: (() => Promise<void>) | undefined;
  private readonly onLeadershipLost: ((reason: string) => Promise<void>) | undefined;

  private state: LeaderElectionState = 'stopped';
  private leaderClient: Client | null = null;
  private retryTimer: NodeJS.Timeout | null = null;
  private acquisitionInProgress = false;
  private stopped = true;
  private retryDisabled = false;
  private readonly closedClients = new WeakSet<Client>();
  private standbyAnnounced = false;

  constructor(options: LeaderElectionOptions) {
    this.databaseUrl = removePrismaOnlyParams(options.databaseUrl);
    this.lockKey = options.lockKey;
    this.retryIntervalMs = options.retryIntervalMs ?? 1_000;
    this.onLeadershipAcquired = options.onLeadershipAcquired;
    this.onLeadershipLost = options.onLeadershipLost;
  }

  getState(): LeaderElectionState {
    return this.state;
  }

  async start(): Promise<void> {
    if (!this.stopped) return;

    this.stopped = false;
    this.retryDisabled = false;
    this.standbyAnnounced = false;
    this.state = 'standby';

    electionLogger.info('Leader Election을 시작합니다.', {
      event: 'leader_election_started',
      lockKey: this.lockKey,
      retryIntervalMs: this.retryIntervalMs,
    });

    await this.tryAcquireLeadership();
  }

  async stop(): Promise<void> {
    if (this.stopped) return;

    this.stopped = true;
    this.retryDisabled = true;
    this.clearRetryTimer();
    this.state = 'stopped';
    this.standbyAnnounced = false;

    const client = this.leaderClient;
    this.leaderClient = null;

    if (client) {
      await this.releaseAndCloseClient(client);
    }

    electionLogger.info('Leader Election을 중지했습니다.', {
      event: 'leader_election_stopped',
    });
  }

  private async tryAcquireLeadership(): Promise<void> {
    if (
      this.stopped ||
      this.retryDisabled ||
      this.state === 'leader' ||
      this.acquisitionInProgress
    ) {
      return;
    }

    this.acquisitionInProgress = true;
    const client = new Client({
      connectionString: this.databaseUrl,
      application_name: 'market-ingestor-leader-election',
    });
    let keepConnection = false;

    try {
      electionLogger.debug('Leader advisory lock 획득을 시도합니다.', {
        event: 'leader_election_attempt',
        lockKey: this.lockKey,
      });

      await client.connect();
      const result = await client.query<{ acquired: boolean }>(
        'SELECT pg_try_advisory_lock($1) AS acquired',
        [this.lockKey],
      );

      if (this.stopped || this.retryDisabled) return;

      if (result.rows[0]?.acquired !== true) {
        this.state = 'standby';

        if(!this.standbyAnnounced){
          this.standbyAnnounced = true;

          electionLogger.info('다른 replica가 Leader입니다. Standby로 대기합니다.', {
            event: 'leader_election_standby',
            lockKey: this.lockKey,
          },);
        } else{
          electionLogger.debug('Standby 상태에서 Leader ownership 획득을 재시도합니다.', {
            event: 'leader_election_retry',
            lockKey: this.lockKey,
          },);
        }
        
        return;
      }

      keepConnection = true;
      this.leaderClient = client;
      this.state = 'leader';
      this.standbyAnnounced = false;
      this.installLeaderConnectionHandlers(client);

      electionLogger.info('Leader ownership을 획득했습니다.', {
        event: 'leader_election_acquired',
        lockKey: this.lockKey,
      });

      try {
        await this.onLeadershipAcquired?.();
      } catch (error) {
        electionLogger.error('Active workload 시작에 실패하여 leadership을 반환합니다.', {
          event: 'leader_election_activation_failed',
          lockKey: this.lockKey,
          error: getErrorMessage(error),
        });

        if (this.leaderClient === client) {
          this.leaderClient = null;
          this.state = 'standby';
          await this.releaseAndCloseClient(client);
          keepConnection = false;
        }
      }
    } catch (error) {
      if (!this.stopped) this.state = 'standby';
      electionLogger.warn('Leader Election 시도 중 오류가 발생했습니다.', {
        event: 'leader_election_attempt_failed',
        lockKey: this.lockKey,
        error: getErrorMessage(error),
      });
    } finally {
      this.acquisitionInProgress = false;

      if (!keepConnection) {
        await this.closeClient(client);
      }

      if (!this.stopped && !this.retryDisabled && this.state !== 'leader') {
        this.scheduleRetry();
      }
    }
  }

  private installLeaderConnectionHandlers(client: Client): void {
    client.once('error', (error) => {
      void this.handleLeadershipLost(
        client,
        `PostgreSQL connection error: ${getErrorMessage(error)}`,
      );
    });

    client.once('end', () => {
      void this.handleLeadershipLost(client, 'PostgreSQL connection ended');
    });
  }

  private async handleLeadershipLost(client: Client, reason: string): Promise<void> {
    if (
      this.stopped ||
      this.state !== 'leader' ||
      this.leaderClient !== client
    ) {
      return;
    }

    this.leaderClient = null;
    this.state = 'standby';
    this.standbyAnnounced = false;

    electionLogger.warn('Leader ownership을 상실했습니다.', {
      event: 'leader_election_lost',
      lockKey: this.lockKey,
      reason,
    });

    try {
      await this.onLeadershipLost?.(reason);
    } catch (error) {
      // Do not become leader again until an operator resolves uncertainty about
      // whether the previous active workload actually stopped.
      this.retryDisabled = true;
      electionLogger.error('Active workload 종료에 실패해 leader 재시도를 중단합니다.', {
        event: 'leader_election_deactivation_failed',
        lockKey: this.lockKey,
        reason,
        error: getErrorMessage(error),
      });
      return;
    } finally {
      await this.closeClient(client);
    }

    if (!this.stopped && !this.retryDisabled) {
      this.scheduleRetry();
    }
  }

  private scheduleRetry(): void {
    if (
      this.stopped ||
      this.retryDisabled ||
      this.state === 'leader' ||
      this.retryTimer
    ) {
      return;
    }

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      void this.tryAcquireLeadership();
    }, this.retryIntervalMs);
  }

  private clearRetryTimer(): void {
    if (!this.retryTimer) return;
    clearTimeout(this.retryTimer);
    this.retryTimer = null;
  }

  private async releaseAndCloseClient(client: Client): Promise<void> {
    try {
      const result = await client.query<{ released: boolean }>(
        'SELECT pg_advisory_unlock($1) AS released',
        [this.lockKey],
      );
      electionLogger.info('Leader advisory lock을 해제했습니다.', {
        event: 'leader_election_released',
        lockKey: this.lockKey,
        released: result.rows[0]?.released ?? false,
      });
    } catch (error) {
      electionLogger.warn('Leader advisory lock 명시적 해제에 실패했습니다. Session 종료로 정리합니다.', {
        event: 'leader_election_release_failed',
        lockKey: this.lockKey,
        error: getErrorMessage(error),
      });
    } finally {
      await this.closeClient(client);
    }
  }

  private async closeClient(client: Client): Promise<void> {
    if (this.closedClients.has(client)) return;
    this.closedClients.add(client);
    await client.end().catch(() => undefined);
  }
}

function removePrismaOnlyParams(databaseUrl: string): string {
  const url = new URL(databaseUrl);
  url.searchParams.delete('schema');
  return url.toString();
}
