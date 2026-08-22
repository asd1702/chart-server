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
 * Owns one dedicated PostgreSQL session for the whole election lifecycle.
 * In standby, the session polls the advisory lock. Once acquired, that exact
 * session becomes the session-level lock owner.
 */
export class LeaderElectionService {
  private readonly databaseUrl: string;
  private readonly lockKey: number;
  private readonly retryIntervalMs: number;
  private readonly onLeadershipAcquired: (() => Promise<void>) | undefined;
  private readonly onLeadershipLost: ((reason: string) => Promise<void>) | undefined;

  private state: LeaderElectionState = 'stopped';
  private coordinationClient: Client | null = null;
  private retryTimer: NodeJS.Timeout | null = null;
  private acquisitionInProgress = false;
  private stopped = true;
  private retryDisabled = false;
  private standbyAnnounced = false;
  private readonly closedClients = new WeakSet<Client>();

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

    const client = this.coordinationClient;
    const wasLeader = this.state === 'leader';
    this.coordinationClient = null;
    this.state = 'stopped';
    this.standbyAnnounced = false;

    if (client) {
      if (wasLeader) {
        await this.releaseAndCloseClient(client);
      } else {
        await this.closeClient(client);
      }
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
    let client: Client | null = null;

    try {
      client = await this.ensureCoordinationClient();
      electionLogger.debug('Leader advisory lock 획득을 시도합니다.', {
        event: 'leader_election_attempt',
        lockKey: this.lockKey,
      });

      const result = await client.query<{ acquired: boolean }>(
        'SELECT pg_try_advisory_lock($1) AS acquired',
        [this.lockKey],
      );

      if (this.stopped || this.retryDisabled) return;

      if (result.rows[0]?.acquired !== true) {
        this.state = 'standby';
        this.announceStandby();
        return;
      }

      // This same standby session now owns the advisory lock. Do not issue a
      // second pg_try_advisory_lock call while state is leader: session locks
      // are re-entrant and would increment the lock count.
      this.state = 'leader';
      this.standbyAnnounced = false;
      electionLogger.info('Leader ownership을 획득했습니다.', {
        event: 'leader_election_acquired',
        lockKey: this.lockKey,
      });

      try {
        await this.onLeadershipAcquired?.();
      } catch (error) {
        electionLogger.error('Leader 획득 후 Active workload 시작에 실패했습니다.', {
          event: 'leader_election_activation_failed',
          lockKey: this.lockKey,
          error: getErrorMessage(error),
        });
        await this.releaseLeadershipAfterActivationFailure(client);
      }
    } catch (error) {
      if (!this.stopped) this.state = 'standby';

      electionLogger.warn('Leader Election 시도 중 오류가 발생했습니다.', {
        event: 'leader_election_attempt_failed',
        lockKey: this.lockKey,
        error: getErrorMessage(error),
      });

      // A failed query leaves the session and lock ownership uncertain. Close
      // it so PostgreSQL is the final authority for cleanup, then reconnect.
      if (client && this.coordinationClient === client) {
        await this.closeCoordinationClient(client);
      }
    } finally {
      this.acquisitionInProgress = false;

      if (!this.stopped && !this.retryDisabled && this.state !== 'leader') {
        this.scheduleRetry();
      }
    }
  }

  private async ensureCoordinationClient(): Promise<Client> {
    if (this.coordinationClient) return this.coordinationClient;

    const client = new Client({
      connectionString: this.databaseUrl,
      application_name: 'market-ingestor-leader-election',
    });

    try {
      await client.connect();

      if (this.stopped || this.retryDisabled) {
        await this.closeClient(client);
        throw new Error('Leader Election stopped while connecting to PostgreSQL');
      }

      this.coordinationClient = client;
      this.installConnectionHandlers(client);
      electionLogger.info('Leader Election PostgreSQL session을 연결했습니다.', {
        event: 'leader_election_session_connected',
        lockKey: this.lockKey,
      });

      return client;
    } catch (error) {
      await this.closeClient(client);
      throw error;
    }
  }

  private announceStandby(): void {
    if (!this.standbyAnnounced) {
      this.standbyAnnounced = true;
      electionLogger.info('다른 replica가 Leader입니다. Standby로 대기합니다.', {
        event: 'leader_election_standby',
        lockKey: this.lockKey,
      });
      return;
    }

    electionLogger.debug('Standby 상태에서 Leader ownership 획득을 재시도합니다.', {
      event: 'leader_election_retry',
      lockKey: this.lockKey,
    });
  }

  private async releaseLeadershipAfterActivationFailure(client: Client): Promise<void> {
    if (this.coordinationClient !== client) return;

    try {
      const result = await client.query<{ released: boolean }>(
        'SELECT pg_advisory_unlock($1) AS released',
        [this.lockKey],
      );
      const released = result.rows[0]?.released === true;

      if (!released) {
        electionLogger.warn('Leader advisory lock 해제 상태를 확인할 수 없어 coordination session을 종료합니다.', {
          event: 'leader_election_release_uncertain',
          lockKey: this.lockKey,
        });
        await this.closeCoordinationClient(client);
        return;
      }

      electionLogger.info('Active workload 시작 실패로 Leader ownership을 반환했습니다.', {
        event: 'leader_election_released',
        lockKey: this.lockKey,
        released,
      });
    } catch (error) {
      electionLogger.warn('Leader ownership 반환에 실패해 coordination session을 종료합니다.', {
        event: 'leader_election_release_failed',
        lockKey: this.lockKey,
        error: getErrorMessage(error),
      });
      await this.closeCoordinationClient(client);
    } finally {
      this.state = 'standby';
      this.standbyAnnounced = false;
    }
  }

  private installConnectionHandlers(client: Client): void {
    client.once('error', (error) => {
      void this.handleCoordinationConnectionLost(
        client,
        `PostgreSQL connection error: ${getErrorMessage(error)}`,
      );
    });

    client.once('end', () => {
      void this.handleCoordinationConnectionLost(client, 'PostgreSQL connection ended');
    });
  }

  private async handleCoordinationConnectionLost(
    client: Client,
    reason: string,
  ): Promise<void> {
    if (this.stopped || this.coordinationClient !== client) return;

    this.coordinationClient = null;
    const wasLeader = this.state === 'leader';
    this.state = 'standby';
    this.standbyAnnounced = false;

    if (!wasLeader) {
      electionLogger.warn('Standby Leader Election PostgreSQL session을 상실했습니다.', {
        event: 'leader_election_session_lost',
        lockKey: this.lockKey,
        reason,
      });
      await this.closeClient(client);
      if (!this.retryDisabled) this.scheduleRetry();
      return;
    }

    electionLogger.warn('Leader ownership을 상실했습니다.', {
      event: 'leader_election_lost',
      lockKey: this.lockKey,
      reason,
    });

    try {
      await this.onLeadershipLost?.(reason);
    } catch (error) {
      this.retryDisabled = true;
      electionLogger.error('Leadership 상실 후 Active workload 정리에 실패했습니다.', {
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

  private async closeCoordinationClient(client: Client): Promise<void> {
    if (this.coordinationClient === client) {
      this.coordinationClient = null;
    }
    await this.closeClient(client);
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
