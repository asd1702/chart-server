import { Client } from 'pg';
import { getErrorMessage, logger } from '../../shared/utils/logger';

export type LeaderElectionState =
  | 'stopped'
  | 'standby'
  | 'activating'
  | 'leader'
  | 'deactivating'
  | 'failed';

export interface LeaderElectionOptions {
  databaseUrl: string;
  lockKey: number;
  retryIntervalMs?: number;
  onLeadershipAcquired?: () => Promise<void>;
  onLeadershipLost?: (reason: string) => Promise<void>;
  onFatalError?: (error: Error) => Promise<void>;
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
 private readonly onFatalError: ((error: Error) => Promise<void>) | undefined;

  private state: LeaderElectionState = 'stopped';
  private coordinationClient: Client | null = null;
  private retryTimer: NodeJS.Timeout | null = null;
  private acquisitionInProgress = false;
  private stopped = true;
  private retryDisabled = false;
  private standbyAnnounced = false;

  /**
   * Serializes Active workload lifecycle transitions.
   *
   * Leadership activation and deactivation must never overlap:
   *
   *   activation
   *      ↓
   *   deactivation
   *
   * Even if PostgreSQL ownership is lost while activation is still running.
   */
  private lifecycleTransition: Promise<void> = Promise.resolve();

  private readonly closedClients = new WeakSet<Client>();

  constructor(options: LeaderElectionOptions) {
    this.databaseUrl = removePrismaOnlyParams(options.databaseUrl);
    this.lockKey = options.lockKey;
    this.retryIntervalMs = options.retryIntervalMs ?? 1_000;
    this.onLeadershipAcquired = options.onLeadershipAcquired;
    this.onLeadershipLost = options.onLeadershipLost;
    this.onFatalError = options.onFatalError;
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
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    this.retryDisabled = true;
    this.clearRetryTimer();

    const client = this.coordinationClient;

    const ownsLeadership =
      this.state === 'activating' ||
      this.state === 'leader';

    this.coordinationClient = null;
    this.state = 'stopped';
    this.standbyAnnounced = false;

    if (client) {
      if (ownsLeadership) {
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
      this.state !== 'standby' ||
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

      if (
        this.stopped ||
        this.retryDisabled ||
        this.coordinationClient !== client
      ) {
        return;
      }

      if (result.rows[0]?.acquired !== true) {
        this.state = 'standby';
        this.announceStandby();
        return;
      }

      /*
      * This exact PostgreSQL session now owns the session-level
      * advisory lock.
      *
      * Ownership has been acquired, but the Active workload is not
      * ready yet. Therefore the lifecycle state is ACTIVATING rather
      * than LEADER.
      */
      this.state = 'activating';
      this.standbyAnnounced = false;

      electionLogger.info('Leader ownership을 획득했습니다.', {
        event: 'leader_election_acquired',
        lockKey: this.lockKey,
      });

      try {
        /*
        * Active workload transitions are serialized.
        *
        * If PostgreSQL ownership is lost while this callback is
        * running, handleCoordinationConnectionLost() will enqueue
        * deactivation behind this activation instead of running both
        * callbacks concurrently.
        */
        await this.runLifecycleTransition(async () => {
          await this.onLeadershipAcquired?.();
        });
      } catch (error) {
        electionLogger.error(
          'Leader 획득 후 Active workload 시작에 실패했습니다.',
          {
            event: 'leader_election_activation_failed',
            lockKey: this.lockKey,
            error: getErrorMessage(error),
          },
        );

        /*
        * The same coordination session still owns the advisory lock.
        * Explicitly return it before going back to standby.
        *
        * If ownership was already lost while activation was running,
        * the connection-loss path owns cleanup instead.
        */
        if (
          !this.stopped &&
          this.coordinationClient === client
        ) {
          await this.releaseLeadershipAfterActivationFailure(client);
        }

        return;
      }

      /*
      * IMPORTANT:
      *
      * An await occurred above. Ownership may have changed while
      * Active workload startup was running.
      *
      * Never promote ACTIVATING -> LEADER without re-validating that
      * the exact PostgreSQL session is still our coordination owner.
      */
      if (
        this.stopped ||
        this.retryDisabled ||
        this.coordinationClient !== client ||
        this.state !== 'activating'
      ) {
        return;
      }

      this.state = 'leader';
    } catch (error) {
      /*
      * Do not overwrite an in-progress deactivation state.
      * Connection-loss handling may already own the lifecycle.
      */
      if (
        !this.stopped && this.getState() !== 'deactivating'
      ) {
        this.state = 'standby';
      }

      electionLogger.warn('Leader Election 시도 중 오류가 발생했습니다.', {
        event: 'leader_election_attempt_failed',
        lockKey: this.lockKey,
        error: getErrorMessage(error),
      });

      /*
      * A failed query leaves session / lock ownership uncertain.
      * Close the session and let PostgreSQL be the final cleanup
      * authority.
      */
      if (
        client &&
        this.coordinationClient === client
      ) {
        await this.closeCoordinationClient(client);
      }
    } finally {
      this.acquisitionInProgress = false;

      if (
        !this.stopped &&
        !this.retryDisabled &&
        this.state === 'standby'
      ) {
        this.scheduleRetry();
      }
    }
  }

  private runLifecycleTransition<T>(
    operation: () => Promise<T>,
  ): Promise<T> {
    /*
    * Both success and failure of the previous transition allow the
    * next transition to run.
    *
    * The returned promise still preserves the current operation's
    * rejection for its caller, while lifecycleTransition itself is
    * normalized back to Promise<void> so one failure does not poison
    * the queue permanently.
    */
    const current = this.lifecycleTransition.then(
      operation,
      operation,
    );

    this.lifecycleTransition = current.then(
      () => undefined,
      () => undefined,
    );

    return current;
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

  private async releaseLeadershipAfterActivationFailure(
    client: Client,
  ): Promise<void> {
    if (this.coordinationClient !== client) {
      return;
    }

    try {
      const result = await client.query<{ released: boolean }>(
        'SELECT pg_advisory_unlock($1) AS released',
        [this.lockKey],
      );

      const released =
        result.rows[0]?.released === true;

      if (!released) {
        electionLogger.warn(
          'Leader advisory lock 해제 상태를 확인할 수 없어 coordination session을 종료합니다.',
          {
            event: 'leader_election_release_uncertain',
            lockKey: this.lockKey,
          },
        );

        await this.closeCoordinationClient(client);
        return;
      }

      electionLogger.info(
        'Active workload 시작 실패로 Leader ownership을 반환했습니다.',
        {
          event: 'leader_election_released',
          lockKey: this.lockKey,
          released,
        },
      );
    } catch (error) {
      electionLogger.warn(
        'Leader ownership 반환에 실패해 coordination session을 종료합니다.',
        {
          event: 'leader_election_release_failed',
          lockKey: this.lockKey,
          error: getErrorMessage(error),
        },
      );

      await this.closeCoordinationClient(client);
    } finally {
      /*
      * Only the activation-failure path that still owns this
      * transition may return the state to standby.
      *
      * Never overwrite DEACTIVATING / STOPPED.
      */
      if (
        !this.stopped &&
        this.state === 'activating'
      ) {
        this.state = 'standby';
        this.standbyAnnounced = false;
      }
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
    if (
      this.stopped ||
      this.coordinationClient !== client
    ) {
      return;
    }

    /*
    * Detach the failed PostgreSQL session immediately.
    *
    * From this point onward this process must never consider the
    * session authoritative, even if an async activation that started
    * earlier eventually completes successfully.
    */
    this.coordinationClient = null;

    const hadLeadership =
      this.state === 'activating' ||
      this.state === 'leader';

    this.standbyAnnounced = false;

    /*
    * Standby session loss owns no Active workload.
    * Reconnect PostgreSQL only; do not invoke deactivation.
    */
    if (!hadLeadership) {
      this.state = 'standby';

      electionLogger.warn(
        'Standby Leader Election PostgreSQL session을 상실했습니다.',
        {
          event: 'leader_election_session_lost',
          lockKey: this.lockKey,
          reason,
        },
      );

      await this.closeClient(client);

      if (
        !this.stopped &&
        !this.retryDisabled
      ) {
        this.scheduleRetry();
      }

      return;
    }

    /*
    * We either had a fully active leader or lost ownership while
    * activation was still in progress.
    *
    * Do NOT execute onLeadershipLost concurrently with activation.
    */
    this.state = 'deactivating';

    electionLogger.warn('Leader ownership을 상실했습니다.', {
      event: 'leader_election_lost',
      lockKey: this.lockKey,
      reason,
    });

    let deactivationError: unknown;

    try {
      await this.runLifecycleTransition(async () => {
        await this.onLeadershipLost?.(reason);
      });
    } catch (error) {
      /*
      * Fail closed.
      *
      * We cannot prove that the old Active workload was fully stopped,
      * so this process must not attempt to become leader again.
      */
      deactivationError = error;
    } finally {
      await this.closeClient(client);
    }

    if (deactivationError !== undefined) {
      this.retryDisabled = true;
      this.state = 'failed';

      const fatalError =
        deactivationError instanceof Error
          ? deactivationError
          : new Error(
              `Leadership deactivation failed: ${getErrorMessage(
                deactivationError,
              )}`,
            );

      electionLogger.error(
        'Leadership 상실 후 Active workload 정리에 실패했습니다.',
        {
          event: 'leader_election_deactivation_failed',
          lockKey: this.lockKey,
          reason,
          error: getErrorMessage(deactivationError),
        },
      );

      try {
        await this.onFatalError?.(fatalError);
      } catch (fatalHandlerError) {
        electionLogger.error(
          'Leader Election fatal error handler 실행에 실패했습니다.',
          {
            event: 'leader_election_fatal_handler_failed',
            lockKey: this.lockKey,
            error: getErrorMessage(fatalHandlerError),
          },
        );
      }

      return;
    }

    if (this.stopped) {
      return;
    }

    this.state = 'standby';

    if (!this.retryDisabled) {
      this.scheduleRetry();
    }
  }

  private scheduleRetry(): void {
    if (
      this.stopped ||
      this.retryDisabled ||
      this.state !== 'standby' ||
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
