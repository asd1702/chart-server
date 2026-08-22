import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

type ClientEvent = 'error' | 'end';

interface FakeClient {
  connect: ReturnType<typeof vi.fn>;
  query: ReturnType<typeof vi.fn>;
  end: ReturnType<typeof vi.fn>;
  emit: (event: ClientEvent, value?: Error) => void;
}

const mocks = vi.hoisted(() => ({
  acquiredResults: [] as boolean[],
  releasedResults: [] as boolean[],
  clients: [] as FakeClient[],
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock('pg', () => ({
  Client: class {
    private readonly handlers = new Map<ClientEvent, (value?: Error) => void>();
    connect = vi.fn().mockResolvedValue(undefined);
    query = vi.fn(async (statement: string) => {
      if (statement.includes('pg_try_advisory_lock')) {
        return { rows: [{ acquired: mocks.acquiredResults.shift() ?? false }] };
      }
      return { rows: [{ released: mocks.releasedResults.shift() ?? true }] };
    });
    end = vi.fn().mockResolvedValue(undefined);

    constructor() {
      mocks.clients.push(this as unknown as FakeClient);
    }

    once(event: ClientEvent, handler: (value?: Error) => void): this {
      this.handlers.set(event, handler);
      return this;
    }

    emit(event: ClientEvent, value?: Error): void {
      const handler = this.handlers.get(event);
      this.handlers.delete(event);
      handler?.(value);
    }
  },
}));

vi.mock('../../shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
  logger: { child: vi.fn(() => mocks.logger) },
}));

import { LeaderElectionService } from './postgres-advisory-leader-election';

describe('LeaderElectionService', () => {
  beforeEach(() => {
    mocks.acquiredResults.length = 0;
    mocks.releasedResults.length = 0;
    mocks.clients.length = 0;
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('starts active work only after its dedicated session acquires the lock', async () => {
    mocks.acquiredResults.push(true);
    const onLeadershipAcquired = vi.fn().mockResolvedValue(undefined);
    const election = new LeaderElectionService({
      databaseUrl: 'postgresql://user:password@localhost:5432/lab?schema=market',
      lockKey: 42,
      onLeadershipAcquired,
    });

    await election.start();

    expect(election.getState()).toBe('leader');
    expect(onLeadershipAcquired).toHaveBeenCalledOnce();
    expect(mocks.clients[0]?.query).toHaveBeenCalledWith(
      'SELECT pg_try_advisory_lock($1) AS acquired',
      [42],
    );

    await election.stop();
    expect(mocks.clients[0]?.query).toHaveBeenCalledWith(
      'SELECT pg_advisory_unlock($1) AS released',
      [42],
    );
    expect(mocks.clients[0]?.end).toHaveBeenCalledOnce();
  });

  it('reuses the same standby session across polling attempts and promotes it', async () => {
    vi.useFakeTimers();
    mocks.acquiredResults.push(false, true);
    const election = new LeaderElectionService({
      databaseUrl: 'postgresql://user:password@localhost:5432/lab',
      lockKey: 42,
      retryIntervalMs: 1_000,
    });

    await election.start();
    expect(election.getState()).toBe('standby');
    expect(mocks.clients).toHaveLength(1);

    await vi.advanceTimersByTimeAsync(1_000);

    expect(election.getState()).toBe('leader');
    expect(mocks.clients).toHaveLength(1);
    expect(mocks.clients[0]?.connect).toHaveBeenCalledOnce();
    expect(mocks.clients[0]?.query).toHaveBeenCalledTimes(2);

    await election.stop();
  });

  it('releases leadership after active startup failure but keeps its coordination session', async () => {
    mocks.acquiredResults.push(true);
    const election = new LeaderElectionService({
      databaseUrl: 'postgresql://user:password@localhost:5432/lab',
      lockKey: 42,
      retryIntervalMs: 60_000,
      onLeadershipAcquired: vi.fn().mockRejectedValue(new Error('RocksDB open failed')),
    });

    await election.start();

    expect(election.getState()).toBe('standby');
    expect(mocks.clients[0]?.query).toHaveBeenCalledWith(
      'SELECT pg_advisory_unlock($1) AS released',
      [42],
    );
    expect(mocks.clients[0]?.end).not.toHaveBeenCalled();

    await election.stop();
    expect(mocks.clients[0]?.end).toHaveBeenCalledOnce();
  });

  it('closes the coordination session when activation unlock returns false', async () => {
    mocks.acquiredResults.push(true);
    mocks.releasedResults.push(false);
    const election = new LeaderElectionService({
      databaseUrl: 'postgresql://user:password@localhost:5432/lab',
      lockKey: 42,
      retryIntervalMs: 60_000,
      onLeadershipAcquired: vi.fn().mockRejectedValue(new Error('RocksDB open failed')),
    });

    await election.start();

    expect(election.getState()).toBe('standby');
    expect(mocks.clients[0]?.end).toHaveBeenCalledOnce();
    expect(mocks.logger.warn).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ event: 'leader_election_release_uncertain' }),
    );

    await election.stop();
  });

  it('stops active work before scheduling takeover after session loss', async () => {
    mocks.acquiredResults.push(true);
    const events: string[] = [];
    const onLeadershipLost = vi.fn(async () => {
      events.push('active-stopped');
    });
    const election = new LeaderElectionService({
      databaseUrl: 'postgresql://user:password@localhost:5432/lab',
      lockKey: 42,
      retryIntervalMs: 60_000,
      onLeadershipLost,
    });

    await election.start();
    mocks.clients[0]?.emit('error', new Error('connection lost'));

    await vi.waitFor(() => expect(onLeadershipLost).toHaveBeenCalledOnce());
    expect(events).toEqual(['active-stopped']);
    expect(election.getState()).toBe('standby');

    await election.stop();
  });

  it('reconnects a lost standby session without stopping active work', async () => {
    mocks.acquiredResults.push(false);
    const onLeadershipLost = vi.fn().mockResolvedValue(undefined);
    const election = new LeaderElectionService({
      databaseUrl: 'postgresql://user:password@localhost:5432/lab',
      lockKey: 42,
      retryIntervalMs: 60_000,
      onLeadershipLost,
    });

    await election.start();
    mocks.clients[0]?.emit('error', new Error('standby connection lost'));

    await vi.waitFor(() => expect(mocks.logger.warn).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ event: 'leader_election_session_lost' }),
    ));
    expect(onLeadershipLost).not.toHaveBeenCalled();
    expect(mocks.clients[0]?.end).toHaveBeenCalledOnce();

    await election.stop();
  });
});
