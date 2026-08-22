import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CandleFlusher } from '../../../src/modules/candle/candle.flusher';
import { logger } from '../../../src/shared/utils/logger';
import type {
  PendingCandle,
  PendingCandleStore,
} from '../../../src/modules/candle/storage/pending-candle.store';

vi.mock('../../../src/shared', () => ({
  prisma: { candle1m: { createMany: vi.fn() } },
}));

vi.mock('../../../src/shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));

describe('CandleFlusher', () => {
  let pending: PendingCandle[];
  let store: PendingCandleStore;
  let createMany: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    vi.clearAllMocks();
    pending = [{
      key: 'pending|0000001700000000|AAPL',
      candle: {
        symbol: 'AAPL',
        startTime: 1_700_000_000,
        open: 100,
        high: 105,
        low: 98,
        close: 102,
        volume: 10,
      },
    }];
    store = {
      enqueue: vi.fn(),
      peek: vi.fn(async () => pending),
      ack: vi.fn(async (keys: string[]) => {
        pending = pending.filter(({ key }) => !keys.includes(key));
      }),
      close: vi.fn(),
    };
    createMany = vi.fn().mockResolvedValue({ count: 1 });
  });

  it('TimescaleDB 저장 성공 후에만 pending candle을 ACK한다', async () => {
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await expect(flusher.flush()).resolves.toBe(1);

    expect(createMany).toHaveBeenCalledWith(expect.objectContaining({ skipDuplicates: true }));
    expect(store.ack).toHaveBeenCalledWith([pendingKey()]);
    expect(createMany.mock.invocationCallOrder[0]).toBeLessThan(
      vi.mocked(store.ack).mock.invocationCallOrder[0]!,
    );
  });

  it('TimescaleDB 저장 실패 시 pending candle을 유지한다', async () => {
    createMany.mockRejectedValue(new Error('database unavailable'));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await expect(flusher.flush()).resolves.toBe(0);

    expect(store.ack).not.toHaveBeenCalled();
    expect(pending).toHaveLength(1);
  });

  it('TimescaleDB 저장 후 ACK 실패 시 idempotent 재시도를 위해 candle을 유지한다', async () => {
    vi.mocked(store.ack).mockRejectedValue(new Error('RocksDB unavailable'));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await expect(flusher.flush()).resolves.toBe(0);

    expect(createMany).toHaveBeenCalledOnce();
    expect(pending).toHaveLength(1);
  });

  it('동시에 시작된 flush를 중복 실행하지 않는다', async () => {
    let releaseWrite: (() => void) | undefined;
    createMany.mockReturnValue(new Promise<void>((resolve) => {
      releaseWrite = resolve;
    }));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    const first = flusher.flush();
    await Promise.resolve();
    await expect(flusher.flush()).resolves.toBe(0);
    releaseWrite?.();
    await expect(first).resolves.toBe(1);

    expect(createMany).toHaveBeenCalledOnce();
  });

  it('동일한 DB write 실패가 반복되면 최초 ERROR만 기록한다', async () => {
    createMany.mockRejectedValue(new Error('database unavailable'));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await flusher.flush();
    await flusher.flush();
    await flusher.flush();

    expect(logEvents(logger.error, 'candle_flush_write_failed')).toHaveLength(1);
  });

  it('반복 실패 후 실제 flush가 성공하면 recovery를 한 번 기록한다', async () => {
    createMany
      .mockRejectedValueOnce(new Error('database unavailable'))
      .mockRejectedValueOnce(new Error('database unavailable'))
      .mockRejectedValueOnce(new Error('database unavailable'));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await flusher.flush();
    await flusher.flush();
    await flusher.flush();
    await flusher.flush();

    expect(logEvents(logger.info, 'candle_flush_recovered')).toHaveLength(1);
  });

  it('복구 후 다시 발생한 DB write 실패를 새로운 transition으로 기록한다', async () => {
    const originalPending = pending[0]!;
    createMany
      .mockRejectedValueOnce(new Error('first outage'))
      .mockResolvedValueOnce({ count: 1 })
      .mockRejectedValueOnce(new Error('second outage'));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await flusher.flush();
    await flusher.flush();
    pending.push(originalPending);
    await flusher.flush();

    expect(logEvents(logger.error, 'candle_flush_write_failed')).toHaveLength(2);
    expect(logEvents(logger.info, 'candle_flush_recovered')).toHaveLength(1);
  });

  it('failure 종류가 write에서 ACK로 바뀌면 각각의 transition을 기록한다', async () => {
    createMany.mockRejectedValueOnce(new Error('database unavailable'));
    vi.mocked(store.ack).mockRejectedValueOnce(new Error('RocksDB unavailable'));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await flusher.flush();
    await flusher.flush();

    expect(logEvents(logger.error, 'candle_flush_write_failed')).toHaveLength(1);
    expect(logEvents(logger.error, 'candle_flush_ack_failed')).toHaveLength(1);
    expect(logEvents(logger.info, 'candle_flush_recovered')).toHaveLength(0);
  });

  it('read 실패 후 empty peek 성공을 복구로 처리하고 재발한 read 실패를 기록한다', async () => {
    vi.mocked(store.peek)
      .mockRejectedValueOnce(new Error('first RocksDB read failure'))
      .mockResolvedValueOnce([])
      .mockRejectedValueOnce(new Error('second RocksDB read failure'));
    const flusher = new CandleFlusher(store, { writer: { createMany } });

    await flusher.flush();
    await flusher.flush();
    await flusher.flush();

    expect(logEvents(logger.error, 'candle_flush_read_failed')).toHaveLength(2);
    expect(logEvents(logger.info, 'candle_flush_recovered')).toEqual([
      expect.arrayContaining([
        expect.any(String),
        expect.objectContaining({ recoveredFrom: 'read' }),
      ]),
    ]);
    expect(createMany).not.toHaveBeenCalled();
    expect(store.ack).not.toHaveBeenCalled();
  });

  function pendingKey(): string {
    return 'pending|0000001700000000|AAPL';
  }

  function logEvents(
    logMethod: unknown,
    event: string,
  ): unknown[][] {
    const calls = (logMethod as ReturnType<typeof vi.fn>).mock.calls;
    return calls.filter(([, metadata]) =>
      metadata
      && typeof metadata === 'object'
      && 'event' in metadata
      && metadata.event === event
    );
  }
});
