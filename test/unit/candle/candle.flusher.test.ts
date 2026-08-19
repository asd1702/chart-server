import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CandleFlusher } from '../../../src/modules/candle/candle.flusher';
import type {
  PendingCandle,
  PendingCandleStore,
} from '../../../src/modules/candle/storage/pending-candle.store';

vi.mock('../../../src/shared', () => ({
  prisma: { candle1m: { createMany: vi.fn() } },
}));

vi.mock('../../../src/shared/utils/logger', () => ({
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

  function pendingKey(): string {
    return 'pending|0000001700000000|AAPL';
  }
});
