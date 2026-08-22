import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  startupError: null as Error | null,
  storeConstructor: vi.fn(),
  storeEnqueue: vi.fn(),
  storeClose: vi.fn(),
  flusherConstructor: vi.fn(),
  flusherStart: vi.fn(),
  flusherStop: vi.fn(),
  flusherNotifyEnqueued: vi.fn(),
  logger: {
    error: vi.fn(),
    info: vi.fn(),
  },
}));

vi.mock('../../../src/config', () => ({
  default: { ROCKSDB_PATH: '/test/rocksdb/candles' },
}));

vi.mock('../../../src/shared/utils/logger', () => ({
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
  logger: mocks.logger,
}));

vi.mock('../../../src/modules/candle/storage/rocks-pending-candle.store', () => ({
  RocksPendingCandleStore: class {
    constructor(path: string) {
      mocks.storeConstructor(path);
      if (mocks.startupError) throw mocks.startupError;
    }

    enqueue(candle: unknown): Promise<void> {
      return mocks.storeEnqueue(candle);
    }

    close(): Promise<void> {
      return mocks.storeClose();
    }
  },
}));

vi.mock('../../../src/modules/candle/candle.flusher', () => ({
  CandleFlusher: class {
    constructor(store: unknown) {
      mocks.flusherConstructor(store);
    }

    start(): void {
      mocks.flusherStart();
    }

    stop(): Promise<void> {
      return mocks.flusherStop();
    }

    notifyEnqueued(): void {
      mocks.flusherNotifyEnqueued();
    }
  },
}));

type PersistenceModule = typeof import('../../../src/modules/candle/candle.persistence.js');

describe('candle persistence lifecycle', () => {
  let persistence: PersistenceModule | null;

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    mocks.startupError = null;
    mocks.storeEnqueue.mockResolvedValue(undefined);
    mocks.storeClose.mockResolvedValue(undefined);
    mocks.flusherStop.mockResolvedValue(undefined);
    persistence = null;
  });

  afterEach(async () => {
    await persistence?.closeCandlePersistence();
  });

  it('module import 시 RocksDB를 열지 않고 명시적 start에서만 생성한다', async () => {
    const module = await import('../../../src/modules/candle/candle.persistence.js');
    persistence = module;

    expect(mocks.storeConstructor).not.toHaveBeenCalled();
    expect(mocks.flusherConstructor).not.toHaveBeenCalled();
    expect(mocks.flusherStart).not.toHaveBeenCalled();

    module.startCandlePersistence();

    expect(mocks.storeConstructor).toHaveBeenCalledOnce();
    expect(mocks.storeConstructor).toHaveBeenCalledWith('/test/rocksdb/candles');
    expect(mocks.flusherConstructor).toHaveBeenCalledOnce();
    expect(mocks.flusherStart).toHaveBeenCalledOnce();
  });

  it('RocksDB startup 실패를 구조화 로그로 남기고 원래 오류를 전파한다', async () => {
    const startupError = new Error('native RocksDB open failure');
    mocks.startupError = startupError;
    const module = await import('../../../src/modules/candle/candle.persistence.js');
    persistence = module;

    expect(() => module.startCandlePersistence()).toThrow(startupError);
    expect(mocks.logger.error).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        subsystem: 'candle-persistence',
        event: 'candle_persistence_start_failed',
        path: '/test/rocksdb/candles',
        error: 'native RocksDB open failure',
      }),
    );
  });

  it('start 전에 enqueue하면 programming error를 발생시킨다', async () => {
    const module = await import('../../../src/modules/candle/candle.persistence.js');
    persistence = module;

    expect(() => module.enqueueCandle({
      symbol: 'BTC/USD',
      startTime: 1_700_000_000,
      open: 100,
      high: 101,
      low: 99,
      close: 100,
      volume: 0,
    })).toThrow('Candle persistence is not started');
  });

  it('enqueue 성공 후 flusher에 새 pending candle을 알린다', async () => {
    const module = await import('../../../src/modules/candle/candle.persistence.js');
    persistence = module;
    const candle = {
      symbol: 'BTC/USD',
      startTime: 1_700_000_000,
      open: 100,
      high: 101,
      low: 99,
      close: 100,
      volume: 0,
    };

    module.startCandlePersistence();
    await module.enqueueCandle(candle);

    expect(mocks.storeEnqueue).toHaveBeenCalledWith(candle);
    expect(mocks.flusherNotifyEnqueued).toHaveBeenCalledOnce();
  });

  it('close 후 같은 process에서 persistence를 다시 시작할 수 있다', async () => {
    const module = await import('../../../src/modules/candle/candle.persistence.js');
    persistence = module;

    await module.closeCandlePersistence();
    module.startCandlePersistence();
    await module.closeCandlePersistence();
    module.startCandlePersistence();

    expect(mocks.storeConstructor).toHaveBeenCalledTimes(2);
    expect(mocks.flusherStart).toHaveBeenCalledTimes(2);
    expect(mocks.flusherStop).toHaveBeenCalledOnce();
    expect(mocks.storeClose).toHaveBeenCalledOnce();
  });

  it('close 실패 후에도 lifecycle state를 reset한다', async () => {
    const closeError = new Error('native RocksDB close failure');
    mocks.storeClose.mockRejectedValueOnce(closeError);
    const module = await import('../../../src/modules/candle/candle.persistence.js');
    persistence = module;

    module.startCandlePersistence();
    await expect(module.closeCandlePersistence()).rejects.toThrow(closeError);
    module.startCandlePersistence();

    expect(mocks.storeConstructor).toHaveBeenCalledTimes(2);
    expect(mocks.flusherStart).toHaveBeenCalledTimes(2);
  });
});
