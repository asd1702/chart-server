import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import fs from 'fs';
import os from 'os';
import path from 'path';

import { RocksPendingCandleStore } from '../../../src/modules/candle/storage/rocks-pending-candle.store';
import type { Candle } from '../../../src/modules/candle/candle.types';

describe('RocksPendingCandleStore', () => {
  let dbPath: string;
  let store: RocksPendingCandleStore | null;

  beforeEach(() => {
    dbPath = fs.mkdtempSync(
      path.join(os.tmpdir(), 'rocks-candle-test-')
    );
    store = null;
  });

  afterEach(async () => {
    await store?.close();
    fs.rmSync(dbPath, {
      recursive: true,
      force: true,
    });
  });

  function createCandle(
    symbol: string,
    startTime: number,
  ): Candle {
    return {
      symbol,
      startTime,
      open: 100,
      high: 110,
      low: 90,
      close: 105,
      volume: 1000,
    };
  }

  it('enqueue한 candle을 peek으로 조회할 수 있다', async () => {
    store = new RocksPendingCandleStore(dbPath);

    await store.enqueue(
      createCandle('AAPL', 100)
    );

    const result = await store.peek(10);

    expect(result).toHaveLength(1);
    expect(result[0]?.candle).toEqual(createCandle('AAPL', 100));
  });

  it('ack한 candle은 더 이상 조회되지 않는다', async () => {
    store = new RocksPendingCandleStore(dbPath);

    await store.enqueue(
      createCandle('AAPL', 100)
    );

    const beforeAck = await store.peek(10);

    expect(beforeAck).toHaveLength(1);

    await store.ack([
      beforeAck[0]!.key,
    ]);

    const afterAck = await store.peek(10);

    expect(afterAck).toHaveLength(0);
  });

  it('DB를 닫았다 다시 열어도 pending candle이 유지된다', async () => {
    store = new RocksPendingCandleStore(dbPath);

    await store.enqueue(
      createCandle('AAPL', 100)
    );

    store.close();
    store = null;

    // 서버 재시작을 흉내낸다.
    store = new RocksPendingCandleStore(dbPath);

    const result = await store.peek(10);

    expect(result).toHaveLength(1);
    expect(result[0]?.candle.symbol).toBe('AAPL');
  });

  it('오래된 candle부터 조회된다', async () => {
    store = new RocksPendingCandleStore(dbPath);

    // 일부러 시간 순서와 반대로 저장
    await store.enqueue(
      createCandle('AAPL', 300)
    );

    await store.enqueue(
      createCandle('AAPL', 100)
    );

    await store.enqueue(
      createCandle('AAPL', 200)
    );

    const result = await store.peek(10);

    expect(result.map(it => it.candle.startTime))
      .toEqual([100, 200, 300]);
  });

  it('같은 symbol과 timestamp는 하나의 pending item으로 유지된다', async () => {
    store = new RocksPendingCandleStore(dbPath);
    const original = createCandle('BTC/USD', 100);

    await store.enqueue(original);
    await store.enqueue({ ...original, close: 107 });

    const result = await store.peek(10);
    expect(result).toHaveLength(1);
    expect(result[0]?.candle.close).toBe(107);
  });

  it('NaN 숫자 필드가 있는 candle은 저장하지 않는다', async () => {
    store = new RocksPendingCandleStore(dbPath);
    const invalidCandle: Candle = { ...createCandle('AAPL', 100), open: Number.NaN };

    await expect(store.enqueue(invalidCandle)).rejects.toThrow('Invalid candle open');
    await expect(store.peek(10)).resolves.toHaveLength(0);
  });

  it('Infinity 숫자 필드가 있는 candle은 저장하지 않는다', async () => {
    store = new RocksPendingCandleStore(dbPath);
    const invalidCandle: Candle = { ...createCandle('AAPL', 100), close: Number.POSITIVE_INFINITY };

    await expect(store.enqueue(invalidCandle)).rejects.toThrow('Invalid candle close');
    await expect(store.peek(10)).resolves.toHaveLength(0);
  });

  it.each([Number.NaN, 0, -1])(
    '유효하지 않은 timestamp %s가 있는 candle은 저장하지 않는다',
    async (startTime) => {
      store = new RocksPendingCandleStore(dbPath);
      const invalidCandle: Candle = createCandle('AAPL', startTime);

      await expect(store.enqueue(invalidCandle)).rejects.toThrow('Invalid candle startTime');
      await expect(store.peek(10)).resolves.toHaveLength(0);
    },
  );

  it('빈 symbol이 있는 candle은 저장하지 않는다', async () => {
    store = new RocksPendingCandleStore(dbPath);
    const invalidCandle: Candle = createCandle('', 100);

    await expect(store.enqueue(invalidCandle)).rejects.toThrow('Invalid candle symbol');
    await expect(store.peek(10)).resolves.toHaveLength(0);
  });
});
