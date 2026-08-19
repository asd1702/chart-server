import { describe, expect, it } from 'vitest';
import { CandleMaker } from './candle.maker';

describe('CandleMaker', () => {
  const minuteStart = 1_700_000_040;

  it('creates the current candle from the first tick', () => {
    const maker = new CandleMaker();

    expect(maker.update('AAPL', 100, 3, minuteStart + 17)).toBeNull();
    expect(maker.getCurrentCandle()).toEqual({
      symbol: 'AAPL',
      startTime: minuteStart,
      open: 100,
      high: 100,
      low: 100,
      close: 100,
      volume: 3,
    });
  });

  it('updates OHLCV for ticks in the same minute', () => {
    const maker = new CandleMaker();

    maker.update('AAPL', 100, 1, minuteStart + 1);
    maker.update('AAPL', 105, 2, minuteStart + 10);
    maker.update('AAPL', 98, 3, minuteStart + 20);
    maker.update('AAPL', 102, 4, minuteStart + 50);

    expect(maker.getCurrentCandle()).toMatchObject({
      open: 100,
      high: 105,
      low: 98,
      close: 102,
      volume: 10,
    });
  });

  it('returns the completed candle when the next minute starts', () => {
    const maker = new CandleMaker();
    maker.update('AAPL', 100, 1, minuteStart + 5);
    maker.update('AAPL', 105, 2, minuteStart + 30);

    const completed = maker.update('AAPL', 103, 4, minuteStart + 65);

    expect(completed).toMatchObject({
      startTime: minuteStart,
      open: 100,
      high: 105,
      low: 100,
      close: 105,
      volume: 3,
    });
    expect(maker.getCurrentCandle()).toMatchObject({
      startTime: minuteStart + 60,
      open: 103,
      high: 103,
      low: 103,
      close: 103,
      volume: 4,
    });
  });

  it('clears the current candle on reset', () => {
    const maker = new CandleMaker();
    maker.update('AAPL', 100, 0, minuteStart);

    maker.reset();

    expect(maker.getCurrentCandle()).toBeNull();
  });
});
