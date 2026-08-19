import { describe, expect, it } from 'vitest';
import {
  isDailyTimeframe,
  parseTimeframe,
  validateTimeframe,
} from './candle.constants';

describe('timeframe utilities', () => {
  it.each([
    ['1m', 1],
    ['5m', 5],
    ['15m', 15],
    ['1h', 60],
    ['4h', 240],
  ])('parses %s as %i minutes', (timeframe, minutes) => {
    expect(parseTimeframe(timeframe)).toBe(minutes);
  });

  it.each(['1D', '1W', '1M'])('recognizes %s as a daily timeframe', (timeframe) => {
    expect(isDailyTimeframe(timeframe)).toBe(true);
  });

  it.each(['1D', '1W', '1M'])('rejects %s in parseTimeframe', (timeframe) => {
    expect(() => parseTimeframe(timeframe)).toThrow();
  });

  it.each(['1m', '5m', '15m', '1h', '4h', '1D', '1W', '1M'])(
    'accepts valid timeframe %s',
    (timeframe) => {
      expect(() => validateTimeframe(timeframe)).not.toThrow();
    }
  );

  it.each(['3m', '2h', 'abc', ''])('rejects invalid timeframe %s', (timeframe) => {
    expect(() => validateTimeframe(timeframe)).toThrow();
  });
});
