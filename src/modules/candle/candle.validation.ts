import { ValidationError } from '../../shared/types/common.types';
import { toEpochSec } from '../../shared/utils/time.util';
import {
  AGGREGATE_REFRESH_TIMEFRAMES,
  validateTimeframe,
} from './candle.constants';

const SYMBOL_PATTERN = /^[A-Za-z0-9._/-]{1,20}$/;
const INTEGER_PATTERN = /^\d+$/;

export function parseSymbol(value: string): string {
  const symbol = value.trim();
  if (!SYMBOL_PATTERN.test(symbol)) {
    throw new ValidationError('INVALID_SYMBOL', 'Invalid symbol.');
  }
  return symbol;
}

export function parseCandleTimeframe(value: string): string {
  try {
    validateTimeframe(value);
  } catch {
    throw new ValidationError('INVALID_TIMEFRAME', 'Unsupported timeframe.');
  }
  return value;
}

export function parseAggregateTimeframe(value: unknown): string {
  if (
    typeof value !== 'string' ||
    !AGGREGATE_REFRESH_TIMEFRAMES.includes(
      value as (typeof AGGREGATE_REFRESH_TIMEFRAMES)[number]
    )
  ) {
    throw new ValidationError('INVALID_TIMEFRAME', 'Unsupported timeframe.');
  }
  return value;
}

export function parseLimit(value: unknown): number {
  if (value === undefined) return 1000;
  if (typeof value !== 'string' || !INTEGER_PATTERN.test(value)) {
    throw new ValidationError(
      'INVALID_LIMIT',
      'limit must be an integer between 1 and 5000.'
    );
  }

  const limit = Number(value);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 5000) {
    throw new ValidationError(
      'INVALID_LIMIT',
      'limit must be an integer between 1 and 5000.'
    );
  }
  return limit;
}

export function parseOptionalTime(value: unknown): number | undefined {
  if (value === undefined || value === null) return undefined;

  const epochSec = toEpochSec(value);
  if (epochSec === undefined || !Number.isFinite(epochSec)) {
    throw new ValidationError('INVALID_TIME_RANGE', 'Invalid time range.');
  }
  return epochSec;
}

export function validateTimeRange(
  from: number | undefined,
  to: number | undefined
): void {
  if (from !== undefined && to !== undefined && from > to) {
    throw new ValidationError('INVALID_TIME_RANGE', 'Invalid time range.');
  }
}
