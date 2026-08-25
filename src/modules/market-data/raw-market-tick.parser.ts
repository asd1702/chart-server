import type { RawMarketTick } from './raw-market-tick';

export function parseRawMarketTick(
  value: Buffer | null,
): RawMarketTick {
  if (value === null) {
    throw new Error('RawMarketTick value must not be null');
  }

  let parsed: unknown;

  try {
    parsed = JSON.parse(value.toString('utf8'));
  } catch {
    throw new Error('RawMarketTick value must be valid JSON');
  }

  if (!isRecord(parsed)) {
    throw new Error('RawMarketTick must be a JSON object');
  }

  if (parsed.schemaVersion !== 1) {
    throw new Error('RawMarketTick schemaVersion must be 1');
  }

  if (
    typeof parsed.symbol !== 'string'
    || !parsed.symbol.trim()
  ) {
    throw new Error('RawMarketTick symbol must be a non-empty string');
  }

  if (!isFiniteNumber(parsed.price)) {
    throw new Error('RawMarketTick price must be a finite number');
  }

  if (!isFiniteNumber(parsed.providerTimestampSec)) {
    throw new Error(
      'RawMarketTick providerTimestampSec must be a finite number',
    );
  }

  if (!isFiniteNumber(parsed.receivedAtMs)) {
    throw new Error('RawMarketTick receivedAtMs must be a finite number');
  }

  if (parsed.source !== 'twelvedata') {
    throw new Error('RawMarketTick source must be twelvedata');
  }

  return {
    schemaVersion: 1,
    symbol: parsed.symbol,
    price: parsed.price,
    providerTimestampSec: parsed.providerTimestampSec,
    receivedAtMs: parsed.receivedAtMs,
    source: 'twelvedata',
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value);
}
