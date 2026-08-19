import { RocksDatabase } from '@harperfast/rocksdb-js';
import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import type { Candle } from '../candle.types';
import {
  type PendingCandle,
  type PendingCandleStore,
} from './pending-candle.store';

const PENDING_PREFIX = 'pending|';
const TIMESTAMP_WIDTH = 16;

export class RocksPendingCandleStore implements PendingCandleStore {
  private readonly db: RocksDatabase;

  constructor(path: string) {
    mkdirSync(dirname(path), { recursive: true });
    this.db = RocksDatabase.open(path, { disableWAL: false });
  }

  async enqueue(candle: Candle): Promise<void> {
    this.validateCandle(candle);
    await this.db.put(this.createKey(candle), JSON.stringify(candle));
  }

  async peek(limit: number): Promise<PendingCandle[]> {
    if (!Number.isInteger(limit) || limit <= 0) return [];

    const result: PendingCandle[] = [];
    for (const { key, value } of this.db.getRange({
      start: PENDING_PREFIX,
      end: `${PENDING_PREFIX}\uffff`,
    })) {
      result.push({ key: String(key), candle: this.deserialize(value) });

      if (result.length >= limit) break;
    }
    return result;
  }

  async ack(keys: string[]): Promise<void> {
    if (keys.length === 0) return;

    await this.db.transaction(async (transaction) => {
      for (const key of keys) await transaction.remove(key);
    });
  }

  close(): void {
    this.db.close();
  }

  private createKey(candle: Candle): string {
    const timestamp = candle.startTime.toString().padStart(TIMESTAMP_WIDTH, '0');
    return `${PENDING_PREFIX}${timestamp}|${encodeURIComponent(candle.symbol)}`;
  }

  private validateCandle(candle: Candle): void {
    if (candle.symbol.trim().length === 0) {
      throw new Error('Invalid candle symbol: symbol must not be empty');
    }

    const numericFields = [
      ['startTime', candle.startTime],
      ['open', candle.open],
      ['high', candle.high],
      ['low', candle.low],
      ['close', candle.close],
      ['volume', candle.volume],
    ] as const;

    for (const [field, value] of numericFields) {
      if (!Number.isFinite(value)) {
        throw new Error(`Invalid candle ${field}: value must be finite`);
      }
    }

    if (!Number.isSafeInteger(candle.startTime) || candle.startTime <= 0) {
      throw new Error('Invalid candle startTime: value must be a positive integer timestamp');
    }
  }

  private deserialize(value: unknown): Candle {
    const parsed: unknown = JSON.parse(String(value));
    if (!this.isCandle(parsed)) {
      throw new Error('Invalid candle found in RocksDB pending store');
    }
    return parsed;
  }

  private isCandle(value: unknown): value is Candle {
    if (typeof value !== 'object' || value === null) return false;

    const candle = value as Record<string, unknown>;
    return typeof candle.symbol === 'string'
      && typeof candle.startTime === 'number'
      && typeof candle.open === 'number'
      && typeof candle.high === 'number'
      && typeof candle.low === 'number'
      && typeof candle.close === 'number'
      && typeof candle.volume === 'number';
  }
}
