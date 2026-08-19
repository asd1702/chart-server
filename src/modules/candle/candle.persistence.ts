import config from '../../config';
import { logger } from '../../shared/utils/logger';
import type { Candle } from './candle.types';
import { CandleFlusher } from './candle.flusher';
import { RocksPendingCandleStore } from './storage/rocks-pending-candle.store';

export const pendingCandleStore = new RocksPendingCandleStore(config.ROCKSDB_PATH);
export const candleFlusher = new CandleFlusher(pendingCandleStore);
const activeEnqueues = new Set<Promise<void>>();
let isClosing = false;

export function enqueueCandle(candle: Candle): Promise<void> {
  if (isClosing) {
    return Promise.reject(new Error('Candle persistence is shutting down'));
  }

  const operation = pendingCandleStore.enqueue(candle)
    .then(() => candleFlusher.notifyEnqueued())
    .catch((error: unknown) => {
      logger.error('RocksDB candle enqueue failed', {
        symbol: candle.symbol,
        startTime: candle.startTime,
        error,
      });
      throw error;
    });

  activeEnqueues.add(operation);
  void operation.then(
    () => activeEnqueues.delete(operation),
    () => activeEnqueues.delete(operation),
  );
  return operation;
}

export async function closeCandlePersistence(): Promise<void> {
  isClosing = true;
  await candleFlusher.stop();
  await Promise.allSettled(activeEnqueues);
  await pendingCandleStore.close();
  logger.info('Candle pending store closed');
}
