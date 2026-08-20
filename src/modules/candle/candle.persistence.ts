import config from '../../config';
import { logger } from '../../shared/utils/logger';
import type { Candle } from './candle.types';
import { CandleFlusher } from './candle.flusher';
import { RocksPendingCandleStore } from './storage/rocks-pending-candle.store';

interface CandlePersistenceRuntime {
  pendingStore: RocksPendingCandleStore;
  flusher: CandleFlusher;
}

let runtime: CandlePersistenceRuntime | null = null;
const activeEnqueues = new Set<Promise<void>>();
let isClosing = false;

export function startCandlePersistence(): void {
  if (runtime) return;

  let pendingStore: RocksPendingCandleStore | null = null;

  try {
    pendingStore = new RocksPendingCandleStore(config.ROCKSDB_PATH);
    const flusher = new CandleFlusher(pendingStore);
    flusher.start();

    runtime = { pendingStore, flusher };
    isClosing = false;
  } catch (error) {
    if (pendingStore) {
      try {
        pendingStore.close();
      } catch {
        // Preserve and report the original startup failure.
      }
    }

    runtime = null;
    isClosing = false;
    logger.error('Candle persistence startup failed', {
      event: 'candle_persistence_start_failed',
      path: config.ROCKSDB_PATH,
      error: error instanceof Error ? error.message : 'Unknown error',
    });
    throw error;
  }
}

export function enqueueCandle(candle: Candle): Promise<void> {
  if (isClosing) {
    return Promise.reject(new Error('Candle persistence is shutting down'));
  }

  const current = getRuntime();
  const operation = current.pendingStore.enqueue(candle)
    .then(() => current.flusher.notifyEnqueued())
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
  const current = runtime;
  if (!current) return;

  isClosing = true;
  let closeError: unknown;

  try {
    try {
      await current.flusher.stop();
    } catch (error) {
      closeError = error;
    }

    await Promise.allSettled([...activeEnqueues]);

    try {
      await current.pendingStore.close();
    } catch (error) {
      closeError ??= error;
    }

    if (closeError !== undefined) throw closeError;
    logger.info('Candle pending store closed');
  } finally {
    if (runtime === current) runtime = null;
    activeEnqueues.clear();
    isClosing = false;
  }
}

function getRuntime(): CandlePersistenceRuntime {
  if (!runtime) {
    throw new Error('Candle persistence is not started');
  }
  return runtime;
}
