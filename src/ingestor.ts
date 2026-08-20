import config from './config';
import {
  closeCandlePersistence,
  startCandlePersistence,
} from './modules/candle/candle.persistence';
import { createRedisPubSubService } from './modules/messaging/pubsub.factory';
import type { MarketEventPublisher } from './modules/messaging/pubsub.interface';
import {
  connectToTwelveData,
  disconnectFromTwelveData,
} from './modules/market-data/twelvedata.provider';
import { runHistoricalBackfill } from './modules/market-data/historical-backfill.service';
import { prisma } from './shared/db/prisma';
import { logger } from './shared/utils/logger';

const ingestorLogger = logger.child({ component: 'market-ingestor' });

export async function startMarketIngestor(): Promise<void> {
  if (!config.TWELVE_DATA_API_KEY.trim()) {
    throw new Error('Market Ingestor requires TWELVE_DATA_API_KEY');
  }

  ingestorLogger.info('Market Ingestor configuration loaded', {
    event: 'ingestor_configuration',
    streamSymbols: config.market.streamSymbols,
    historicalBackfillEnabled: config.market.historicalBackfillEnabled,
  });

  const publisher: MarketEventPublisher = createRedisPubSubService('publisher');
  let backfillOperation: Promise<void> = Promise.resolve();

  try {
    startCandlePersistence();
    connectToTwelveData(publisher);
    if (config.market.historicalBackfillEnabled) {
      backfillOperation = runHistoricalBackfill().catch((error) => {
        ingestorLogger.error('Historical backfill failed', {
          event: 'historical_backfill_failed',
          error: error instanceof Error ? error.message : 'Unknown error',
        });
      });
    }
  } catch (error) {
    await disconnectFromTwelveData().catch(() => undefined);
    await closeCandlePersistence().catch(() => undefined);
    await publisher.disconnect().catch(() => undefined);
    throw error;
  }

  ingestorLogger.info('Market Ingestor started');
  installShutdownHandlers(publisher, () => backfillOperation);
}

function installShutdownHandlers(
  publisher: MarketEventPublisher,
  getSyncOperation: () => Promise<void>
): void {
  let isShuttingDown = false;

  const shutdown = async (signal: string): Promise<void> => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    ingestorLogger.info('Shutdown started', { signal });

    const forceExitTimer = setTimeout(() => {
      ingestorLogger.error('Graceful shutdown timed out');
      process.exit(1);
    }, 15_000);
    forceExitTimer.unref();

    try {
      await disconnectFromTwelveData();
      await getSyncOperation();
      await closeCandlePersistence();
      await publisher.disconnect();
      await prisma.$disconnect();
      clearTimeout(forceExitTimer);
      ingestorLogger.info('Shutdown completed');
      process.exit(0);
    } catch (error) {
      ingestorLogger.error('Graceful shutdown failed', {
        error: error instanceof Error ? error.message : 'Unknown error',
      });
      process.exit(1);
    }
  };

  process.once('SIGTERM', () => void shutdown('SIGTERM'));
  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGUSR2', () => void shutdown('SIGUSR2'));
}

if (require.main === module) {
  startMarketIngestor().catch(async (error) => {
    ingestorLogger.error('Market Ingestor startup failed', {
      error: error instanceof Error ? error.message : 'Unknown error',
    });
    await prisma.$disconnect();
    process.exit(1);
  });
}
