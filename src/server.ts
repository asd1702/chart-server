import http from 'http';
import config from './config';
import { createApp } from './app';
import { closeWebSocketServer, initWebSocketServer } from './modules/realtime';
import { createRedisPubSubService } from './modules/messaging/pubsub.factory';
import type { MarketEventSubscriber } from './modules/messaging/pubsub.interface';
import { prisma } from './shared/db/prisma';
import {
  getErrorMessage,
  logger,
  setLogComponent,
} from './shared/utils/logger';

const serverLogger = logger.child({ component: 'chart-server' });

export async function startChartServer(): Promise<void> {
  setLogComponent('chart-server');

  const subscriber = createRedisPubSubService('subscriber');
  const httpServer = http.createServer(createApp({
    getRedisSubscriberStatus: () => subscriber.getStatus(),
  }));

  try {
    await initWebSocketServer(httpServer, subscriber);
    await listen(httpServer, config.port);
  } catch (error) {
    await subscriber.disconnect();
    throw error;
  }

  serverLogger.info('Chart Server 시작을 완료했습니다.', {
    event: 'chart_server_started',
    port: config.port,
    url: `http://localhost:${config.port}`,
    websocketUrl: `ws://localhost:${config.port}/ws`,
  });

  installShutdownHandlers(httpServer, subscriber);
}

function installShutdownHandlers(
  httpServer: http.Server,
  subscriber: MarketEventSubscriber
): void {
  let isShuttingDown = false;

  const shutdown = async (signal: string): Promise<void> => {
    if (isShuttingDown) return;
    isShuttingDown = true;
    serverLogger.info('Chart Server 종료 요청을 처리합니다.', {
      event: 'shutdown_requested',
      signal,
    });

    const forceExitTimer = setTimeout(() => {
      serverLogger.error('Chart Server 정상 종료 제한 시간을 초과했습니다.', {
        event: 'shutdown_timed_out',
      });
      process.exit(1);
    }, 10_000);
    forceExitTimer.unref();

    try {
      const httpClosed = closeHttpServer(httpServer);
      await closeWebSocketServer();
      await httpClosed;
      await subscriber.disconnect();
      await prisma.$disconnect();
      clearTimeout(forceExitTimer);
      serverLogger.info('Chart Server 종료를 완료했습니다.', {
        event: 'chart_server_stopped',
      });
      process.exit(0);
    } catch (error) {
      serverLogger.error('Chart Server 정상 종료에 실패했습니다.', {
        event: 'shutdown_failed',
        error: getErrorMessage(error),
      });
      process.exit(1);
    }
  };

  process.once('SIGTERM', () => void shutdown('SIGTERM'));
  process.once('SIGINT', () => void shutdown('SIGINT'));
  process.once('SIGUSR2', () => void shutdown('SIGUSR2'));
}

function listen(server: http.Server, port: number): Promise<void> {
  return new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, () => {
      server.off('error', reject);
      resolve();
    });
  });
}

function closeHttpServer(server: http.Server): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
}

if (require.main === module) {
  startChartServer().catch(async (error) => {
    serverLogger.error('Chart Server 시작에 실패했습니다.', {
      event: 'chart_server_start_failed',
      error: getErrorMessage(error),
    });
    await prisma.$disconnect();
    process.exit(1);
  });
}
