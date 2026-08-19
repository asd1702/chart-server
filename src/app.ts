import express, { Application, Request, Response } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import apiRoutes from './routes';
import { errorHandler, notFoundHandler, prisma } from './shared';
import { logger } from './shared/utils/logger';
import type { PubSubConnectionStatus } from './modules/messaging/pubsub.interface';

interface AppOptions {
  getRedisSubscriberStatus?: () => PubSubConnectionStatus;
}

export function createApp(options: AppOptions = {}): Application {
  const app: Application = express();

  // ==================== Security Middlewares ====================

  // Helmet: 보안 HTTP 헤더 설정
  app.use(helmet());

  // CORS: 모든 Origin 허용 (디버깅 위해 개방)
  app.use(cors({
    origin: true,
    credentials: true,
  }));

  // ==================== Body Parsers ====================
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));

  // ==================== Health Check ====================

  // Liveness: 프로세스가 살아있는지
  app.get('/', (req: Request, res: Response) => {
    res.status(200).json({
      status: 'ok',
      message: 'Market Data Architecture Lab chart server is running'
    });
  });

  // Readiness: DB 등 의존성이 준비되었는지
  app.get('/health', async (req: Request, res: Response) => {
    const health: {
      status: string;
      timestamp: string;
      services: {
        database: { status: string; latency?: number; error?: string };
        redis?: { status: PubSubConnectionStatus; role: 'subscriber' };
      };
    } = {
      status: 'ok',
      timestamp: new Date().toISOString(),
      services: {
        database: { status: 'unknown' },
      },
    };

    if (options.getRedisSubscriberStatus) {
      const redisStatus = options.getRedisSubscriberStatus();
      health.services.redis = { status: redisStatus, role: 'subscriber' };
      if (redisStatus === 'unavailable') health.status = 'degraded';
    }

    try {
      // DB Ping
      const dbStart = Date.now();
      await prisma.$queryRaw`SELECT 1`;
      health.services.database = {
        status: 'healthy',
        latency: Date.now() - dbStart,
      };
    } catch (error) {
      health.status = 'degraded';
      health.services.database = {
        status: 'unhealthy',
        error: error instanceof Error ? error.message : 'Unknown error',
      };
      logger.error('Health check failed: Database', { error });
    }

    // Redis loss degrades realtime delivery but REST/DB serving remains ready.
    const statusCode = health.services.database.status === 'healthy' ? 200 : 503;
    res.status(statusCode).json(health);
  });

  // ==================== API Routes ====================
  app.use('/api', apiRoutes);

  // ==================== Error Handling ====================
  app.use(notFoundHandler);
  app.use(errorHandler);

  return app;
}
