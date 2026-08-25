import config from './config';
import { CandleProcessor } from './modules/candle-processing/candle-processor';
import { CandleProcessingRuntime } from './modules/candle-processing/candle-processing.runtime';
import { CandleRepository } from './modules/candle/candle.repository';
import { KafkaRawTickConsumer } from './modules/kafka/kafka-raw-tick.consumer';
import { createRedisPubSubService } from './modules/messaging/pubsub.factory';
import type { MarketEventPublisher } from './modules/messaging/pubsub.interface';
import { CandleProcessorMetrics } from './modules/observability/candle-processor.metrics';
import { MetricsServer } from './modules/observability/metrics-server';
import { prisma } from './shared/db/prisma';
import {
  getErrorMessage,
  logger,
  setLogComponent,
} from './shared/utils/logger';

const candleProcessorLogger = logger.child({
  component: 'candle-processor',
});

export async function startCandleProcessor(): Promise<void> {
  setLogComponent('candle-processor');

  candleProcessorLogger.info('Candle Processor 설정을 불러왔습니다.', {
    event: 'candle_processor_configuration',
    symbols: config.candleProcessor.symbols,
    topic: config.kafka.rawTicksTopic,
    consumerGroup: config.kafka.candleConsumerGroup,
  });

  const publisher = createRedisPubSubService('publisher');
  const metrics = new CandleProcessorMetrics();
  const metricsServer = new MetricsServer(
    config.observability.candleProcessorMetricsPort,
    metrics.registry,
  );
  const consumer = new KafkaRawTickConsumer(
    [...config.kafka.brokers],
    config.kafka.rawTicksTopic,
    config.kafka.candleConsumerGroup,
    config.kafka.candleConsumerClientId,
  );
  const processor = new CandleProcessor(
    config.candleProcessor.symbols,
    new CandleRepository(),
    consumer,
    metrics,
  );
  const runtime = new CandleProcessingRuntime(
    consumer,
    processor,
    publisher,
    async (error) => {
      candleProcessorLogger.error(
        'Candle Processor가 복구 불가능한 processing 오류를 감지했습니다.',
        {
          event: 'candle_processor_fatal',
          error: getErrorMessage(error),
        },
      );

      process.exit(1);
    },
  );

  installShutdownHandlers(runtime, publisher, metricsServer);

  try {
    await metricsServer.start();
    await runtime.start();
  } catch (error) {
    await metricsServer.stop().catch(() => undefined);
    await publisher.disconnect().catch(() => undefined);
    throw error;
  }

  candleProcessorLogger.info('Candle Processor를 시작했습니다.', {
    event: 'candle_processor_started',
  });
}

function installShutdownHandlers(
  runtime: CandleProcessingRuntime,
  publisher: MarketEventPublisher,
  metricsServer: MetricsServer,
): void {
  let isShuttingDown = false;

  const shutdown = async (signal: string): Promise<void> => {
    if (isShuttingDown) {
      return;
    }

    isShuttingDown = true;

    candleProcessorLogger.info('Candle Processor 종료 요청을 처리합니다.', {
      event: 'candle_processor_shutdown_requested',
      signal,
    });

    let firstError: unknown;

    try {
      await runtime.stop();
    } catch (error) {
      firstError ??= error;
    }

    try {
      await metricsServer.stop();
    } catch (error) {
      firstError ??= error;
    }

    try {
      await publisher.disconnect();
    } catch (error) {
      firstError ??= error;
    }

    try {
      await prisma.$disconnect();
    } catch (error) {
      firstError ??= error;
    }

    if (firstError !== undefined) {
      candleProcessorLogger.error('Candle Processor 정상 종료에 실패했습니다.', {
        event: 'candle_processor_shutdown_failed',
        error: getErrorMessage(firstError),
      });
      process.exit(1);
      return;
    }

    candleProcessorLogger.info('Candle Processor 종료를 완료했습니다.', {
      event: 'candle_processor_stopped',
    });
    process.exit(0);
  };

  process.once('SIGTERM', () => void shutdown('SIGTERM'));
  process.once('SIGINT', () => void shutdown('SIGINT'));
}

if (require.main === module) {
  startCandleProcessor().catch(async (error) => {
    candleProcessorLogger.error('Candle Processor 시작에 실패했습니다.', {
      event: 'candle_processor_start_failed',
      error: getErrorMessage(error),
    });

    await prisma.$disconnect();
    process.exit(1);
  });
}