import {
  collectDefaultMetrics,
  Counter,
  Histogram,
  Registry,
} from 'prom-client';

export class IngestorMetrics {
  readonly registry: Registry;

  private readonly rawTicksReceived: Counter<'symbol'>;
  private readonly kafkaAcked: Counter<'symbol'>;
  private readonly kafkaFailed: Counter<'symbol'>;
  private readonly kafkaPublishDuration: Histogram<'symbol'>;

  constructor(registry = new Registry()) {
    this.registry = registry;
    collectDefaultMetrics({ register: registry });

    this.rawTicksReceived = new Counter({
      name: 'market_raw_ticks_received_total',
      help: 'Validated raw ticks accepted from the market WebSocket',
      labelNames: ['symbol'],
      registers: [registry],
    });
    this.kafkaAcked = new Counter({
      name: 'market_raw_ticks_kafka_acked_total',
      help: 'Raw ticks acknowledged by Kafka before application processing',
      labelNames: ['symbol'],
      registers: [registry],
    });
    this.kafkaFailed = new Counter({
      name: 'market_raw_ticks_kafka_failed_total',
      help: 'Raw tick Kafka durable admission failures',
      labelNames: ['symbol'],
      registers: [registry],
    });
    this.kafkaPublishDuration = new Histogram({
      name: 'market_kafka_publish_duration_seconds',
      help: 'Kafka raw tick publish ACK latency',
      labelNames: ['symbol'],
      buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
      registers: [registry],
    });
  }

  recordRawTickReceived(symbol: string): void {
    safely(() => this.rawTicksReceived.inc({ symbol }));
  }

  startKafkaPublish(symbol: string): () => void {
    try {
      const end = this.kafkaPublishDuration.startTimer({ symbol });

      return () => safely(() => {
        end();
      });
    } catch {
      return () => undefined;
    }
  }

  recordKafkaAcknowledged(symbol: string): void {
    safely(() => this.kafkaAcked.inc({ symbol }));
  }

  recordKafkaFailed(symbol: string): void {
    safely(() => this.kafkaFailed.inc({ symbol }));
  }
}

function safely(action: () => void): void {
  try {
    action();
  } catch {
    // Metrics are strictly best-effort instrumentation.
  }
}
