import { KafkaJS } from '@confluentinc/kafka-javascript';
import type { RawMarketTick } from '../market-data/raw-market-tick';
import type { RawTickPublisher } from './raw-tick.publisher';

const { Kafka } = KafkaJS;

/** KafkaJS-compatible producer for the single raw-tick durable log. */
export class KafkaRawTickPublisher implements RawTickPublisher {
  private readonly producer;

  constructor(
    brokers: string[],
    private readonly topic: string,
    clientId: string,
  ) {
    const kafka = new Kafka({
      kafkaJS: {
        brokers,
        clientId,
      },
    });

    this.producer = kafka.producer({
      kafkaJS: {
        acks: -1,
        idempotent: true,
        allowAutoTopicCreation: false,
      },
    });
  }

  async start(): Promise<void> {
    await this.producer.connect();
  }

  async publish(tick: RawMarketTick): Promise<void> {
    await this.producer.send({
      topic: this.topic,
      messages: [
        {
          key: tick.symbol,
          value: JSON.stringify(tick),
        },
      ],
    });
  }

  async stop(): Promise<void> {
    await this.producer.disconnect();
  }
}
