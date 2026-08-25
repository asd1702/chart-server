import { KafkaJS } from '@confluentinc/kafka-javascript';
import type {
  ConsumedRawTickRecord,
  RawTickConsumer,
} from './raw-tick.consumer';

const { Kafka } = KafkaJS;

export class KafkaRawTickConsumer implements RawTickConsumer {
  private readonly consumer;

  constructor(
    brokers: string[],
    private readonly topic: string,
    groupId: string,
    clientId: string,
  ) {
    const kafka = new Kafka({
      kafkaJS: {
        brokers,
        clientId,
      },
    });

    this.consumer = kafka.consumer({
      kafkaJS: {
        groupId,
        autoCommit: false,
        allowAutoTopicCreation: false,
        fromBeginning: true,
      },
    });
  }

  async start(
    handler: (record: ConsumedRawTickRecord) => Promise<void>,
  ): Promise<void> {
    await this.consumer.connect();

    await this.consumer.subscribe({
      topics: [this.topic],
    });

    await this.consumer.run({
      partitionsConsumedConcurrently: 1,
      eachMessage: async ({ topic, partition, message }) => {
        await handler({
          topic,
          partition,
          offset: message.offset,
          value: message.value,
        });
      },
    });
  }

  async commitOffset(input: {
    topic: string;
    partition: number;
    offset: string;
  }): Promise<void> {
    await this.consumer.commitOffsets([input]);
  }

  async stop(): Promise<void> {
    await this.consumer.disconnect();
  }
}
