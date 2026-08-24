import { describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  connect: vi.fn().mockResolvedValue(undefined),
  disconnect: vi.fn().mockResolvedValue(undefined),
  send: vi.fn().mockResolvedValue([]),
  producer: vi.fn(),
  kafka: vi.fn(),
}));

vi.mock('@confluentinc/kafka-javascript', () => {
  return {
    KafkaJS: {
      Kafka: class {
        constructor(config: unknown) {
          mocks.kafka(config);
        }

        producer(config: unknown) {
          return mocks.producer(config);
        }
      },
    },
  };
});

import { KafkaRawTickPublisher } from './kafka-raw-tick.publisher';

describe('KafkaRawTickPublisher', () => {
  it('connects, publishes a symbol-keyed RawMarketTick, and disconnects', async () => {
    mocks.producer.mockReturnValue({
      connect: mocks.connect,
      disconnect: mocks.disconnect,
      send: mocks.send,
    });

    const publisher = new KafkaRawTickPublisher(
      ['kafka:19092'],
      'market.raw-ticks',
      'market-feed-ingestor',
    );

    await publisher.start();
    await publisher.publish({
      schemaVersion: 1,
      symbol: 'BTC/USD',
      price: 100_000,
      providerTimestampSec: 1_700_000_000,
      receivedAtMs: 1_700_000_001_000,
      source: 'twelvedata',
    });
    await publisher.stop();

    expect(mocks.kafka).toHaveBeenCalledWith({
      kafkaJS: {
        brokers: ['kafka:19092'],
        clientId: 'market-feed-ingestor',
      },
    });
    expect(mocks.producer).toHaveBeenCalledWith({
      'linger.ms': 0,
      kafkaJS: {
        acks: -1,
        idempotent: true,
        allowAutoTopicCreation: false,
      },
    });
    expect(mocks.connect).toHaveBeenCalledOnce();
    expect(mocks.send).toHaveBeenCalledWith({
      topic: 'market.raw-ticks',
      messages: [
        {
          key: 'BTC/USD',
          value: JSON.stringify({
            schemaVersion: 1,
            symbol: 'BTC/USD',
            price: 100_000,
            providerTimestampSec: 1_700_000_000,
            receivedAtMs: 1_700_000_001_000,
            source: 'twelvedata',
          }),
        },
      ],
    });
    expect(mocks.disconnect).toHaveBeenCalledOnce();
  });
});
