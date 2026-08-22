import { EventEmitter } from 'events';
import { beforeEach, describe, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({
  clients: [] as Array<EventEmitter & {
    publish: ReturnType<typeof vi.fn>;
    subscribe: ReturnType<typeof vi.fn>;
    unsubscribe: ReturnType<typeof vi.fn>;
    disconnect: ReturnType<typeof vi.fn>;
  }>,
  log: {
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));

vi.mock('ioredis', () => ({
  default: class FakeRedis extends EventEmitter {
    publish = vi.fn();
    subscribe = vi.fn().mockResolvedValue(1);
    unsubscribe = vi.fn().mockResolvedValue(undefined);
    disconnect = vi.fn();

    constructor() {
      super();
      mocks.clients.push(this);
    }
  },
}));

vi.mock('../../config', () => ({
  default: { REDIS_URL: 'redis://test:6379' },
}));

vi.mock('../../shared/utils/logger', () => ({
  logger: { child: vi.fn(() => mocks.log) },
  getErrorMessage: (error: unknown) => error instanceof Error ? error.message : 'Unknown error',
}));

import { RedisPubSubService } from './redis.pubsub';

describe('RedisPubSubService logging', () => {
  beforeEach(() => {
    mocks.clients.length = 0;
    vi.clearAllMocks();
  });

  it('logs one unavailable transition and one recovery for repeated publish failures', async () => {
    const service = new RedisPubSubService('publisher');
    const client = mocks.clients[0]!;
    client.emit('ready');
    vi.clearAllMocks();
    client.publish.mockRejectedValue(new Error('Redis unavailable'));

    const message = {
      type: 'tick' as const,
      symbol: 'BTC/USD',
      price: 1,
      timestamp: 1,
    };
    await Promise.allSettled([
      service.publish(message),
      service.publish(message),
      service.publish(message),
    ]);

    expect(mocks.log.warn).toHaveBeenCalledOnce();
    expect(mocks.log.warn).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ event: 'redis_publisher_unavailable' }),
    );

    client.publish.mockResolvedValue(1);
    await service.publish(message);
    await service.publish(message);

    expect(mocks.log.info).toHaveBeenCalledOnce();
    expect(mocks.log.info).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({ event: 'redis_publisher_recovered' }),
    );
  });
});
