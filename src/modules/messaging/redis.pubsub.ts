import Redis from 'ioredis';
import config from '../../config';
import { logger } from '../../shared/utils/logger';
import type {
  IPubSubService,
  PubSubConnectionStatus,
} from './pubsub.interface';
import type { MarketEvent } from '../market-data/market-data.types';

export type RedisPubSubRole = 'publisher' | 'subscriber' | 'both';
type RedisClientRole = 'publisher' | 'subscriber';

interface ConnectionState {
  available: boolean;
  unavailableLogged: boolean;
}

export class RedisPubSubService implements IPubSubService {
  private readonly publisher: Redis | null;
  private readonly subscriber: Redis | null;
  private readonly channel = 'market_stream';
  private readonly serviceLogger = logger.child({ subsystem: 'redis-pubsub' });
  private readonly states: Record<RedisClientRole, ConnectionState> = {
    publisher: { available: false, unavailableLogged: false },
    subscriber: { available: false, unavailableLogged: false },
  };
  private disconnecting = false;

  constructor(private readonly role: RedisPubSubRole = 'both') {
    const redisOptions = {
      retryStrategy: (times: number) => Math.min(times * 50, 2000),
      maxRetriesPerRequest: 3,
    };

    this.publisher = role === 'publisher' || role === 'both'
      ? new Redis(config.REDIS_URL, {
        ...redisOptions,
        // Realtime delivery is best-effort. Commands must not accumulate
        // while Redis is unavailable.
        enableOfflineQueue: false,
      })
      : null;
    this.subscriber = role === 'subscriber' || role === 'both'
      ? new Redis(config.REDIS_URL, redisOptions)
      : null;

    this.observeConnection(this.publisher, 'publisher');
    this.observeConnection(this.subscriber, 'subscriber');
    this.serviceLogger.info('Redis Pub/Sub initialized', {
      event: 'redis_pubsub_initialized',
      role,
    });
  }

  async publish(message: MarketEvent): Promise<void> {
    if (!this.publisher) {
      throw new Error('Redis Pub/Sub instance is not configured as a publisher');
    }

    try {
      await this.publisher.publish(this.channel, JSON.stringify(message));
      this.markAvailable('publisher');
    } catch (error) {
      this.markUnavailable('publisher', error);
      throw error;
    }
  }

  async subscribe(callback: (message: MarketEvent) => void): Promise<void> {
    if (!this.subscriber) {
      throw new Error('Redis Pub/Sub instance is not configured as a subscriber');
    }

    try {
      const count = await this.subscriber.subscribe(this.channel);
      this.markAvailable('subscriber');
      this.serviceLogger.info('Redis subscription ready', {
        event: 'redis_subscriber_ready',
        role: 'subscriber',
        subscriptionCount: count,
      });
    } catch (error) {
      this.markUnavailable('subscriber', error);
      throw error;
    }

    this.subscriber.on('message', (channel, text) => {
      if (channel !== this.channel) return;
      try {
        callback(JSON.parse(text) as MarketEvent);
      } catch (error) {
        this.serviceLogger.error('Redis Pub/Sub message parsing failed', {
          event: 'redis_message_parse_failed',
          role: 'subscriber',
          error: getErrorMessage(error),
        });
      }
    });
  }

  async disconnect(): Promise<void> {
    this.disconnecting = true;
    if (this.subscriber) {
      await this.subscriber.unsubscribe(this.channel);
      this.subscriber.disconnect();
    }
    this.publisher?.disconnect();
    this.states.publisher.available = false;
    this.states.subscriber.available = false;
    this.serviceLogger.info('Redis Pub/Sub disconnected', {
      event: 'redis_pubsub_disconnected',
      role: this.role,
    });
  }

  getStatus(): PubSubConnectionStatus {
    const publisherReady = !this.publisher || this.states.publisher.available;
    const subscriberReady = !this.subscriber || this.states.subscriber.available;
    return publisherReady && subscriberReady ? 'available' : 'unavailable';
  }

  private observeConnection(client: Redis | null, role: RedisClientRole): void {
    if (!client) return;
    client.on('ready', () => this.markAvailable(role));
    client.on('error', (error) => this.markUnavailable(role, error));
    client.on('close', () => this.markUnavailable(role));
    client.on('end', () => this.markUnavailable(role));
  }

  private markAvailable(role: RedisClientRole): void {
    const state = this.states[role];
    if (state.available) return;

    const recovered = state.unavailableLogged;
    state.available = true;
    state.unavailableLogged = false;

    this.serviceLogger.info(
      recovered ? `Redis ${role} recovered` : `Redis ${role} connected`,
      {
        event: recovered
          ? `redis_${role}_recovered`
          : `redis_${role}_connected`,
        role,
      },
    );
  }

  private markUnavailable(role: RedisClientRole, error?: unknown): void {
    const state = this.states[role];
    state.available = false;
    if (this.disconnecting || state.unavailableLogged) return;

    state.unavailableLogged = true;
    this.serviceLogger.warn(`Redis ${role} unavailable`, {
      event: `redis_${role}_unavailable`,
      role,
      ...(error === undefined ? {} : { error: getErrorMessage(error) }),
    });
  }
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error ? error.message : 'Unknown error';
}
