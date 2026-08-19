/**
 * In-Memory PubSub 구현체 (단일 서버용)
 * 
 * EventEmitter 기반으로 동작하며, Redis 없이 메모리 내에서 즉시 이벤트 전달.
 * - 네트워크 오버헤드 없음 (가장 빠름)
 * - 단일 서버에서만 동작 (멀티 서버 불가)
 */

import { EventEmitter } from 'events';
import type {
  IPubSubService,
  PubSubConnectionStatus,
} from './pubsub.interface';
import type { MarketEvent } from '../market-data/market-data.types';
import { logger } from '../../shared/utils/logger';

export class MemoryPubSubService extends EventEmitter implements IPubSubService {
  private readonly CHANNEL = 'market_stream';

  constructor() {
    super();
    // MaxListeners 제한 해제 (내부 이벤트 버스로 사용)
    this.setMaxListeners(0);
    logger.info('Memory Pub/Sub initialized', {
      subsystem: 'memory-pubsub',
      event: 'memory_pubsub_initialized',
    });
  }

  /**
   * 메시지 발행 (네트워크 IO 없이 즉시 메모리 내 이벤트 발생)
   */
  async publish(message: MarketEvent): Promise<void> {
    this.emit(this.CHANNEL, message);
  }

  /**
   * 메시지 구독 (이벤트 리스너 등록)
   */
  async subscribe(callback: (message: MarketEvent) => void): Promise<void> {
    this.on(this.CHANNEL, callback);
    logger.info('Memory Pub/Sub subscription ready', {
      subsystem: 'memory-pubsub',
      event: 'memory_pubsub_subscribed',
    });
  }

  async disconnect(): Promise<void> {
    this.removeAllListeners(this.CHANNEL);
  }

  getStatus(): PubSubConnectionStatus {
    return 'available';
  }
}
