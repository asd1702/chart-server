/**
 * Split runtime uses Redis Pub/Sub as the process boundary.
 */

import type { IPubSubService } from './pubsub.interface';
import { RedisPubSubService, type RedisPubSubRole } from './redis.pubsub';
import { logger } from '../../shared/utils/logger';

export function createRedisPubSubService(role: RedisPubSubRole): IPubSubService {
  logger.info('분리 런타임의 Redis Pub/Sub 전송 계층을 선택했습니다.', {
    subsystem: 'pubsub-factory',
    event: 'redis_pubsub_selected',
    role,
  });
  return new RedisPubSubService(role);
}
