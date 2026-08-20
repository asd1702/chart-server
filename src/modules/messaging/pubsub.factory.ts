/**
 * Split runtime uses Redis Pub/Sub as the process boundary.
 */

import type { IPubSubService } from './pubsub.interface';
import { RedisPubSubService, type RedisPubSubRole } from './redis.pubsub';
import { logger } from '../../shared/utils/logger';

export function createRedisPubSubService(role: RedisPubSubRole): IPubSubService {
  logger.info('Redis Pub/Sub selected for split runtime', {
    subsystem: 'pubsub-factory',
    event: 'redis_pubsub_selected',
    role,
  });
  return new RedisPubSubService(role);
}
