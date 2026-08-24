import type { RawMarketTick } from '../market-data/raw-market-tick';

/**
 * Durable raw-tick log boundary.
 *
 * TwelveDataStream depends only on this contract; the active runtime owns the
 * concrete Kafka producer and its leadership-epoch lifecycle.
 */
export interface RawTickPublisher {
  start(): Promise<void>;
  publish(tick: RawMarketTick): Promise<void>;
  stop(): Promise<void>;
}
