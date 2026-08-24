export interface RawMarketTick {
  schemaVersion: 1;
  symbol: string;
  price: number;
  providerTimestampSec: number;
  receivedAtMs: number;
  source: 'twelvedata';
}
