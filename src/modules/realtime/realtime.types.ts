export type { MarketEvent as OutboundSocketMessage } from '../market-data/market-data.types';

export interface SubscribeMessage {
  type: 'subscribe';
  symbols: string[];
}

export interface UnsubscribeMessage {
  type: 'unsubscribe';
  symbols: string[];
}

export type InboundSocketMessage = SubscribeMessage | UnsubscribeMessage;
