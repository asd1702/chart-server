import type { Candle } from '../candle/candle.types';

export interface CandleMarketEvent {
  type: 'candle';
  timeframe: string;
  candle: Candle;
}

export interface PriceTickMarketEvent {
  type: 'tick';
  symbol: string;
  price: number;
  timestamp: number;
}

export type MarketEvent = CandleMarketEvent | PriceTickMarketEvent;

export interface TwelveDataPriceMessage {
  event: 'price';
  symbol: string;
  price: number;
  timestamp: number;
}

export interface TwelveDataSubscription {
  action: 'subscribe' | 'unsubscribe';
  params: {
    symbols: string;
  };
}
