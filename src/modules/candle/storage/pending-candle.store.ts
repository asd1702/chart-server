import { Candle } from '../candle.types';

export interface PendingCandle{
  key: string;
  candle: Candle;
}

export interface PendingCandleStore{
  enqueue(candle: Candle): Promise<void>;

  peek(limit: number): Promise<PendingCandle[]>;

  ack(keys: string[]): Promise<void>;

  close(): Promise<void> | void;
}
