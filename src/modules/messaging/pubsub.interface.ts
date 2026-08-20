/**
 * Market event transport contract.
 *
 * Market Ingestor는 publisher 역할을 사용하고,
 * Chart Server는 subscriber 역할을 사용한다.
 * 현재 split runtime transport는 Redis Pub/Sub이다.
 */

import type { MarketEvent } from '../market-data/market-data.types';

export type PubSubConnectionStatus = 'available' | 'unavailable';

export interface PubSubHealthIndicator {
  getStatus(): PubSubConnectionStatus;
}

export interface MarketEventPublisher {
  /**
   * 메시지 발행 (Broadcast)
   * - Ingestor(데이터 수집기)가 호출
   * - 모든 구독자에게 메시지 전달
   */
  publish(message: MarketEvent): Promise<void>;
  disconnect(): Promise<void>;
}

export interface MarketEventSubscriber {
  /**
   * 메시지 수신 (Subscribe)
   * - Socket Server(방송국)가 호출
   * - 메시지를 받으면 콜백 실행
   */
  subscribe(callback: (message: MarketEvent) => void): Promise<void>;
  disconnect(): Promise<void>;
}

export interface IPubSubService
  extends MarketEventPublisher, MarketEventSubscriber, PubSubHealthIndicator {}
