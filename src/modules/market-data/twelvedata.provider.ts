import WebSocket from 'ws';
import config from '../../config';
import { CandleMaker } from '../candle/candle.maker';
import { enqueueCandle } from '../candle/candle.persistence';
import type { RawTickPublisher } from '../kafka/raw-tick.publisher';
import type { MarketEventPublisher } from '../messaging/pubsub.interface';
import { getErrorMessage, logger } from '../../shared/utils/logger';
import type { TwelveDataSubscription } from './market-data.types';
import type { RawMarketTick } from './raw-market-tick';


const HEARTBEAT_CHECK_MS = 30_000;
const DISCONNECT_TIMEOUT_MS = 60_000;
const RECONNECT_DELAY_MS = 5_000;

export interface TwelveDataStreamOptions {
  symbols: readonly string[];
}

export class TwelveDataStream {
  private readonly publisher: MarketEventPublisher;
  private readonly rawTickPublisher: RawTickPublisher;
  private readonly symbols: readonly string[];

  private readonly candleMakers =
    new Map<string, CandleMaker>();

  private readonly activeMessageHandlers =
    new Set<Promise<void>>();

  private readonly symbolDurableTails =
    new Map<string, Promise<void>>();

  private ws: WebSocket | null = null;

  private reconnectTimer: NodeJS.Timeout | null = null;

  private heartbeatInterval: NodeJS.Timeout | null = null;

  private lastMessageTime = Date.now();

  private running = false;

  constructor(
    publisher: MarketEventPublisher,
    rawTickPublisher: RawTickPublisher,
    options: TwelveDataStreamOptions,
  ) {
    this.publisher = publisher;
    this.rawTickPublisher = rawTickPublisher;
    this.symbols = [...options.symbols];

    for (const symbol of this.symbols) {
      this.candleMakers.set(
        symbol,
        new CandleMaker(),
      );
    }
  }

  start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    this.connectWebSocket();
  }

  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    this.cleanupConnection();

    await Promise.allSettled(
      [...this.activeMessageHandlers],
    );

    logger.info(
      'TwelveData WebSocket 수집을 중지했습니다.',
      {
        subsystem: 'twelvedata-websocket',
        event: 'twelvedata_websocket_stopped',
      },
    );
  }

  async handlePriceUpdate(
    symbol: string,
    price: number,
    timestamp: number,
  ): Promise<void> {
    const maker = this.candleMakers.get(symbol);

    if (!maker) {
      return;
    }

    const rawTick: RawMarketTick = {
      schemaVersion: 1,
      symbol,
      price,
      providerTimestampSec: timestamp,
      receivedAtMs: Date.now(),
      source: 'twelvedata',
    };

    const completedCandle =
      await this.runSymbolDurablePhase(
        symbol,
        async () => {
          /*
           * Kafka acknowledgement is the admission gate for the legacy path.
           * A tick that did not reach the durable raw log must not mutate the
           * epoch-local CandleMaker state or reach RocksDB/Redis.
           */
          await this.rawTickPublisher.publish(rawTick);

          const completed = maker.update(
            symbol,
            price,
            0,
            timestamp,
          );

          if (completed) {
            await enqueueCandle(completed);
          }

          return completed;
        },
      );

    // Tick은 durability 대상이 아닌
    // best-effort realtime event다.
    await publishSafely(
      this.publisher,
      {
        type: 'tick',
        symbol,
        price,
        timestamp,
      },
    );

    if (completedCandle) {
      // RocksDB enqueue 성공 후에만
      // candle realtime event를 발행한다.
      await publishSafely(
        this.publisher,
        {
          type: 'candle',
          timeframe: '1m',
          candle: completedCandle,
        },
      );

      logger.debug(
        '1분 캔들이 완성되었습니다.',
        {
          subsystem: 'twelvedata-websocket',
          event: 'candle_completed',
          symbol,
          time: new Date(
            completedCandle.startTime * 1000,
          ).toISOString(),
        },
      );
    }
  }

  private runSymbolDurablePhase<T>(
    symbol: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    const previousTail =
      this.symbolDurableTails.get(symbol)
      ?? Promise.resolve();

    const result = previousTail.then(operation);

    /*
     * Queue tails resolve after failures so a rejected tick does not block
     * subsequent ticks for the same symbol. The original result still
     * propagates its failure to the caller.
     */
    const currentTail = result.then(
      () => undefined,
      () => undefined,
    );

    this.symbolDurableTails.set(symbol, currentTail);

    return result.finally(() => {
      if (this.symbolDurableTails.get(symbol) === currentTail) {
        this.symbolDurableTails.delete(symbol);
      }
    });
  }

  private connectWebSocket(): void {
    if (!this.running) {
      return;
    }

    this.cleanupConnection();

    logger.info(
      'TwelveData WebSocket 연결을 시작합니다.',
      {
        subsystem: 'twelvedata-websocket',
        event: 'twelvedata_websocket_connecting',
      },
    );

    const ws = new WebSocket(
      `wss://ws.twelvedata.com/v1/quotes/price?apikey=${config.TWELVE_DATA_API_KEY}`,
    );

    this.ws = ws;

    ws.on('open', () => {
      if (!this.running || this.ws !== ws) {
        return;
      }

      logger.info(
        'TwelveData WebSocket에 연결되었습니다.',
        {
          subsystem: 'twelvedata-websocket',
          event: 'twelvedata_websocket_connected',
        },
      );

      this.lastMessageTime = Date.now();

      this.startHeartbeatMonitor();

      ws.send(
        JSON.stringify(
          buildTwelveDataSubscription(
            this.symbols,
          ),
        ),
      );

      logger.info(
        'TwelveData 심볼 구독을 요청했습니다.',
        {
          subsystem: 'twelvedata-websocket',
          event: 'twelvedata_subscription_sent',
          symbols: this.symbols,
        },
      );
    });

    ws.on(
      'message',
      (data: WebSocket.RawData) => {
        if (!this.running || this.ws !== ws) {
          return;
        }

        this.lastMessageTime = Date.now();

        const operation =
          this.processMessage(data);

        this.activeMessageHandlers.add(
          operation,
        );

        void operation.finally(() => {
          this.activeMessageHandlers.delete(
            operation,
          );
        });
      },
    );

    ws.on('error', (error) => {
      logger.error(
        'TwelveData WebSocket 오류가 발생했습니다.',
        {
          subsystem: 'twelvedata-websocket',
          event: 'twelvedata_websocket_error',
          error: error.message,
        },
      );
    });

    ws.on('close', (code) => {
      /*
       * 이미 다른 socket으로 교체된 뒤
       * 오래된 socket의 close event가 도착할 수도 있다.
       */
      if (this.ws !== ws) {
        return;
      }

      this.ws = null;

      this.stopHeartbeatMonitor();

      if (!this.running) {
        return;
      }

      logger.warn(
        'TwelveData WebSocket 연결이 종료되어 재연결을 예약합니다.',
        {
          subsystem: 'twelvedata-websocket',
          event: 'twelvedata_websocket_disconnected',
          code,
          retryAfterMs: RECONNECT_DELAY_MS,
        },
      );

      this.scheduleReconnect();
    });
  }

  private scheduleReconnect(): void {
    if (!this.running) {
      return;
    }

    if (this.reconnectTimer) {
      return;
    }

    this.reconnectTimer = setTimeout(
      () => {
        this.reconnectTimer = null;

        if (!this.running) {
          return;
        }

        this.connectWebSocket();
      },
      RECONNECT_DELAY_MS,
    );
  }

  private startHeartbeatMonitor(): void {
    this.stopHeartbeatMonitor();

    this.heartbeatInterval = setInterval(
      () => {
        const elapsed =
          Date.now() - this.lastMessageTime;

        if (
          elapsed <= DISCONNECT_TIMEOUT_MS
        ) {
          return;
        }

        logger.warn(
          'TwelveData 데이터 수신이 중단되어 WebSocket을 재연결합니다.',
          {
            subsystem: 'twelvedata-websocket',
            event: 'twelvedata_websocket_stalled',
            elapsedMs: elapsed,
          },
        );

        const ws = this.ws;

        if (ws) {
          /*
           * terminate()
           *   ↓
           * close event
           *   ↓
           * scheduleReconnect()
           *
           * reconnect 자체는 close handler가 담당한다.
           */
          ws.terminate();
          return;
        }

        this.scheduleReconnect();
      },
      HEARTBEAT_CHECK_MS,
    );
  }

  private stopHeartbeatMonitor(): void {
    if (!this.heartbeatInterval) {
      return;
    }

    clearInterval(
      this.heartbeatInterval,
    );

    this.heartbeatInterval = null;
  }

  private cleanupConnection(): void {
    if (this.reconnectTimer) {
      clearTimeout(
        this.reconnectTimer,
      );

      this.reconnectTimer = null;
    }

    this.stopHeartbeatMonitor();

    const ws = this.ws;

    this.ws = null;

    if (!ws) {
      return;
    }

    /*
     * 의도적인 cleanup에서 발생하는 close event가
     * reconnect를 예약하지 못하게 listener를 제거한다.
     */
    ws.removeAllListeners();

    ws.close();
  }

  private async processMessage(
    data: WebSocket.RawData,
  ): Promise<void> {
    try {
      const text =
        typeof data === 'string'
          ? data
          : data.toString();

      const message = JSON.parse(text);

      if (
        message.event === 'price' &&
        typeof message.symbol === 'string'
      ) {
        const price =
          Number(message.price);

        const timestamp =
          Number(message.timestamp);

        if (
          !Number.isFinite(price) ||
          !Number.isFinite(timestamp) ||
          timestamp <= 0
        ) {
          logger.warn(
            '유효하지 않은 TwelveData 가격 이벤트를 무시했습니다.',
            {
              subsystem: 'twelvedata-websocket',
              event:
                'twelvedata_price_event_invalid',
              symbol: message.symbol,
            },
          );

          return;
        }

        await this.handlePriceUpdate(
          message.symbol,
          price,
          timestamp,
        );

        return;
      }

      if (message.event === 'heartbeat') {
        logger.debug(
          'TwelveData heartbeat를 수신했습니다.',
          {
            subsystem:
              'twelvedata-websocket',
            event:
              'twelvedata_heartbeat_received',
          },
        );
      }
    } catch (error) {
      logger.error(
        'TwelveData 메시지 처리에 실패했습니다.',
        {
          subsystem: 'twelvedata-websocket',
          event:
            'twelvedata_message_processing_failed',
          error: getErrorMessage(error),
        },
      );
    }
  }
}

export function buildTwelveDataSubscription(
  symbols: readonly string[],
): TwelveDataSubscription {
  return {
    action: 'subscribe',
    params: {
      symbols: symbols.join(','),
    },
  };
}

async function publishSafely(
  publisher: MarketEventPublisher,
  message: Parameters<
    MarketEventPublisher['publish']
  >[0],
): Promise<void> {
  try {
    await publisher.publish(message);
  } catch {
    // RedisPubSubService reports connection state transitions.
    // Per-event failures are intentionally suppressed
    // to avoid tick-level log storms.
  }
}
