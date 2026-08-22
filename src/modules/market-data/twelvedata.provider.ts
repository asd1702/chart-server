import WebSocket from 'ws';
import config from '../../config';
import { CandleMaker } from '../candle/candle.maker';
import { enqueueCandle } from '../candle/candle.persistence';
import type { MarketEventPublisher } from '../messaging/pubsub.interface';
import { getErrorMessage, logger } from '../../shared/utils/logger';
import type { TwelveDataSubscription } from './market-data.types';

const SYMBOLS = config.market.streamSymbols;
const candleMakers = new Map<string, CandleMaker>();

// 각 심볼마다 CandleMaker 인스턴스 생성
SYMBOLS.forEach((s) => candleMakers.set(s, new CandleMaker()));

let tdWs: WebSocket | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let reconnectEnabled = false;
let eventPublisher: MarketEventPublisher | null = null;
const activeMessageHandlers = new Set<Promise<void>>();

// Heartbeat state
let lastMessageTime: number = Date.now();
let heartbeatInterval: NodeJS.Timeout | null = null;
const HEARTBEAT_CHECK_MS = 30000; // 30초마다 검사
const DISCONNECT_TIMEOUT_MS = 60000; // 60초간 데이터 없으면 재연결

/**
 * TwelveData WebSocket 연결
 */
export function connectToTwelveData(publisher: MarketEventPublisher): void {
  eventPublisher = publisher;
  reconnectEnabled = true;

  // 기존 연결 정리
  cleanup();

  logger.info('TwelveData WebSocket 연결을 시작합니다.', {
    subsystem: 'twelvedata-websocket',
    event: 'twelvedata_websocket_connecting',
  });

  tdWs = new WebSocket(
    `wss://ws.twelvedata.com/v1/quotes/price?apikey=${config.TWELVE_DATA_API_KEY}`
  );

  tdWs.on('open', () => {
    logger.info('TwelveData WebSocket에 연결되었습니다.', {
      subsystem: 'twelvedata-websocket',
      event: 'twelvedata_websocket_connected',
    });
    lastMessageTime = Date.now();
    startHeartbeatMonitor(); // 모니터링 시작

    tdWs?.send(
      JSON.stringify(buildTwelveDataSubscription(SYMBOLS))
    );
    logger.info('TwelveData 심볼 구독을 요청했습니다.', {
      subsystem: 'twelvedata-websocket',
      event: 'twelvedata_subscription_sent',
      symbols: SYMBOLS,
    });
  });

  tdWs.on('message', (data: WebSocket.RawData) => {
    lastMessageTime = Date.now(); // 활동 갱신
    const operation = processMessage(data, publisher);
    activeMessageHandlers.add(operation);
    void operation.finally(() => activeMessageHandlers.delete(operation));
  });

  tdWs.on('error', (err) => {
    logger.error('TwelveData WebSocket 오류가 발생했습니다.', {
      subsystem: 'twelvedata-websocket',
      event: 'twelvedata_websocket_error',
      error: err.message,
    });
  });

  tdWs.on('close', (code) => {
    logger.warn('TwelveData WebSocket 연결이 종료되어 재연결을 예약합니다.', {
      subsystem: 'twelvedata-websocket',
      event: 'twelvedata_websocket_disconnected',
      code,
      retryAfterMs: 5000,
    });
    cleanup(); // 인터벌 정지
    if (reconnectEnabled) {
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        if (eventPublisher) connectToTwelveData(eventPublisher);
      }, 5000);
    }
  });
}

export function buildTwelveDataSubscription(
  symbols: readonly string[],
): TwelveDataSubscription {
  return {
    action: 'subscribe',
    params: { symbols: symbols.join(',') },
  };
}

function startHeartbeatMonitor() {
  if (heartbeatInterval) clearInterval(heartbeatInterval);

  heartbeatInterval = setInterval(() => {
    const elapsed = Date.now() - lastMessageTime;

    if (elapsed > DISCONNECT_TIMEOUT_MS) {
      logger.warn('TwelveData 데이터 수신이 중단되어 WebSocket을 재연결합니다.', {
        subsystem: 'twelvedata-websocket',
        event: 'twelvedata_websocket_stalled',
        elapsedMs: elapsed,
      });
      // 강제 재연결 (기존 소켓 close -> close 이벤트 핸들러가 재연결 트리거)
      if (tdWs) {
        tdWs.terminate(); // 즉시 종료
      } else {
        if (eventPublisher) connectToTwelveData(eventPublisher);
      }
    }
  }, HEARTBEAT_CHECK_MS);
}

function cleanup() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (heartbeatInterval) {
    clearInterval(heartbeatInterval);
    heartbeatInterval = null;
  }
  if (tdWs) {
    tdWs.removeAllListeners(); // 리스너 제거로 중복 실행 방지
    tdWs.close();
    tdWs = null;
  }
}

/**
 * 가격 업데이트 처리
 * 
 * 집계(Aggregation)는 TimescaleDB Continuous Aggregates가 담당
 * 애플리케이션은 1분봉 저장과 실시간 브로드캐스트만 담당
 */
export async function handlePriceUpdate(
  symbol: string,
  price: number,
  timestamp: number,
  publisher: MarketEventPublisher
): Promise<void> {
  // Redis I/O 전에 CandleMaker state를 먼저 갱신한다.
  const maker = candleMakers.get(symbol);
  const completedCandle = maker?.update(symbol, price, 0, timestamp) ?? null;

  // 완성된 candle은 realtime delivery보다 durability를 먼저 확보한다.
  if (completedCandle) {
    await enqueueCandle(completedCandle);
  }

  // Tick은 durability 대상이 아닌 best-effort realtime event다.
  await publishSafely(publisher, { type: 'tick', symbol, price, timestamp });

  if (completedCandle) {
    // RocksDB enqueue 성공 후에만 candle realtime event를 발행한다.
    await publishSafely(publisher, { type: 'candle', timeframe: '1m', candle: completedCandle });
    logger.debug('1분 캔들이 완성되었습니다.', {
      subsystem: 'twelvedata-websocket',
      event: 'candle_completed',
      symbol,
      time: new Date(completedCandle.startTime * 1000).toISOString(),
    });

    // 상위 타임프레임 집계는 TimescaleDB Continuous Aggregates가 처리
    // 애플리케이션 레벨에서 집계하지 않음 (Race Condition 방지)
  }
}

/**
 * TwelveData 연결 해제
 */
export async function disconnectFromTwelveData(): Promise<void> {
  const wasActive = reconnectEnabled || tdWs !== null || eventPublisher !== null;
  reconnectEnabled = false;
  cleanup();
  await Promise.allSettled(activeMessageHandlers);
  eventPublisher = null;
  if (wasActive) {
    logger.info('TwelveData WebSocket 수집을 중지했습니다.', {
      subsystem: 'twelvedata-websocket',
      event: 'twelvedata_websocket_stopped',
    });
  }
}

async function processMessage(
  data: WebSocket.RawData,
  publisher: MarketEventPublisher
): Promise<void> {
  try {
    const text = typeof data === 'string' ? data : data.toString();
    const message = JSON.parse(text);

    if (message.event === 'price' && typeof message.symbol === 'string') {
      const price = Number(message.price);
      const timestamp = Number(message.timestamp);
      if (!Number.isFinite(price) || !Number.isFinite(timestamp) || timestamp <= 0) {
        logger.warn('유효하지 않은 TwelveData 가격 이벤트를 무시했습니다.', {
          subsystem: 'twelvedata-websocket',
          event: 'twelvedata_price_event_invalid',
          symbol: message.symbol,
        });
        return;
      }
      await handlePriceUpdate(message.symbol, price, timestamp, publisher);
    } else if (message.event === 'heartbeat') {
      logger.debug('TwelveData heartbeat를 수신했습니다.', {
        subsystem: 'twelvedata-websocket',
        event: 'twelvedata_heartbeat_received',
      });
    }
  } catch (error) {
    logger.error('TwelveData 메시지 처리에 실패했습니다.', {
      subsystem: 'twelvedata-websocket',
      event: 'twelvedata_message_processing_failed',
      error: getErrorMessage(error),
    });
  }
}

async function publishSafely(
  publisher: MarketEventPublisher,
  message: Parameters<MarketEventPublisher['publish']>[0]
): Promise<void> {
  try {
    await publisher.publish(message);
  } catch {
    // RedisPubSubService reports connection state transitions. Per-event
    // failures are intentionally suppressed to avoid tick-level log storms.
  }
}

export function resetTwelveDataCandleState(): void {
  candleMakers.clear();

  for(const symbol of SYMBOLS){
    candleMakers.set(
      symbol,
      new CandleMaker(),
    );
  }

  logger.info('TwelveData candle state를 초기화했습니다.', {
    event: 'twelvedata_candle_state_reset',
    symbols: SYMBOLS,
  },);
}
