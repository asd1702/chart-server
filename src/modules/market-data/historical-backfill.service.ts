/**
 * Optional startup backfill for market-data gaps created while the ingestor
 * was offline. Runtime streaming does not depend on this feature.
 * 
 * 서버가 꺼져 있던 동안의 데이터 누락을 자동으로 복구합니다.
 * - Rate Limiter(Bottleneck) 적용으로 API 제한 준수
 * - TimescaleDB Continuous Aggregates 사용으로 상위 타임프레임 집계 불필요
 */

import axios from 'axios';
import config from '../../config';
import { candleRepository } from '../candle';
import { logger } from '../../shared/utils/logger';
import {
  isTwelveDataRateLimitError,
  scheduleTwelveDataRequest,
} from '../../shared/utils/rate-limiter';
import { candleService } from '../candle';
import pLimit from 'p-limit';

const SYMBOLS = config.market.streamSymbols;

// TwelveData API 응답 타입
interface TwelveDataTimeSeriesResponse {
  status?: string;
  message?: string;
  values?: TwelveDataCandle[];
}

interface TwelveDataCandle {
  datetime: string;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
}

/**
 * 서버 시작 시 데이터 정합성 검사 및 백필
 */
export async function runHistoricalBackfill(): Promise<void> {
  logger.info('Historical backfill started', {
    event: 'historical_backfill_started',
    symbols: SYMBOLS,
  });

  const limit = pLimit(2);

  // 각 심볼별로 가장 오래된 갱신 시점을 반환받음
  const tasks = SYMBOLS.map(symbol =>
    limit(() => syncSymbol(symbol))
  );

  const results = await Promise.all(tasks);

  // 갱신된 내역 중 가장 오래된 시간 찾기
  let globalMinEpoch: number | null = null;

  for (const epoch of results) {
    if (epoch !== null) {
      if (globalMinEpoch === null || epoch < globalMinEpoch) {
        globalMinEpoch = epoch;
      }
    }
  }

  // 갱신된 데이터가 있다면 한 번에 CA Refresh 수행
  if (globalMinEpoch !== null) {
    logger.info('Triggering batched Continuous Aggregate refresh...', {
      from: new Date(globalMinEpoch * 1000).toISOString()
    });

    // DB 부하 고려하여 잠깐 대기
    await delay(500);

    await candleService.refreshAllContinuousAggregates(globalMinEpoch);
    logger.info('Batched CA Refresh completed.');
  } else {
    logger.info('No data gaps found. Skipping CA Refresh.');
  }

  logger.info('Historical backfill completed', {
    event: 'historical_backfill_completed',
  });
}

// 가장 이른 갱신 시간 복귀 (없으면 null)
async function syncSymbol(symbol: string): Promise<number | null> {
  try {
    // DB에서 마지막 2개 1분봉 조회 (Race Condition 및 중간 갭 감지용)
    const lastCandles = await candleRepository.getLastCandles(symbol, 2);

    if (lastCandles.length === 0) {
      logger.info('No existing data (new symbol). Initial seeding required.', { symbol });
      return null;
    }

    const now = new Date();
    const latestCandle = lastCandles[0]!;
    let minUpdatedTime: number | null = null;

    // 1. 마지막 캔들 이후의 갭 체크 (일반적인 다운타임 복구)
    const t1 = await checkAndFillGap(symbol, latestCandle.time, now);
    if (t1) minUpdatedTime = t1;

    // 2. 마지막 캔들 직전의 갭 체크 (Race Condition 복구)
    if (lastCandles.length === 2) {
      const prevCandle = lastCandles[1]!;
      const gapMinutes = (latestCandle.time.getTime() - prevCandle.time.getTime()) / (1000 * 60);

      // 1분봉이므로 2분 이상 차이나면 갭으로 간주
      if (gapMinutes > 2) {
        logger.warn('Gap detected BEFORE the latest candle (Race Condition recovery)', {
          symbol,
          prevTime: prevCandle.time.toISOString(),
          latestTime: latestCandle.time.toISOString(),
          gapMinutes: Math.floor(gapMinutes)
        });

        // latestCandle.time은 이미 존재하므로, 그 전까지만 채움
        const t2 = await checkAndFillGap(symbol, prevCandle.time, latestCandle.time);

        // 더 이른 시간을 minUpdatedTime으로 설정
        if (t2) {
          if (minUpdatedTime === null || t2 < minUpdatedTime) {
            minUpdatedTime = t2;
          }
        }
      }
    }

    return minUpdatedTime;

  } catch (error) {
    if (isTwelveDataRateLimitError(error)) return null;
    logger.error('Sync failed for symbol', {
      symbol,
      error: error instanceof Error ? error.message : 'Unknown error',
    });
    return null;
  }
}

// 갱신 시작 시간(초) 반환, 없으면 null
async function checkAndFillGap(symbol: string, startTime: Date, endTime: Date): Promise<number | null> {
  const diffMinutes = (endTime.getTime() - startTime.getTime()) / (1000 * 60);

  // 2분 미만 갭은 무시
  if (diffMinutes < 2) {
    return null;
  }

  logger.info('Data gap detected, starting recovery...', {
    symbol,
    start: startTime.toISOString(),
    end: endTime.toISOString(),
    gapMinutes: Math.floor(diffMinutes)
  });

  // TwelveData Time Series API 호출
  const apiStartDate = new Date(startTime.getTime() + 60000); // 시작 + 1분

  // API 호출 시 end_date가 미래면 현재 시간으로 조정 (API 에러 방지)
  const now = new Date();
  const apiEndDate = endTime > now ? now : endTime;

  if (apiStartDate >= apiEndDate) return null;

  const response = await scheduleTwelveDataRequest(() =>
    axios.get<TwelveDataTimeSeriesResponse>('https://api.twelvedata.com/time_series', {
      params: {
        symbol,
        interval: '1min',
        apikey: config.TWELVE_DATA_API_KEY,
        start_date: apiStartDate.toISOString(),
        end_date: apiEndDate.toISOString(),
        outputsize: 5000,
        order: 'ASC',
      },
    })
  );

  if (response.data.status === 'error') {
    logger.error('TwelveData API error during sync', {
      symbol,
      message: response.data.message
    });
    return null;
  }

  const candles = response.data.values;
  if (!candles || candles.length === 0) {
    logger.info('No data to recover for period', { symbol });
    return null;
  }

  // 1분봉 벌크 저장
  const count = await candleRepository.bulkSave1mCandles(
    candles.map((c: TwelveDataCandle) => ({
      symbol,
      time: new Date(c.datetime),
      open: parseFloat(c.open),
      high: parseFloat(c.high),
      low: parseFloat(c.low),
      close: parseFloat(c.close),
      volume: parseInt(c.volume) || 0,
    }))
  );

  logger.info('Candles recovered successfully', { symbol, count });

  // DB 부하 분산: 다른 요청 처리를 위해 잠시 대기
  await delay(500);

  // 개별 리프레시 제거 -> 상위에서 일괄 처리
  // 리턴값: 갱신 시작 Epoch Time (초)
  return Math.floor(apiStartDate.getTime() / 1000);
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
