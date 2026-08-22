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
import { getErrorMessage, logger } from '../../shared/utils/logger';
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

type GapFillResult =
  | { status: 'unchanged' }
  | { status: 'backfilled'; minUpdatedEpoch: number }
  | { status: 'failed' };

type SymbolBackfillResult =
  | { status: 'no_gap'; symbol: string }
  | { status: 'backfilled'; symbol: string; minUpdatedEpoch: number }
  | { status: 'failed'; symbol: string; minUpdatedEpoch: number | null }
  | { status: 'skipped'; symbol: string };

/**
 * 서버 시작 시 데이터 정합성 검사 및 백필
 */
export async function runHistoricalBackfill(): Promise<void> {
  logger.info('과거 데이터 백필을 시작합니다.', {
    subsystem: 'historical-backfill',
    event: 'historical_backfill_started',
    symbols: SYMBOLS,
  });

  const limit = pLimit(2);

  // 각 심볼별 결과를 독립적으로 집계한다.
  const tasks = SYMBOLS.map(symbol =>
    limit(() => syncSymbol(symbol))
  );

  const results = await Promise.all(tasks);

  const summary = {
    succeeded: results.filter(({ status }) =>
      status === 'no_gap' || status === 'backfilled'
    ).length,
    failed: results.filter(({ status }) => status === 'failed').length,
    skipped: results.filter(({ status }) => status === 'skipped').length,
  };

  // 부분 성공을 포함해 실제 갱신된 내역 중 가장 오래된 시간 찾기
  let globalMinEpoch: number | null = null;

  for (const result of results) {
    if (
      (result.status === 'backfilled' || result.status === 'failed')
      && result.minUpdatedEpoch !== null
    ) {
      if (
        globalMinEpoch === null
        || result.minUpdatedEpoch < globalMinEpoch
      ) {
        globalMinEpoch = result.minUpdatedEpoch;
      }
    }
  }

  let aggregateRefreshFailed = 0;

  // 갱신된 데이터가 있다면 한 번에 CA Refresh 수행
  if (globalMinEpoch !== null) {
    logger.info('Continuous Aggregate 일괄 갱신을 시작합니다.', {
      subsystem: 'historical-backfill',
      event: 'continuous_aggregate_refresh_started',
      from: new Date(globalMinEpoch * 1000).toISOString()
    });

    // DB 부하 고려하여 잠깐 대기
    await delay(500);

    const refreshResult = await candleService.refreshAllContinuousAggregates(
      globalMinEpoch,
    );
    aggregateRefreshFailed = refreshResult.failed;
    const refreshMetadata = {
      subsystem: 'historical-backfill',
      succeeded: refreshResult.succeeded,
      failed: refreshResult.failed,
    };

    if (refreshResult.failed > 0) {
      logger.warn('Continuous Aggregate 일괄 갱신이 일부 오류와 함께 완료되었습니다.', {
        ...refreshMetadata,
        event: 'continuous_aggregate_refresh_completed_with_errors',
      });
    } else {
      logger.info('Continuous Aggregate 일괄 갱신을 완료했습니다.', {
        ...refreshMetadata,
        event: 'continuous_aggregate_refresh_completed',
      });
    }
  } else if (summary.failed === 0 && summary.skipped === 0) {
    logger.info('데이터 공백이 없어 Continuous Aggregate 갱신을 생략합니다.', {
      subsystem: 'historical-backfill',
      event: 'historical_backfill_gap_not_found',
    });
  } else if (summary.failed > 0) {
    logger.warn('백필 실패로 갱신할 데이터가 없어 Continuous Aggregate 갱신을 생략합니다.', {
      subsystem: 'historical-backfill',
      event: 'continuous_aggregate_refresh_skipped',
      failed: summary.failed,
    });
  } else {
    logger.info('백필된 데이터가 없어 Continuous Aggregate 갱신을 생략합니다.', {
      subsystem: 'historical-backfill',
      event: 'continuous_aggregate_refresh_skipped',
      skipped: summary.skipped,
    });
  }

  const completionMetadata = {
    subsystem: 'historical-backfill',
    succeeded: summary.succeeded,
    failed: summary.failed,
    skipped: summary.skipped,
    aggregateRefreshFailed,
  };

  if (summary.failed > 0 || aggregateRefreshFailed > 0) {
    logger.warn('과거 데이터 백필이 일부 오류와 함께 완료되었습니다.', {
      ...completionMetadata,
      event: 'historical_backfill_completed_with_errors',
    });
  } else {
    logger.info('과거 데이터 백필을 완료했습니다.', {
      ...completionMetadata,
      event: 'historical_backfill_completed',
    });
  }
}

async function syncSymbol(symbol: string): Promise<SymbolBackfillResult> {
  try {
    // DB에서 마지막 2개 1분봉 조회 (Race Condition 및 중간 갭 감지용)
    const lastCandles = await candleRepository.getLastCandles(symbol, 2);

    if (lastCandles.length === 0) {
      logger.info('기존 캔들이 없어 초기 적재가 필요합니다.', {
        subsystem: 'historical-backfill',
        event: 'historical_backfill_seed_required',
        symbol,
      });
      return { status: 'skipped', symbol };
    }

    const now = new Date();
    const latestCandle = lastCandles[0]!;
    let minUpdatedTime: number | null = null;
    let syncFailed = false;

    // 1. 마지막 캔들 이후의 갭 체크 (일반적인 다운타임 복구)
    const t1 = await checkAndFillGap(symbol, latestCandle.time, now);
    if (t1.status === 'failed') syncFailed = true;
    if (t1.status === 'backfilled') minUpdatedTime = t1.minUpdatedEpoch;

    // 2. 마지막 캔들 직전의 갭 체크 (Race Condition 복구)
    if (lastCandles.length === 2) {
      const prevCandle = lastCandles[1]!;
      const gapMinutes = (latestCandle.time.getTime() - prevCandle.time.getTime()) / (1000 * 60);

      // 1분봉이므로 2분 이상 차이나면 갭으로 간주
      if (gapMinutes > 2) {
        logger.warn('최신 캔들 직전의 데이터 공백을 발견했습니다.', {
          subsystem: 'historical-backfill',
          event: 'historical_backfill_prior_gap_detected',
          symbol,
          prevTime: prevCandle.time.toISOString(),
          latestTime: latestCandle.time.toISOString(),
          gapMinutes: Math.floor(gapMinutes)
        });

        // latestCandle.time은 이미 존재하므로, 그 전까지만 채움
        const t2 = await checkAndFillGap(symbol, prevCandle.time, latestCandle.time);

        if (t2.status === 'failed') syncFailed = true;
        if (t2.status === 'backfilled') {
          if (
            minUpdatedTime === null
            || t2.minUpdatedEpoch < minUpdatedTime
          ) {
            minUpdatedTime = t2.minUpdatedEpoch;
          }
        }
      }
    }

    if (syncFailed) {
      return { status: 'failed', symbol, minUpdatedEpoch: minUpdatedTime };
    }
    if (minUpdatedTime !== null) {
      return { status: 'backfilled', symbol, minUpdatedEpoch: minUpdatedTime };
    }
    return { status: 'no_gap', symbol };

  } catch (error) {
    if (!isTwelveDataRateLimitError(error)) {
      logger.error('심볼의 과거 데이터 동기화에 실패했습니다.', {
        subsystem: 'historical-backfill',
        event: 'historical_backfill_symbol_failed',
        symbol,
        error: getErrorMessage(error),
      });
    }
    return { status: 'failed', symbol, minUpdatedEpoch: null };
  }
}

async function checkAndFillGap(
  symbol: string,
  startTime: Date,
  endTime: Date,
): Promise<GapFillResult> {
  const diffMinutes = (endTime.getTime() - startTime.getTime()) / (1000 * 60);

  // 2분 미만 갭은 무시
  if (diffMinutes < 2) {
    return { status: 'unchanged' };
  }

  logger.info('데이터 공백 복구를 시작합니다.', {
    subsystem: 'historical-backfill',
    event: 'historical_backfill_gap_recovery_started',
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

  if (apiStartDate >= apiEndDate) return { status: 'unchanged' };

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
    logger.error('과거 데이터 동기화 중 TwelveData API 오류가 발생했습니다.', {
      subsystem: 'historical-backfill',
      event: 'historical_backfill_upstream_failed',
      symbol,
      upstreamMessage: response.data.message,
    });
    return { status: 'failed' };
  }

  const candles = response.data.values;
  if (!candles || candles.length === 0) {
    logger.info('해당 기간에 복구할 데이터가 없습니다.', {
      subsystem: 'historical-backfill',
      event: 'historical_backfill_period_empty',
      symbol,
    });
    return { status: 'unchanged' };
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

  logger.info('과거 캔들 복구를 완료했습니다.', {
    subsystem: 'historical-backfill',
    event: 'historical_backfill_candles_recovered',
    symbol,
    count,
  });

  // DB 부하 분산: 다른 요청 처리를 위해 잠시 대기
  await delay(500);

  // 개별 리프레시 제거 -> 상위에서 일괄 처리
  // 리턴값: 갱신 시작 Epoch Time (초)
  return {
    status: 'backfilled',
    minUpdatedEpoch: Math.floor(apiStartDate.getTime() / 1000),
  };
}

function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
