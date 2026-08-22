import { candleRepository } from './candle.repository';
import type { ContinuousAggregateRefreshResult } from './candle.repository';
import { 
  CandleResponse, 
  FormattedCandle, 
  Candle
} from './candle.types';
import { 
  parseTimeframe, 
  isDailyTimeframe,
  validateTimeframe,
  DailyTimeframe
} from './candle.constants';
import { epochToDate } from '../../shared';

export class CandleService {
  /**
   * 캔들 데이터 조회 (1분봉, 집계봉, 일봉/주봉/월봉)
   */
  async getCandles(
    symbol: string,
    timeframeStr: string,
    options: {
      limit?: number | undefined;
      from?: number | undefined; // epoch seconds
      to?: number | undefined;   // epoch seconds
    }
  ): Promise<CandleResponse> {
    const { from, to} = options;
    const requestLimit = options.limit ?? 300;
    const fetchLimit = requestLimit + 1;
    
    // 타임프레임 유효성 검사
    validateTimeframe(timeframeStr);

    let data: FormattedCandle[];

    // Case A: 일봉/주봉/월봉
    if (isDailyTimeframe(timeframeStr)) {
      data = await candleRepository.findDailyCandles(symbol, timeframeStr as DailyTimeframe, {
        ...(from && { from: epochToDate(from) }),
        ...(to && { to: epochToDate(to) }),
        limit: fetchLimit,
        orderDesc: !(from !== undefined && to !== undefined),
      });
    }
    // Case B: 분봉 (1m, 5m, 15m, 1h, 4h)
    else {
      const timeframeMinutes = parseTimeframe(timeframeStr);
      const is1m = timeframeMinutes === 1;

      // Case B-1: 범위 조회 (Gap Filling)
      if (from !== undefined && to !== undefined) {
        if (is1m) {
          data = await candleRepository.find1mCandles(symbol, {
            from: epochToDate(from),
            to: epochToDate(to),
            limit: 5000,
            orderDesc: false,
          });
        } else {
          // TimescaleDB Continuous Aggregate 사용
          data = await candleRepository.findContinuousAggCandles(symbol, timeframeMinutes, {
            from: epochToDate(from),
            to: epochToDate(to),
            limit: 5000,
            orderDesc: false,
          });
        }
      }
      // Case B-2: 페이지네이션 / 초기 로드
      else {
        if (is1m) {
          data = await candleRepository.find1mCandles(symbol, {
            ...(to && { to: epochToDate(to) }),
            limit: fetchLimit,
            orderDesc: true,
          });
        } else {
          // TimescaleDB Continuous Aggregate 사용
          data = await candleRepository.findContinuousAggCandles(symbol, timeframeMinutes, {
            ...(to && { to: epochToDate(to) }),
            limit: fetchLimit,
            orderDesc: true,
          });
        }
      }
    }

    return {
      symbol,
      timeframe: timeframeStr,
      data,
    };
  }

  /**
   * 1분봉 저장 (단일)
   * 참고: 실시간 데이터는 RocksDB pending store를 거쳐 배치 저장됩니다.
   */
  async save1mCandle(candle: Candle): Promise<void> {
    await candleRepository.save1mCandle(candle);
  }

  /**
   * TimescaleDB Continuous Aggregate 뷰를 수동으로 새로고침
   * 1분봉 데이터를 백필한 후 상위 타임프레임에 반영하기 위해 사용
   */
  async refreshContinuousAggregate(
    timeframe: string,
    startEpochSec?: number,
    endEpochSec?: number
  ): Promise<void> {
    const fromDate = startEpochSec ? epochToDate(startEpochSec) : undefined;
    const toDate = endEpochSec ? epochToDate(endEpochSec) : undefined;
    
    await candleRepository.refreshContinuousAggregate(timeframe, fromDate, toDate);
  }

  /**
   * 모든 CA 뷰를 새로고침 (백필 후 사용)
   */
  async refreshAllContinuousAggregates(
    startEpochSec?: number,
  ): Promise<ContinuousAggregateRefreshResult> {
    const fromDate = startEpochSec ? epochToDate(startEpochSec) : undefined;
    return candleRepository.refreshAllContinuousAggregates(fromDate);
  }

  /**
   * 데이터 범위 조회
   */
  async getDataBoundary(symbol: string) {
    return candleRepository.get1mBoundary(symbol);
  }
}

export const candleService = new CandleService();
