import { prisma } from '../../shared';
import { Prisma } from '@prisma/client';
import { Candle, FormattedCandle } from './candle.types';
import { DailyTimeframe } from './candle.constants';

export class CandleRepository {
  /**
   * 1분봉 조회 (범위)
   */
  async find1mCandles(
    symbol: string,
    options: {
      from?: Date;
      to?: Date;
      limit?: number;
      orderDesc?: boolean;
    }
  ): Promise<FormattedCandle[]> {
    const { from, to, limit = 1000, orderDesc = true } = options;

    const where: any = { symbol };
    
    if (from && to) {
      where.time = { gte: from, lte: to };
    } else if (to) {
      where.time = { lt: to };
    } else if (from) {
      where.time = { gte: from };
    }

    const data = await prisma.candle1m.findMany({
      where,
      orderBy: { time: orderDesc ? 'desc' : 'asc' },
      take: limit,
    });

    // DESC로 가져왔으면 다시 ASC로 뒤집음 (차트용)
    if (orderDesc) {
      data.reverse();
    }

    return data.map(d => ({
      time: Math.floor(d.time.getTime() / 1000),
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
      volume: d.volume,
    }));
  }

  // ✅ findAggCandles는 더 이상 사용하지 않습니다.
  // TimescaleDB Continuous Aggregates 뷰를 직접 조회하는 findContinuousAggCandles를 사용하세요.

  /**
   * TimescaleDB Continuous Aggregate 뷰에서 집계봉 조회
   * - 5분, 15분, 1시간, 4시간봉 지원
   * - DB가 자동으로 집계한 데이터 조회 (Race Condition 없음)
   */
  async findContinuousAggCandles(
    symbol: string,
    timeframeMinutes: number,
    options: {
      from?: Date;
      to?: Date;
      limit?: number;
      orderDesc?: boolean;
    }
  ): Promise<FormattedCandle[]> {
    const { from, to, limit = 1000, orderDesc = true } = options;

    // 타임프레임별 뷰 이름 매핑
    // ⚠️ SECURITY WARNING: DO NOT use user input directly here.
    // viewName MUST be validated through this whitelist mapping.
    // Direct string interpolation of timeframe into SQL would create SQL injection vulnerability.
    const viewMap: Record<number, string> = {
      5: 'market.candle_5m',
      15: 'market.candle_15m',
      60: 'market.candle_1h',
      240: 'market.candle_4h',
    };

    const viewName = viewMap[timeframeMinutes];
    if (!viewName) {
      throw new Error(`Unsupported timeframe: ${timeframeMinutes}m. Use 1m, 5m, 15m, 1h, or 4h.`);
    }

    // 조건 빌드
    let whereClause = `WHERE symbol = $1`;
    const params: (string | Date | number)[] = [symbol];
    let paramIndex = 2;

    if (from && to) {
      whereClause += ` AND bucket >= $${paramIndex} AND bucket <= $${paramIndex + 1}`;
      params.push(from, to);
      paramIndex += 2;
    } else if (to) {
      whereClause += ` AND bucket < $${paramIndex}`;
      params.push(to);
      paramIndex++;
    } else if (from) {
      whereClause += ` AND bucket >= $${paramIndex}`;
      params.push(from);
      paramIndex++;
    }

    const orderClause = `ORDER BY bucket ${orderDesc ? 'DESC' : 'ASC'}`;
    const limitClause = `LIMIT $${paramIndex}`;
    params.push(limit);

    const query = `
      SELECT bucket, symbol, open, high, low, close, volume
      FROM ${viewName}
      ${whereClause}
      ${orderClause}
      ${limitClause}
    `;

    const data = await prisma.$queryRawUnsafe<Array<{
      bucket: Date;
      symbol: string;
      open: number;
      high: number;
      low: number;
      close: number;
      volume: number;
    }>>(query, ...params);

    // DESC로 가져왔으면 다시 ASC로 뒤집음 (차트용)
    if (orderDesc) {
      data.reverse();
    }

    return data.map(d => ({
      time: Math.floor(d.bucket.getTime() / 1000),
      open: d.open,
      high: d.high,
      low: d.low,
      close: d.close,
      volume: d.volume,
    }));
  }

  /**
   * 1분봉 저장 (단일)
   * 참고: 실시간 데이터는 RocksDB pending store를 거쳐 배치 저장됩니다.
   */
  async save1mCandle(candle: Candle): Promise<void> {
    await prisma.candle1m.create({
      data: {
        symbol: candle.symbol,
        time: new Date(candle.startTime * 1000),
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume,
      },
    });
  }

  /**
   * 1분봉 벌크 저장
   */
  async bulkSave1mCandles(
    candles: Array<{
      symbol: string;
      time: Date;
      open: number;
      high: number;
      low: number;
      close: number;
      volume: number;
    }>
  ): Promise<number> {
    const result = await prisma.candle1m.createMany({
      data: candles,
      skipDuplicates: true,
    });
    return result.count;
  }

  // ✅ upsertAggCandle 함수는 삭제되었습니다.
  // TimescaleDB Continuous Aggregates가 자동으로 집계를 관리합니다.
  // 애플리케이션에서 집계 데이터를 직접 INSERT/UPDATE하지 마세요.

  /**
   * 심볼의 1분봉 데이터 범위 조회
   */
  async get1mBoundary(symbol: string): Promise<{ min: Date | null; max: Date | null }> {
    const result = await prisma.candle1m.aggregate({
      where: { symbol },
      _min: { time: true },
      _max: { time: true },
    });
    return {
      min: result._min.time,
      max: result._max.time,
    };
  }

  /**
   * 심볼의 마지막 N개 1분봉 조회
   */
  async getLastCandles(symbol: string, limit: number = 1) {
    const candles = await prisma.candle1m.findMany({
      where: { symbol },
      orderBy: { time: 'desc' },
      take: limit,
    });
    return candles; // desc 정렬 그대로 반환 (index 0이 가장 최신)
  }

  /**
   * 심볼의 마지막 1분봉 조회
   */
  async getLast1mCandle(symbol: string) {
    return prisma.candle1m.findFirst({
      where: { symbol },
      orderBy: { time: 'desc' },
    });
  }

  /**
   * 특정 범위의 1분봉 조회 (집계용)
   */
  async get1mCandlesForAggregation(
    symbol: string,
    from: Date,
    to: Date
  ) {
    return prisma.candle1m.findMany({
      where: {
        symbol,
        time: { gte: from, lt: to },
      },
      orderBy: { time: 'asc' },
    });
  }

  /**
   * 일봉/주봉/월봉 조회 (TimescaleDB Continuous Aggregates 사용)
   * 
   * 1분봉 데이터를 기반으로 DB가 자동 집계한 일/주/월봉을 조회합니다.
   * 기존 CandleDaily/Weekly/Monthly 테이블 대신 CA 뷰 사용.
   */
  async findDailyCandles(
    symbol: string,
    timeframe: DailyTimeframe,
    options: {
      from?: Date;
      to?: Date;
      limit?: number;
      orderDesc?: boolean;
    }
  ): Promise<FormattedCandle[]> {
    const { from, to, limit = 1000, orderDesc = true } = options;

    // 타임프레임별 뷰 이름 매핑
    const viewMap: Record<DailyTimeframe, string> = {
      '1D': 'market.candle_1d',
      '1W': 'market.candle_1w',
      '1M': 'market.candle_1mo',
    };

    const viewName = viewMap[timeframe];

    // 조건 빌드
    let whereClause = `WHERE symbol = $1`;
    const params: (string | Date | number)[] = [symbol];
    let paramIndex = 2;

    if (from && to) {
      whereClause += ` AND bucket >= $${paramIndex} AND bucket <= $${paramIndex + 1}`;
      params.push(from, to);
      paramIndex += 2;
    } else if (to) {
      whereClause += ` AND bucket < $${paramIndex}`;
      params.push(to);
      paramIndex++;
    } else if (from) {
      whereClause += ` AND bucket >= $${paramIndex}`;
      params.push(from);
      paramIndex++;
    }

    const orderClause = `ORDER BY bucket ${orderDesc ? 'DESC' : 'ASC'}`;
    const limitClause = `LIMIT $${paramIndex}`;
    params.push(limit);

    const query = `
      SELECT bucket, symbol, open, high, low, close, volume
      FROM ${viewName}
      ${whereClause}
      ${orderClause}
      ${limitClause}
    `;

    try {
      const data = await prisma.$queryRawUnsafe<Array<{
        bucket: Date;
        symbol: string;
        open: number;
        high: number;
        low: number;
        close: number;
        volume: number;
      }>>(query, ...params);

      // DESC로 가져왔으면 다시 ASC로 뒤집음 (차트용)
      if (orderDesc) {
        data.reverse();
      }

      return data.map(d => ({
        time: Math.floor(d.bucket.getTime() / 1000),
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
        volume: d.volume,
      }));
    } catch (error) {
      throw new Error(`Failed to query Continuous Aggregate view ${viewName}: ${error}`);
    }
  }

  // ==================== CA 뷰 새로고침 (데이터 복구 시 사용) ====================

  /**
   * TimescaleDB Continuous Aggregate 뷰를 수동으로 새로고침
   * 1분봉 데이터를 백필한 후 상위 타임프레임에 반영하기 위해 사용
   */
  async refreshContinuousAggregate(
    timeframe: string,
    fromDate?: Date,
    toDate?: Date
  ): Promise<void> {
    const viewMap: Record<string, string> = {
      '5m': 'market.candle_5m',
      '15m': 'market.candle_15m',
      '1h': 'market.candle_1h',
      '4h': 'market.candle_4h',
      '1D': 'market.candle_1d',
      '1W': 'market.candle_1w',
      '1M': 'market.candle_1mo',
    };

    const viewName = viewMap[timeframe];
    if (!viewName) {
      throw new Error(`Unknown timeframe for CA refresh: ${timeframe}`);
    }

    await prisma.$executeRawUnsafe(
      `CALL public.refresh_continuous_aggregate('${viewName}', $1::timestamptz, $2::timestamptz)`,
      fromDate ?? null,
      toDate ?? null
    );
  }

  /**
   * 모든 CA 뷰를 새로고침
   */
  async refreshAllContinuousAggregates(fromDate?: Date): Promise<void> {
    const timeframes = ['5m', '15m', '1h', '4h', '1D', '1W', '1M'];
    
    for (const tf of timeframes) {
      try {
        await this.refreshContinuousAggregate(tf, fromDate);
        console.log(`✅ Refreshed ${tf} CA view`);
      } catch (error) {
        console.error(`❌ Failed to refresh ${tf} CA view:`, error);
      }
    }
  }
}

export const candleRepository = new CandleRepository();
