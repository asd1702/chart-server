/**
 * Candle 타입 정의
 * 
 * 집계 데이터(5m, 15m, 1h, 4h, 1D, 1W, 1M)는 TimescaleDB Continuous Aggregates가 관리합니다.
 * 애플리케이션에서는 1분봉만 저장하고, 상위 타임프레임은 DB 뷰를 조회합니다.
 */

export interface Candle {
  symbol: string;
  startTime: number; // epoch seconds
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// API Response 타입
export interface CandleResponse {
  symbol: string;
  timeframe: string;
  data: FormattedCandle[];
}

export interface FormattedCandle {
  time: number; // epoch seconds
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

// Request 파라미터 타입
export interface GetCandlesParams {
  symbol: string;
  timeframe: string;
}

export interface GetCandlesQuery {
  limit?: string;
  from?: string;
  to?: string;
}
