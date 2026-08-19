import { Candle } from './candle.types';

/**
 * 실시간 틱 데이터로부터 1분봉을 조립하는 클래스
 */
export class CandleMaker {
  private currentCandle: Candle | null = null;

  /**
   * 새 틱 데이터로 캔들 업데이트
   * @returns 완성된 캔들 (새 분봉 시작 시) 또는 null
   */
  update(
    symbol: string,
    price: number,
    volume: number = 0,
    timestamp: number
  ): Candle | null {
    const candleStartTime = Math.floor(timestamp / 60) * 60;

    // 새 분봉 시작
    if (!this.currentCandle || this.currentCandle.startTime !== candleStartTime) {
      const completedCandle = this.currentCandle;

      this.currentCandle = {
        symbol,
        startTime: candleStartTime,
        open: price,
        high: price,
        low: price,
        close: price,
        volume,
      };

      return completedCandle;
    }

    // 기존 분봉 업데이트
    this.currentCandle.high = Math.max(this.currentCandle.high, price);
    this.currentCandle.low = Math.min(this.currentCandle.low, price);
    this.currentCandle.close = price;
    this.currentCandle.volume += volume;

    return null;
  }

  getCurrentCandle(): Candle | null {
    return this.currentCandle;
  }

  reset(): void {
    this.currentCandle = null;
  }
}
