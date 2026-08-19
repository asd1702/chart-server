// Timeframe 상수 및 유틸리티

// 분봉 타임프레임 (분 단위)
export const TIMEFRAME_MAP: Record<string, number> = {
  '1m': 1,
  '5m': 5,
  '15m': 15,
  '1h': 60,
  '4h': 240,
};

// 일봉/주봉/월봉 타임프레임 (특수 값)
export const DAILY_TIMEFRAMES = ['1D', '1W', '1M'] as const;
export type DailyTimeframe = typeof DAILY_TIMEFRAMES[number];

export const VALID_TIMEFRAME_KEYS = [...Object.keys(TIMEFRAME_MAP), ...DAILY_TIMEFRAMES];
export const AGG_TIMEFRAMES = [5, 15, 60, 240]; // 집계 대상 타임프레임 (분)
export const AGGREGATE_REFRESH_TIMEFRAMES = [
  '5m',
  '15m',
  '1h',
  '4h',
  ...DAILY_TIMEFRAMES,
] as const;

/**
 * 일봉/주봉/월봉 타임프레임인지 확인
 */
export function isDailyTimeframe(tf: string): tf is DailyTimeframe {
  return DAILY_TIMEFRAMES.includes(tf as DailyTimeframe);
}

/**
 * 문자열 타임프레임을 분 단위로 변환
 * @throws Error if invalid timeframe (for intraday only)
 */
export function parseTimeframe(tf: string): number {
  if (isDailyTimeframe(tf)) {
    throw new Error(`parseTimeframe should not be called for daily timeframes: ${tf}`);
  }
  const minutes = TIMEFRAME_MAP[tf];
  if (!minutes) {
    throw new Error(`Unsupported timeframe '${tf}'. Valid: ${VALID_TIMEFRAME_KEYS.join(', ')}`);
  }
  return minutes;
}

/**
 * 타임프레임 유효성 검사 (일봉 포함)
 */
export function validateTimeframe(tf: string): void {
  if (!VALID_TIMEFRAME_KEYS.includes(tf)) {
    throw new Error(`Unsupported timeframe '${tf}'. Valid: ${VALID_TIMEFRAME_KEYS.join(', ')}`);
  }
}

/**
 * 분 단위를 문자열 타임프레임으로 변환
 */
export function timeframeToString(minutes: number): string {
  const entry = Object.entries(TIMEFRAME_MAP).find(([, v]) => v === minutes);
  return entry ? entry[0] : `${minutes}m`;
}

/**
 * 해당 분봉이 특정 타임프레임의 마지막 분봉인지 확인
 */
export function isCandleEndOfTimeframe(minuteStartEpoch: number, timeframeMinutes: number): boolean {
  const endMinuteIndex = minuteStartEpoch / 60;
  return (endMinuteIndex + 1) % timeframeMinutes === 0;
}
