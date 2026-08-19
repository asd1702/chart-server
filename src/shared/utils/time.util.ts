/**
 * 에포크 초를 Date 객체로 변환
 */
export function epochToDate(epochSec: number): Date {
  return new Date(epochSec * 1000);
}

/**
 * Date 객체를 에포크 초로 변환
 */
export function dateToEpoch(date: Date): number {
  return Math.floor(date.getTime() / 1000);
}

/**
 * 에포크 초를 특정 타임프레임(분 단위)의 시작 시점으로 정렬
 */
export function alignToTimeframe(epochSec: number, timeframeMinutes: number): number {
  const minuteIndex = Math.floor(epochSec / 60);
  const alignedMinuteIndex = Math.floor(minuteIndex / timeframeMinutes) * timeframeMinutes;
  return alignedMinuteIndex * 60;
}

/**
 * 현재 시간을 에포크 초로 반환
 */
export function nowEpoch(): number {
  return Math.floor(Date.now() / 1000);
}

/**
 * 분 단위 차이 계산
 */
export function diffMinutes(from: Date, to: Date): number {
  return (to.getTime() - from.getTime()) / (1000 * 60);
}

/**
 * ISO 문자열 또는 에포크 초를 에포크 초로 통합 변환
 */
export function toEpochSec(value: unknown): number | undefined {
  if (value === undefined || value === null) return undefined;
  
  if (typeof value === 'number') {
    return Math.floor(value);
  }
  
  if (typeof value === 'string') {
    // 숫자 문자열인 경우
    if (/^\d+$/.test(value)) {
      return Math.floor(Number(value));
    }
    // ISO 문자열인 경우
    const ms = Date.parse(value);
    if (!Number.isNaN(ms)) {
      return Math.floor(ms / 1000);
    }
  }
  
  return undefined;
}
