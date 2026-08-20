/**
 * 에포크 초를 Date 객체로 변환
 */
export function epochToDate(epochSec: number): Date {
  return new Date(epochSec * 1000);
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
