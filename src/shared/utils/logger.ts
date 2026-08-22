/**
 * Winston Logger - 구조화된 로깅
 * 
 * 특징:
 * - 프로덕션: JSON 포맷 (로그 분석 도구 호환)
 * - 개발: 컬러풀한 텍스트 포맷 (가독성)
 * - 에러 로그 별도 파일 저장
 */

import winston from 'winston';
import path from 'path';

// 로그 레벨 정의
const levels = {
  error: 0,
  warn: 1,
  info: 2,
  http: 3,
  debug: 4,
};

// 개발 환경용 컬러
const colors = {
  error: 'red',
  warn: 'yellow',
  info: 'green',
  http: 'magenta',
  debug: 'white',
};

winston.addColors(colors);

// 환경 판단 (config import 시 순환 참조 방지)
const isDevelopment = process.env.NODE_ENV !== 'production';

// 로그 포맷 정의
const devFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.colorize({ all: true }),
  winston.format.printf(
    ({ timestamp, level, message, ...meta }) => {
      const metaStr = Object.keys(meta).length ? ` ${JSON.stringify(meta)}` : '';
      return `${timestamp} ${level}: ${message}${metaStr}`;
    }
  )
);

const prodFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.errors({ stack: true }),
  winston.format.json()
);

// 로그 디렉토리 (프로젝트 루트 기준)
const logDir = path.join(process.cwd(), 'logs');

// Transport 설정
const transports: winston.transport[] = [
  // 콘솔 출력 (항상)
  new winston.transports.Console(),
];

// 프로덕션에서만 파일 로깅
if (!isDevelopment) {
  transports.push(
    // 에러 로그 별도 파일
    new winston.transports.File({
      filename: path.join(logDir, 'error.log'),
      level: 'error',
    }),
    // 전체 로그
    new winston.transports.File({
      filename: path.join(logDir, 'combined.log'),
    })
  );
}

// Logger 인스턴스 생성
export const logger = winston.createLogger({
  level: isDevelopment ? 'debug' : 'info',
  levels,
  defaultMeta: process.env.LOG_COMPONENT
    ? { component: process.env.LOG_COMPONENT }
    : undefined,
  format: isDevelopment ? devFormat : prodFormat,
  transports,
  // 예외 처리
  exceptionHandlers: isDevelopment ? undefined : [
    new winston.transports.File({ 
      filename: path.join(logDir, 'exceptions.log') 
    }),
  ],
});

// 편의 메서드: 객체와 함께 로깅
export const log = {
  info: (message: string, meta?: object) => logger.info(message, meta),
  warn: (message: string, meta?: object) => logger.warn(message, meta),
  error: (message: string, meta?: object) => logger.error(message, meta),
  debug: (message: string, meta?: object) => logger.debug(message, meta),
  http: (message: string, meta?: object) => logger.http(message, meta),
};

/**
 * 동일 이미지의 서로 다른 entry point가 process component를 명시한다.
 * Docker의 LOG_COMPONENT가 없어도 이후 module 로그에 process 경계를 남긴다.
 */
export function setLogComponent(component: string): void {
  logger.defaultMeta = {
    ...logger.defaultMeta,
    component,
  };
}

/**
 * Error 객체를 nested metadata로 그대로 넘길 때 JSON에서 빈 객체가 되는
 * 문제를 피하면서 원래 오류 메시지는 보존한다.
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  return 'Unknown error';
}
