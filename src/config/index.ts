/**
 * 환경 변수 설정 (Envalid로 검증)
 * 
 * Fail Fast: 필수 환경 변수가 없거나 잘못되면 서버 시작 시 즉시 에러
 */

import { cleanEnv, str, port, bool, url, num } from 'envalid';
import dotenv from 'dotenv';
import { DEFAULT_STREAM_SYMBOL, parseStreamSymbols } from './market.config';

// Compose/production 환경 변수와 로컬 .env 로딩을 모두 지원하되,
// dotenv의 promotional tip은 애플리케이션 운영 로그에서 제외한다.
dotenv.config({ quiet: true });

// 환경 변수 검증 및 파싱
const env = cleanEnv(process.env, {
  // Server
  NODE_ENV: str({ 
    choices: ['development', 'production', 'test'], 
    default: 'development' 
  }),
  PORT: port({ default: 8080 }),

  // Database (필수)
  DATABASE_URL: url({ desc: 'PostgreSQL 연결 URL' }),

  // Redis process boundary
  REDIS_URL: str({ default: 'redis://localhost:6379' }),

  // External APIs (Market Ingestor에서 필수)
  TWELVE_DATA_API_KEY: str({ default: '' }),

  // Streaming (선택)
  STREAM_SYMBOLS: str({ default: DEFAULT_STREAM_SYMBOL }),
  ENABLE_HISTORICAL_BACKFILL: bool({ default: false }),

  // Leader Election
  LEADER_ELECTION_LOCK_KEY: num({ default: 424242 }),
  LEADER_ELECTION_RETRY_MS: num({ default: 1000 }),
  // Durable candle queue
  ROCKSDB_PATH: str({ default: './data/rocksdb/candles' }),
});

const config = {
  // Server
  port: env.PORT,
  nodeEnv: env.NODE_ENV,
  isDevelopment: env.isDevelopment,
  isProduction: env.isProduction,

  // Database
  DATABASE_URL: env.DATABASE_URL,

  // Redis
  REDIS_URL: env.REDIS_URL,

  // External APIs
  TWELVE_DATA_API_KEY: env.TWELVE_DATA_API_KEY,

  // Market workload. The runtime default is BTC/USD, while parsing remains
  // generic so operators can configure any number of symbols.
  market: {
    streamSymbols: parseStreamSymbols(env.STREAM_SYMBOLS),
    historicalBackfillEnabled: env.ENABLE_HISTORICAL_BACKFILL,
  },

  leaderElection: {
    lockKey: env.LEADER_ELECTION_LOCK_KEY,
    retryIntervalMs: env.LEADER_ELECTION_RETRY_MS,
  },

  // Durable candle queue
  ROCKSDB_PATH: env.ROCKSDB_PATH,
} as const;

export default config;
