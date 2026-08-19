/**
 * 환경 변수 설정 (Envalid로 검증)
 * 
 * Fail Fast: 필수 환경 변수가 없거나 잘못되면 서버 시작 시 즉시 에러
 */

import { cleanEnv, str, port, bool, url } from 'envalid';
import dotenv from 'dotenv';
import { DEFAULT_STREAM_SYMBOL, parseStreamSymbols } from './market.config';

dotenv.config();

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

  // Durable candle queue
  ROCKSDB_PATH: env.ROCKSDB_PATH,
} as const;

export default config;
