import { cleanEnv, str, port, bool, url, num } from 'envalid';
import dotenv from 'dotenv';
import { DEFAULT_STREAM_SYMBOL, parseStreamSymbols } from './market.config';

dotenv.config({ quiet: true });

const env = cleanEnv(process.env, {
  NODE_ENV: str({ 
    choices: ['development', 'production', 'test'], 
    default: 'development' 
  }),
  PORT: port({ default: 8080 }),

  DATABASE_URL: url({ desc: 'PostgreSQL 연결 URL' }),

  REDIS_URL: str({ default: 'redis://localhost:6379' }),

  TWELVE_DATA_API_KEY: str({ default: '' }),

  STREAM_SYMBOLS: str({ default: DEFAULT_STREAM_SYMBOL }),
  ENABLE_HISTORICAL_BACKFILL: bool({ default: false }),

  LEADER_ELECTION_LOCK_KEY: num({ default: 424242 }),
  LEADER_ELECTION_RETRY_MS: num({ default: 1000 }),

  ROCKSDB_PATH: str({ default: './data/rocksdb/candles' }),

  KAFKA_BROKERS: str({ default: 'localhost:9092' }),

  KAFKA_RAW_TICKS_TOPIC: str({ default: 'market.raw-ticks' }),

  KAFKA_CLIENT_ID: str({ default: 'market-feed-ingestor' }),
});

const kafkaBrokers = env.KAFKA_BROKERS
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);

if (kafkaBrokers.length === 0) {
  throw new Error('KAFKA_BROKERS must contain at least one broker');
}

const config = {
  port: env.PORT,
  nodeEnv: env.NODE_ENV,
  isDevelopment: env.isDevelopment,
  isProduction: env.isProduction,

  DATABASE_URL: env.DATABASE_URL,

  REDIS_URL: env.REDIS_URL,

  TWELVE_DATA_API_KEY: env.TWELVE_DATA_API_KEY,

  market: {
    streamSymbols: parseStreamSymbols(env.STREAM_SYMBOLS),
    historicalBackfillEnabled: env.ENABLE_HISTORICAL_BACKFILL,
  },

  leaderElection: {
    lockKey: env.LEADER_ELECTION_LOCK_KEY,
    retryIntervalMs: env.LEADER_ELECTION_RETRY_MS,
  },

  ROCKSDB_PATH: env.ROCKSDB_PATH,

  kafka: {
    brokers: kafkaBrokers,
    rawTicksTopic: env.KAFKA_RAW_TICKS_TOPIC,
    clientId: env.KAFKA_CLIENT_ID,
  },
} as const;

export default config;
