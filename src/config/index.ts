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

  TWELVE_DATA_WS_URL: str({
    default: 'wss://ws.twelvedata.com/v1/quotes/price',
  }),

  STREAM_SYMBOLS: str({ default: DEFAULT_STREAM_SYMBOL }),
  ENABLE_HISTORICAL_BACKFILL: bool({ default: false }),

  LEADER_ELECTION_LOCK_KEY: num({ default: 424242 }),
  LEADER_ELECTION_RETRY_MS: num({ default: 1000 }),

  ROCKSDB_PATH: str({ default: './data/rocksdb/candles' }),

  KAFKA_BROKERS: str({ default: 'localhost:9092' }),

  KAFKA_RAW_TICKS_TOPIC: str({ default: 'market.raw-ticks' }),

  KAFKA_CLIENT_ID: str({ default: 'market-feed-ingestor' }),

  KAFKA_CANDLE_CONSUMER_GROUP: str({
    default: 'candle-processor-v1',
  }),

  KAFKA_CANDLE_CONSUMER_CLIENT_ID: str({
    default: 'candle-processor',
  }),

  CANDLE_PROCESSOR_SYMBOLS: str({
    default: DEFAULT_STREAM_SYMBOL,
  }),
});

const kafkaBrokers = env.KAFKA_BROKERS
  .split(',')
  .map((broker) => broker.trim())
  .filter(Boolean);

if (kafkaBrokers.length === 0) {
  throw new Error('KAFKA_BROKERS must contain at least one broker');
}

const streamSymbols = parseStreamSymbols(env.STREAM_SYMBOLS);
const candleProcessorSymbols = parseStreamSymbols(
  env.CANDLE_PROCESSOR_SYMBOLS,
);

if (!haveSameSymbols(streamSymbols, candleProcessorSymbols)) {
  throw new Error(
    'STREAM_SYMBOLS and CANDLE_PROCESSOR_SYMBOLS must contain the same symbols',
  );
}

const config = {
  port: env.PORT,
  nodeEnv: env.NODE_ENV,
  isDevelopment: env.isDevelopment,
  isProduction: env.isProduction,

  DATABASE_URL: env.DATABASE_URL,

  REDIS_URL: env.REDIS_URL,

  TWELVE_DATA_API_KEY: env.TWELVE_DATA_API_KEY,

  TWELVE_DATA_WS_URL: env.TWELVE_DATA_WS_URL,

  market: {
    streamSymbols,
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
    candleConsumerGroup: env.KAFKA_CANDLE_CONSUMER_GROUP,
    candleConsumerClientId: env.KAFKA_CANDLE_CONSUMER_CLIENT_ID,
  },

  candleProcessor: {
    symbols: candleProcessorSymbols,
  },
} as const;

export default config;

function haveSameSymbols(
  first: readonly string[],
  second: readonly string[],
): boolean {
  if (first.length !== second.length) {
    return false;
  }

  const firstSymbols = new Set(first);

  return second.every((symbol) => firstSymbols.has(symbol));
}
