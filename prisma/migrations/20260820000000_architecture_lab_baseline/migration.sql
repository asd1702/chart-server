CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE market."Candle1m" (
  "time" TIMESTAMPTZ NOT NULL,
  "symbol" VARCHAR(20) NOT NULL,
  "open" DOUBLE PRECISION NOT NULL,
  "high" DOUBLE PRECISION NOT NULL,
  "low" DOUBLE PRECISION NOT NULL,
  "close" DOUBLE PRECISION NOT NULL,
  "volume" DOUBLE PRECISION NOT NULL,
  CONSTRAINT "Candle1m_pkey" PRIMARY KEY ("time", "symbol")
);

CREATE INDEX "Candle1m_symbol_time_idx"
  ON market."Candle1m" ("symbol", "time" DESC);

SELECT public.create_hypertable(
  'market."Candle1m"',
  'time',
  chunk_time_interval => INTERVAL '1 day'
);

CREATE MATERIALIZED VIEW market.candle_5m
WITH (timescaledb.continuous) AS
SELECT
  public.time_bucket(INTERVAL '5 minutes', "time") AS bucket,
  "symbol",
  public.first("open", "time") AS open,
  max("high") AS high,
  min("low") AS low,
  public.last("close", "time") AS close,
  sum("volume") AS volume
FROM market."Candle1m"
GROUP BY bucket, "symbol"
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
  'market.candle_5m',
  start_offset => INTERVAL '1 hour',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '5 minutes'
);
CREATE INDEX idx_candle_5m_symbol_bucket
  ON market.candle_5m (symbol, bucket DESC);

CREATE MATERIALIZED VIEW market.candle_15m
WITH (timescaledb.continuous) AS
SELECT
  public.time_bucket(INTERVAL '15 minutes', "time") AS bucket,
  "symbol",
  public.first("open", "time") AS open,
  max("high") AS high,
  min("low") AS low,
  public.last("close", "time") AS close,
  sum("volume") AS volume
FROM market."Candle1m"
GROUP BY bucket, "symbol"
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
  'market.candle_15m',
  start_offset => INTERVAL '2 hours',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '15 minutes'
);
CREATE INDEX idx_candle_15m_symbol_bucket
  ON market.candle_15m (symbol, bucket DESC);

CREATE MATERIALIZED VIEW market.candle_1h
WITH (timescaledb.continuous) AS
SELECT
  public.time_bucket(INTERVAL '1 hour', "time") AS bucket,
  "symbol",
  public.first("open", "time") AS open,
  max("high") AS high,
  min("low") AS low,
  public.last("close", "time") AS close,
  sum("volume") AS volume
FROM market."Candle1m"
GROUP BY bucket, "symbol"
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
  'market.candle_1h',
  start_offset => INTERVAL '4 hours',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 hour'
);
CREATE INDEX idx_candle_1h_symbol_bucket
  ON market.candle_1h (symbol, bucket DESC);

CREATE MATERIALIZED VIEW market.candle_4h
WITH (timescaledb.continuous) AS
SELECT
  public.time_bucket(INTERVAL '4 hours', "time") AS bucket,
  "symbol",
  public.first("open", "time") AS open,
  max("high") AS high,
  min("low") AS low,
  public.last("close", "time") AS close,
  sum("volume") AS volume
FROM market."Candle1m"
GROUP BY bucket, "symbol"
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
  'market.candle_4h',
  start_offset => INTERVAL '12 hours',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '4 hours'
);
CREATE INDEX idx_candle_4h_symbol_bucket
  ON market.candle_4h (symbol, bucket DESC);

CREATE MATERIALIZED VIEW market.candle_1d
WITH (timescaledb.continuous) AS
SELECT
  public.time_bucket(INTERVAL '1 day', "time") AS bucket,
  "symbol",
  public.first("open", "time") AS open,
  max("high") AS high,
  min("low") AS low,
  public.last("close", "time") AS close,
  sum("volume") AS volume
FROM market."Candle1m"
GROUP BY bucket, "symbol"
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
  'market.candle_1d',
  start_offset => INTERVAL '3 days',
  end_offset => INTERVAL '1 minute',
  schedule_interval => INTERVAL '1 hour'
);
CREATE INDEX idx_candle_1d_symbol_bucket
  ON market.candle_1d (symbol, bucket DESC);

CREATE MATERIALIZED VIEW market.candle_1w
WITH (timescaledb.continuous) AS
SELECT
  public.time_bucket(INTERVAL '1 week', "time") AS bucket,
  "symbol",
  public.first("open", "time") AS open,
  max("high") AS high,
  min("low") AS low,
  public.last("close", "time") AS close,
  sum("volume") AS volume
FROM market."Candle1m"
GROUP BY bucket, "symbol"
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
  'market.candle_1w',
  start_offset => INTERVAL '4 weeks',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 day'
);
CREATE INDEX idx_candle_1w_symbol_bucket
  ON market.candle_1w (symbol, bucket DESC);

CREATE MATERIALIZED VIEW market.candle_1mo
WITH (timescaledb.continuous) AS
SELECT
  public.time_bucket(INTERVAL '1 month', "time") AS bucket,
  "symbol",
  public.first("open", "time") AS open,
  max("high") AS high,
  min("low") AS low,
  public.last("close", "time") AS close,
  sum("volume") AS volume
FROM market."Candle1m"
GROUP BY bucket, "symbol"
WITH NO DATA;

SELECT public.add_continuous_aggregate_policy(
  'market.candle_1mo',
  start_offset => INTERVAL '3 months',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 week'
);
CREATE INDEX idx_candle_1mo_symbol_bucket
  ON market.candle_1mo (symbol, bucket DESC);
