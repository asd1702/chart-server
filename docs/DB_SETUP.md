# Architecture Lab Database

TimescaleDB is the canonical historical candle store. RocksDB is not a second
historical database; it is the Market Ingestor's local pending queue until a
candle is accepted by TimescaleDB.

## Schema

The Prisma schema contains one model:

```text
Candle1m
├ time
├ symbol
├ open / high / low / close
└ volume
```

`(time, symbol)` is the primary key used for idempotent at-least-once writes.
The baseline migration also creates the TimescaleDB extension, converts
`Candle1m` to a hypertable, and creates these continuous aggregates:

```text
candle_5m  candle_15m  candle_1h  candle_4h
candle_1d  candle_1w   candle_1mo
```

## Bootstrap a new lab database

```bash
docker compose up -d timescaledb
npm run prisma:generate
npm run migrate:deploy
```

The main Compose file publishes TimescaleDB on `POSTGRES_PORT` for the host-side
Prisma command. No additional SQL setup script is required. The lab uses a new
`timescale-data` Compose volume; an older `find_chart_timescale_data` volume is
not deleted automatically.

## Optional historical backfill

Historical backfill recovers market-data gaps created while the Market
Ingestor was offline. It is disabled by default:

```env
ENABLE_HISTORICAL_BACKFILL=false
```

Set it to `true` only for a gap-recovery experiment. It operates on the
configured `STREAM_SYMBOLS` and uses the same TwelveData REST rate-limit
protection as other optional REST work.

## Local data reset

`db:reset:dev` deletes only `Candle1m` rows and is protected by the existing
development-only safety checks:

```bash
CONFIRM_DB_RESET=YES npm run db:reset:dev
```

It is blocked for production-looking database URLs. Removing a Docker volume is
not part of this script.
