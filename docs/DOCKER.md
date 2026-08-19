# Architecture Lab Docker

The main Compose file is the single local workflow and defines four services:

```text
timescaledb
redis
market-ingestor
chart-server
```

The application image uses `node:22-bookworm-slim` in all Dockerfile stages.
The build stage generates Prisma Client and compiles TypeScript; the runtime
stage contains production dependencies, Prisma Client, migrations, and current
`dist/` output only.

## Start

```bash
docker compose build
docker compose up -d
npm run migrate:deploy
docker compose ps
```

The default image is `market-data-architecture-lab:local`.

## Runtime ownership

- `market-ingestor` receives `TWELVE_DATA_API_KEY`, `STREAM_SYMBOLS`, the
  historical-backfill flag, and the RocksDB volume.
- `chart-server` has no TwelveData key and no RocksDB access.
- both processes use Redis because Pub/Sub is their process boundary.
- both processes connect to TimescaleDB; only the Ingestor writes live candles.
- the default stream workload is `BTC/USD`.

The API key is injected at runtime and must remain a placeholder in checked-in
examples. It is never part of the image.

## Status and logs

```bash
docker compose ps
docker compose logs -f market-ingestor
curl http://localhost:8080/health
```

Expected startup configuration:

```json
{
  "event": "ingestor_configuration",
  "streamSymbols": ["BTC/USD"],
  "historicalBackfillEnabled": false
}
```

The Compose volumes intentionally persist TimescaleDB, Redis AOF, and RocksDB
state across normal `docker compose down`. Failure experiments should manage
those volumes explicitly and must not treat them as interchangeable state.
