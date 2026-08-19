# Market Data Architecture Lab

`BTC/USD`의 24/7 market feed를 이용해 process isolation, local durability,
failure recovery, SPOF와 state ownership을 실험하는 TypeScript 프로젝트입니다.
현재 runtime workload는 BTC/USD 하나지만, comma-separated configuration과
symbol-aware persistence 구조는 여러 symbol을 지원합니다.

BTC/USD는 거래일·장 마감 없이 지속되어 장애 시점을 만들고 recovery 결과를
관찰하기 쉬운 deterministic workload입니다. Market Ingestor와 Chart Server를
분리해 serving 장애가 ingestion durability로 전파되지 않도록 했습니다.

## Runtime architecture

```text
TwelveData WebSocket
        │
        ▼
Market Ingestor ── best effort ──▶ Redis Pub/Sub
        │                               │
        ▼                               ▼
   CandleMaker                     Chart Server
        │                               │
        ▼                               ▼
RocksDB Pending Store                 WS Clients
        │
        ▼
 CandleFlusher
        │
        ▼
  TimescaleDB
```

완성된 candle은 Redis publish보다 먼저 RocksDB에 기록됩니다. CandleFlusher는
TimescaleDB의 idempotent insert가 성공한 뒤에만 RocksDB record를 ACK합니다.

- RocksDB: Market Ingestor가 소유하는 local durable pending store
- TimescaleDB: symbol-aware canonical historical time-series store
- Redis Pub/Sub: 유실을 허용하는 ephemeral realtime fan-out
- Chart Server: historical candle REST와 client WebSocket serving

## 기본 workload

```env
STREAM_SYMBOLS=BTC/USD
ENABLE_HISTORICAL_BACKFILL=false
```

`STREAM_SYMBOLS=BTC/USD,ETH/USD`처럼 변경하면 코드 수정 없이 여러 symbol을
구독할 수 있습니다. 빈 symbol 설정은 startup 시 거부됩니다. Historical
backfill은 Ingestor downtime 동안 수신하지 못한 1분봉을 복구하는 선택 기능이며,
architecture experiment의 기본값은 OFF입니다.

## 실행

```bash
npm install
cp .env.example .env
docker compose up -d timescaledb redis
npm run migrate:deploy
docker compose up -d --build market-ingestor chart-server
docker compose logs -f market-ingestor
```

서비스 이름은 `timescaledb`, `redis`, `market-ingestor`, `chart-server`입니다.
실제 `TWELVE_DATA_API_KEY`는 실행 환경에서 주입하며 로그에 출력하지 않습니다.

개별 개발 실행:

```bash
npm run start:ingestor
npm run start:server
```

## HTTP / WebSocket

- `GET /`: Chart Server liveness
- `GET /health`: database readiness와 Redis subscriber degraded 상태
- `GET /api/candles/:symbol/:timeframe`: candle 조회
- `POST /api/aggregate/refresh`: TimescaleDB Continuous Aggregate refresh
- `ws://localhost:8080/ws`: client market-event subscription

REST path의 `BTC/USD`는 `BTC%2FUSD`로 인코딩합니다. WebSocket에서는 그대로
사용합니다.

```json
{ "type": "subscribe", "symbols": ["BTC/USD"] }
```

## 검증

```bash
npm run typecheck
npm run build
npm test
docker compose config --quiet
```

## 의도적으로 남겨둔 한계

- Market Ingestor는 single instance이며 SPOF입니다.
- Leader Election과 multi-instance ownership coordination이 없습니다.
- RocksDB는 Market Ingestor의 local durable state이므로 failover ownership 문제가
  남아 있습니다.
- Kafka/Redpanda 같은 distributed durable log를 사용하지 않습니다.

이 한계들은 후속 장애 실험에서 직접 관찰하기 위해 의도적으로 유지합니다.

## Future experiments

- Market Ingestor SIGKILL과 ingestion SPOF 관찰
- 두 Ingestor의 duplicate subscription 및 RocksDB ownership 충돌 관찰
- Leader Election/ownership 모델 비교
- local pending store와 distributed durable log의 차이 비교

위 항목은 현재 구현된 기능이 아닙니다.

## 관련 문서

- [API](docs/API_DOCUMENTATION.md)
- [Docker](docs/DOCKER.md)
- [DB 설정](docs/DB_SETUP.md)
