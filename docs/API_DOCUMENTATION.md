# Architecture Lab API

이 문서는 Chart Server 코드에 실제 등록된 REST API를 기준으로 정리한 문서입니다.

## 기본 정보

- 기본 REST 접두사: `/api`
- WebSocket 엔드포인트: `/ws`
- 상태 검사: `GET /health`

## 상태 확인

| 메서드 | 경로 | 설명 |
| --- | --- | --- |
| `GET` | `/` | 서버 프로세스 상태 확인 |
| `GET` | `/health` | DB 연결을 포함한 준비 상태 확인 |

## 캔들 API

기본 경로: `/api/candles`

### 캔들 데이터 조회

```http
GET /api/candles/:symbol/:timeframe
```

| 항목 | 설명 |
| --- | --- |
| `symbol` | 1~20자의 영문자, 숫자, `.`, `-`, `_`, `/`. 예: `BTC/USD`, `ETH/USD` |
| `timeframe` | `1m`, `5m`, `15m`, `1h`, `4h`, `1D`, `1W`, `1M` |
| `limit` | 선택 쿼리. `1~5000` 범위의 정수, 기본값 `1000` |
| `from` | 선택 쿼리. Unix 초 단위 시각 또는 날짜 문자열 |
| `to` | 선택 쿼리. Unix 초 단위 시각 또는 날짜 문자열 |

`from`과 `to`를 함께 전달하면 `from <= to`여야 합니다.
`/`가 포함된 종목은 경로 구간 충돌을 피하도록 URL 인코딩해서 전달합니다.

```http
GET /api/candles/BTC%2FUSD/1m
```

서버에서는 이를 `BTC/USD`로 처리합니다. `/api/candles/BTC/USD/1m`처럼 슬래시를 그대로 보내면 경로 구간이 분리되어 404 응답이 발생할 수 있습니다.

응답 예시:

```json
{
  "symbol": "BTC/USD",
  "timeframe": "15m",
  "data": [
    {
      "time": 1731400000,
      "open": 100,
      "high": 103,
      "low": 99,
      "close": 101,
      "volume": 9999
    }
  ]
}
```

## 집계 API

기본 경로: `/api/aggregate`

### Continuous Aggregate 수동 새로고침

```http
POST /api/aggregate/refresh
```

1분봉 데이터를 백필한 뒤 TimescaleDB Continuous Aggregate View에 즉시 반영하고 싶을 때 사용합니다.
`timeframe`은 `5m`, `15m`, `1h`, `4h`, `1D`, `1W`, `1M` 중 하나여야 하며, `from <= to`여야 합니다.

요청 예시:

```json
{
  "timeframe": "5m",
  "from": 1638316800,
  "to": 1638403200
}
```

응답 예시:

```json
{
  "success": true,
  "timeframe": "5m",
  "message": "Continuous Aggregate 뷰가 새로고침되었습니다."
}
```

### 입력값 검증 오류

Chart Server의 실패 응답은 HTTP 상태와 함께 다음 공통 형식으로 전달됩니다.

```json
{
  "success": false,
  "errorCode": "INVALID_TIMEFRAME",
  "message": "Unsupported timeframe."
}
```

## WebSocket 프로토콜

```text
ws://<host>:<port>/ws
```

연결 직후 기본 구독은 없으며 서버가 다음 메시지를 전송합니다.

```json
{
  "type": "welcome",
  "message": "Connected to Chart WebSocket.",
  "subscriptionRequired": true
}
```

클라이언트는 종목을 명시적으로 구독하거나 해제해야 합니다.

```json
{ "type": "subscribe", "symbols": ["BTC/USD", "ETH/USD"] }
```

```json
{ "type": "unsubscribe", "symbols": ["ETH/USD"] }
```

서버는 각각 `subscribed`, `unsubscribed` 메시지로 처리된 종목을 응답합니다. 구독하지 않은 종목의 `tick`과 `candle`은 전송하지 않습니다.

```json
{ "type": "tick", "symbol": "BTC/USD", "price": 100.12, "timestamp": 1731400000 }
```

```json
{
  "type": "candle",
  "timeframe": "1m",
  "candle": {
    "symbol": "BTC/USD",
    "startTime": 1731400000,
    "open": 100,
    "high": 101,
    "low": 99,
    "close": 100.5,
    "volume": 0
  }
}
```

잘못된 메시지는 연결을 유지한 채 오류로 응답합니다.

```json
{
  "type": "error",
  "errorCode": "INVALID_WS_MESSAGE",
  "message": "Invalid WebSocket message."
}
```

`BTC/USD`는 WebSocket JSON에서는 그대로 사용합니다. URL 인코딩한 `BTC%2FUSD`가 필요한 곳은 REST 경로 매개변수뿐입니다. 현재 필터링은 Chart Server 클라이언트별 구독에 적용되며 TwelveData 상류 종목 구독은 정적 설정을 유지합니다.

## 관련 문서

- [Chart Server README](../README.md)
- [DB 설정](DB_SETUP.md)
- [Docker](DOCKER.md)
