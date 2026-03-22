# @sha3/polymarket-snapshot-collector

Internal HTTP service that subscribes to `@sha3/polymarket-snapshot`, persists each flat snapshot into ClickHouse, keeps a lightweight market catalog, and exposes historical snapshot reads by date range.

## TL;DR

```bash
npm install
npm run check
npm run start
```

```bash
curl "http://localhost:3000/snapshots?fromDate=2026-03-11T10:00:00.000Z&toDate=2026-03-11T10:15:00.000Z&marketSlug=btc-5m-example"
```

## Why

Use this service when you want one collector process that:

- receives the full upstream flat snapshot contract,
- stores it as one ClickHouse row per instant,
- keeps a small market catalog for discovery,
- and exposes one simple range API for historical reads.

## Main Capabilities

- Creates the `market` and `snapshot` tables on startup when they do not exist.
- Subscribes to `@sha3/polymarket-snapshot` and batch-inserts flat snapshots into `snapshot`.
- Stores one catalog row per discovered live market slug in `market`.
- Exposes `GET /markets` for market discovery.
- Exposes `GET /snapshots` for historical range reads, optionally filtered by `marketSlug`.

## Installation

```bash
npm install
```

## Setup

Create a `.env` file when you need non-default runtime values:

```dotenv
HTTP_PORT=3000
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=default
CLICKHOUSE_DATABASE=default
```

## Running Locally

```bash
npm run start
```

Default bind:

- `http://0.0.0.0:3000`

## Usage

Start the runtime:

```ts
import { ServiceRuntime } from "@sha3/polymarket-snapshot-collector";

const serviceRuntime = ServiceRuntime.createDefault();
await serviceRuntime.startServer();
```

## Examples

Build the HTTP server without listening:

```ts
import { ServiceRuntime } from "@sha3/polymarket-snapshot-collector";

const serviceRuntime = ServiceRuntime.createDefault();
const server = serviceRuntime.buildServer();
```

List discovered markets:

```bash
curl "http://localhost:3000/markets?asset=btc&window=5m"
```

Read snapshots for a time range:

```bash
curl "http://localhost:3000/snapshots?fromDate=2026-03-11T10:00:00.000Z&toDate=2026-03-11T10:15:00.000Z"
```

Read snapshots from the oldest stored row up to a cutoff:

```bash
curl "http://localhost:3000/snapshots?toDate=2026-03-11T10:15:00.000Z"
```

Read only snapshots that include a specific market slug:

```bash
curl "http://localhost:3000/snapshots?fromDate=2026-03-11T10:00:00.000Z&toDate=2026-03-11T10:15:00.000Z&marketSlug=btc-5m-example"
```

## HTTP API

### `GET /`

Returns:

```json
{
  "ok": true,
  "serviceName": "@sha3/polymarket-snapshot-collector"
}
```

### `GET /markets`

Query params:

- `asset`: optional, one of `btc`, `eth`, `sol`, `xrp`
- `window`: optional, one of `5m`, `15m`
- `fromDate`: optional ISO-8601 timestamp

Returns:

```json
{
  "markets": [
    {
      "slug": "btc-5m-example",
      "asset": "btc",
      "window": "5m",
      "priceToBeat": 100123.45,
      "marketStart": "2026-03-11T10:00:00.000Z",
      "marketEnd": "2026-03-11T10:05:00.000Z"
    }
  ]
}
```

### `GET /snapshots`

Query params:

- `fromDate`: optional ISO-8601 timestamp. When omitted, the query starts at the oldest stored snapshot.
- `toDate`: required ISO-8601 timestamp
- `limit`: optional integer, default `1000`, max `5000`
- `marketSlug`: optional non-empty slug string

Returns:

```json
{
  "fromDate": "2026-03-11T10:00:00.000Z",
  "toDate": "2026-03-11T10:15:00.000Z",
  "marketSlug": "btc-5m-example",
  "snapshots": [
    {
      "id": "4a7b2c2b-e2b4-48a2-a7d3-7f59aef8a8ae",
      "generated_at": 1773223200000,
      "btc_binance_price": 100123.45,
      "btc_5m_slug": "btc-5m-example",
      "btc_5m_market_start": "2026-03-11T10:00:00.000Z",
      "btc_5m_market_end": "2026-03-11T10:05:00.000Z",
      "btc_5m_price_to_beat": 100100.0,
      "inserted_at": "2026-03-11T10:00:00.123Z"
    }
  ]
}
```

Behavior notes:

- rows are sorted ascending by `generated_at`
- when `fromDate` is omitted, the lower date bound is not applied and the query starts from the oldest stored row
- when `marketSlug` is present, the service returns only snapshots where any supported pair slug field equals that value
- the response preserves upstream field names exactly

## Public API

### `ServiceRuntime`

Primary runtime entrypoint for composing or starting the service.

```ts
import { ServiceRuntime } from "@sha3/polymarket-snapshot-collector";
```

#### `createDefault()`

Builds the default runtime wiring.

Returns:

- `ServiceRuntime`

#### `buildServer()`

Builds the Node HTTP server without binding a port.

Returns:

- `Server`

#### `startServer()`

Creates schema, starts the collector, and starts listening.

Returns:

- `Promise<Server>`

#### `stop()`

Stops the HTTP server, collector, and ClickHouse client.

Returns:

- `Promise<void>`

### `AppInfoPayload`

```ts
type AppInfoPayload = { ok: true; serviceName: string };
```

### `MarketSummary`

```ts
type MarketSummary = {
  slug: string;
  asset: "btc" | "eth" | "sol" | "xrp";
  window: "5m" | "15m";
  priceToBeat: number | null;
  marketStart: string;
  marketEnd: string;
};
```

### `MarketListPayload`

```ts
type MarketListPayload = { markets: MarketSummary[] };
```

### `StoredSnapshot`

```ts
type StoredSnapshot = {
  id: string;
  generated_at: number;
  inserted_at: string;
} & Record<string, number | string | null>;
```

### `SnapshotRangePayload`

```ts
type SnapshotRangePayload = {
  fromDate: string | null;
  toDate: string;
  marketSlug: string | null;
  snapshots: StoredSnapshot[];
};
```

## Compatibility

- Node.js 20+
- ESM (`"type": "module"`)
- ClickHouse with `MergeTree` support
- Flat upstream snapshots from `@sha3/polymarket-snapshot@2.2.0`

## Storage Model

### `snapshot`

Primary table for the flat upstream snapshot contract.

- one row per `generated_at`
- stores all upstream fields as explicit columns
- partitioned by `toDate(generated_at)`

### `market`

Lightweight discovery table.

- one row per discovered slug
- stores `slug`, `asset`, `window`, `price_to_beat`, `market_start`, `market_end`

## Configuration

Configuration lives in [src/config.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/config.ts).

- `RESPONSE_CONTENT_TYPE`: HTTP response `content-type` header.
- `HTTP_HOST`: interface used by the HTTP server.
- `HTTP_PORT`: listening port used by `startServer()`.
- `SERVICE_NAME`: name returned by `GET /`.
- `CLICKHOUSE_URL`: ClickHouse server URL.
- `CLICKHOUSE_USER`: ClickHouse username.
- `CLICKHOUSE_PASSWORD`: ClickHouse password.
- `CLICKHOUSE_DATABASE`: ClickHouse database name.
- `CLICKHOUSE_MARKET_TABLE`: table name for the lightweight market catalog.
- `CLICKHOUSE_SNAPSHOT_TABLE`: flat snapshot target table name.
- `SUPPORTED_ASSETS`: comma-separated supported assets list.
- `SUPPORTED_WINDOWS`: comma-separated supported market windows list.
- `SNAPSHOT_INTERVAL_MS`: polling interval passed to `@sha3/polymarket-snapshot`.
- `SNAPSHOT_BATCH_SIZE`: maximum batch size written to ClickHouse in one insert.
- `SNAPSHOT_FLUSH_INTERVAL_MS`: maximum time the collector waits before flushing a partial batch.

## Scripts

- `npm run start`: start the service runtime
- `npm run build`: compile to `dist/`
- `npm run standards:check`: run `code-standards verify`
- `npm run lint`: run Biome checks
- `npm run format:check`: run Biome formatter checks
- `npm run typecheck`: run `tsc --noEmit`
- `npm run test`: run the Node test suite
- `npm run check`: run the full blocking quality gate

## Structure

- [src/config.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/config.ts): canonical runtime configuration
- [src/clickhouse/clickhouse-client.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/clickhouse/clickhouse-client.service.ts): thin ClickHouse wrapper
- [src/clickhouse/clickhouse-schema.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/clickhouse/clickhouse-schema.service.ts): schema creation
- [src/collector/snapshot-collector.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/collector/snapshot-collector.service.ts): snapshot subscription and batching
- [src/market/market-repository.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/market/market-repository.service.ts): lightweight market catalog persistence
- [src/market/market-sync.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/market/market-sync.service.ts): market discovery from incoming snapshots
- [src/snapshot/flat-snapshot-repository.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/snapshot/flat-snapshot-repository.service.ts): flat snapshot persistence and reads
- [src/snapshot/snapshot-query.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/snapshot/snapshot-query.service.ts): query use cases
- [src/http/http-server.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/http/http-server.service.ts): HTTP API
- [src/app/service-runtime.service.ts](/Users/jc/Documents/GitHub/polymarket-snapshot-collector/src/app/service-runtime.service.ts): runtime composition

## Troubleshooting

### ClickHouse connection failures

Verify:

- `CLICKHOUSE_URL`
- `CLICKHOUSE_USER`
- `CLICKHOUSE_PASSWORD`
- `CLICKHOUSE_DATABASE`

### Empty `/markets` response

The catalog is populated only when incoming snapshots include live slug, start, and end fields for a pair.

## AI Workflow

- Read `AGENTS.md`, `ai/contract.json`, and the relevant `ai/<assistant>.md` before editing.
- Keep managed files read-only unless the task explicitly requires a standards update.
- Run `npm run standards:check` and `npm run check` before finishing.
