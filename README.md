# @sha3/polymarket-snapshot-collector

Internal service that subscribes to `@sha3/polymarket-snapshot`, stores full snapshots in ClickHouse, and exposes a small HTTP API for market discovery and snapshot retrieval.

## TL;DR

```bash
npm install
npm run check
npm run start
```

```bash
curl "http://localhost:3000/markets?asset=btc&window=5m"
```

## Why

- Keep Polymarket snapshot persistence in one process with one storage model.
- Keep market metadata in `market` and store only time-varying snapshot fields in `snapshot`.
- Expose a simple internal API for market lists and stored snapshot playback.

## Main Capabilities

- Creates `market` and `snapshot` tables on startup when they do not exist.
- Subscribes internally to the configured asset and window pairs.
- Prevents duplicate writes for the same canonical snapshot identity with a short in-memory deduplication cache.
- Fails fast when the same canonical identity is received again with a different payload.
- Stores stable market metadata once in `market` and avoids repeating it in every snapshot row.
- Exposes HTTP endpoints for market listing and snapshot retrieval.

## Installation

```bash
npm install
```

## Running Locally

```bash
npm run start
```

Defaults:

- HTTP bind: `0.0.0.0:3000`
- ClickHouse: `http://localhost:8123`
- ClickHouse user: `default`
- ClickHouse password: `default`

Because the service binds to `0.0.0.0`, it can be called from other machines in the same internal network by using the host machine IP address.

Use the LAN ClickHouse instance when needed:

```bash
CLICKHOUSE_URL=http://192.168.1.2:8123 npm run start
```

## Usage

```ts
import { ServiceRuntime } from "@sha3/polymarket-snapshot-collector";

const serviceRuntime = ServiceRuntime.createDefault();
await serviceRuntime.startServer();
```

## Examples

Build the server without binding:

```ts
import { ServiceRuntime } from "@sha3/polymarket-snapshot-collector";

const serviceRuntime = ServiceRuntime.createDefault();
const server = serviceRuntime.buildServer();
```

Stop the runtime cleanly:

```ts
import { ServiceRuntime } from "@sha3/polymarket-snapshot-collector";

const serviceRuntime = ServiceRuntime.createDefault();
await serviceRuntime.startServer();
await serviceRuntime.stop();
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

- `asset`: required, one of `btc`, `eth`, `sol`, `xrp`
- `window`: required, one of `5m`, `15m`
- `fromDate`: optional ISO timestamp

Example:

```bash
curl "http://192.168.1.10:3000/markets?asset=btc&window=5m&fromDate=2026-03-11T10:00:00.000Z"
```

### `GET /markets/:slug/snapshots`

Example:

```bash
curl "http://192.168.1.10:3000/markets/btc-5m-example/snapshots"
```

Behavior notes:

- rows are returned in ascending `generatedAt`
- `5m` markets may contain at most `600` snapshots
- `15m` markets may contain at most `1800` snapshots

### `GET /dashboard`

Returns an internal HTML dashboard with one widget per `asset/window` pair.

Each widget shows:

- active market slug
- `priceToBeat`
- `upPrice`
- `downPrice`
- `chainlinkPrice`
- `binancePrice`
- `coinbasePrice`
- `krakenPrice`
- `okxPrice`
- last snapshot age

Example:

```bash
open "http://192.168.1.10:3000/dashboard"
```

### `GET /dashboard/state`

Returns the JSON payload that powers the dashboard.

## Public API

### `ServiceRuntime`

#### `createDefault()`

Builds the default runtime wiring.

Returns:

- `ServiceRuntime`

#### `buildServer()`

Builds the Hono-based Node HTTP server without calling `listen()`.

Returns:

- `Server`

#### `startServer()`

Creates schema, starts the collector, and starts listening on the configured host and port.

Returns:

- `Promise<Server>`

#### `stop()`

Stops the HTTP server, disconnects the collector runtime, and closes the ClickHouse client.

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
  window: "5m" | "15m";
  asset: "btc" | "eth" | "sol" | "xrp";
  priceToBeat: number | null;
  marketStart: string;
  marketEnd: string;
};
```

### `MarketListPayload`

```ts
type MarketListPayload = { markets: MarketSummary[] };
```

### `MarketSnapshotsPayload`

```ts
type MarketSnapshotsPayload = {
  slug: string;
  asset: "btc" | "eth" | "sol" | "xrp";
  window: "5m" | "15m";
  marketStart: string;
  marketEnd: string;
  snapshots: Snapshot[];
};
```

### `DashboardPayload`

```ts
type DashboardPayload = {
  generatedAt: string;
  widgets: Array<{
    asset: "btc" | "eth" | "sol" | "xrp";
    window: "5m" | "15m";
    market: MarketSummary | null;
    snapshotCount: number;
    latestSnapshot: {
      generatedAt: number;
      priceToBeat: number | null;
      upPrice: number | null;
      downPrice: number | null;
      chainlinkPrice: number | null;
      binancePrice: number | null;
      coinbasePrice: number | null;
      krakenPrice: number | null;
      okxPrice: number | null;
    } | null;
    latestSnapshotAgeMs: number | null;
    isStale: boolean;
  }>;
};
```

## Compatibility

- Node.js 20+
- ESM
- TypeScript with relative `.ts` imports enabled

## Configuration

- `config.RESPONSE_CONTENT_TYPE`: response `content-type` header.
- `config.HTTP_HOST`: bind host for `startServer()`. Default is `0.0.0.0` so the API is reachable from other internal hosts.
- `config.DEFAULT_PORT`: bind port for `startServer()`.
- `config.SERVICE_NAME`: service name returned by `GET /`.
- `config.CLICKHOUSE_URL`: ClickHouse base URL.
- `config.CLICKHOUSE_DATABASE`: ClickHouse database name.
- `config.CLICKHOUSE_USERNAME`: ClickHouse username.
- `config.CLICKHOUSE_PASSWORD`: ClickHouse password.
- `config.CLICKHOUSE_MARKET_TABLE`: market table name.
- `config.CLICKHOUSE_SNAPSHOT_TABLE`: snapshot table name.
- `config.SNAPSHOT_INTERVAL_MS`: polling interval passed to `@sha3/polymarket-snapshot`.
- `config.DASHBOARD_STALE_AFTER_MS`: age threshold used to paint dashboard widgets as stale. Default `1000` ms.
- `config.ENABLE_PERF_LOGS`: enables per-snapshot persistence timing logs for the write path.
- `config.SNAPSHOT_INSERT_BATCH_MAX_SIZE`: maximum number of snapshot rows grouped into one ClickHouse insert.
- `config.SNAPSHOT_INSERT_BATCH_MAX_WAIT_MS`: maximum wait before a partial snapshot batch is flushed to ClickHouse.
- `config.SUPPORTED_ASSETS`: subscribed assets.
- `config.SUPPORTED_WINDOWS`: subscribed windows.
- `config.SNAPSHOT_DEDUPLICATION_TTL_MS`: duplicate cache TTL in milliseconds.
- `config.SNAPSHOT_DEDUPLICATION_MAX_KEYS`: duplicate cache max size.

## Scripts

- `npm run standards:check`
- `npm run lint`
- `npm run format:check`
- `npm run typecheck`
- `npm run test`
- `npm run check`
- `npm run start`

## Structure

- `src/app/service-runtime.service.ts`: runtime orchestration
- `src/clickhouse/`: ClickHouse client and schema services
- `src/collector/`: snapshot collector runtime
- `src/http/`: HTTP server and request error mapping
- `src/market/`: market persistence repository
- `src/snapshot/`: snapshot persistence, query logic, deduplication, and exported payload types
- `test/`: deterministic node:test coverage

## Troubleshooting

### ClickHouse connection failures

Verify `CLICKHOUSE_URL`, `CLICKHOUSE_USERNAME`, and `CLICKHOUSE_PASSWORD`. To use the LAN instance set `CLICKHOUSE_URL=http://192.168.1.2:8123`.

### API is not reachable from another machine

Verify that `HTTP_HOST` is still `0.0.0.0` and that the machine firewall allows inbound traffic on the configured port.

### Duplicate snapshot writes

The canonical identity is `market_slug + asset + window + generated_at`. The service prevents duplicate writes in the ingestion path with a short in-memory deduplication cache.

ClickHouse `MergeTree` ordering keys are not unique constraints, so the storage engine does not enforce row uniqueness by itself. If duplicate rows already exist because data was inserted outside this service or before deduplication was in place, clean that data from ClickHouse before relying on the API responses.

### Snapshot count consistency failures

`5m` markets must stay at or below `600` rows and `15m` markets must stay at or below `1800` rows.

## AI Workflow

- Read `AGENTS.md`, `ai/contract.json`, and the assistant adapter before editing code.
- Keep managed files read-only unless the task is a standards update.
- Run `npm run standards:check` and `npm run check` before finishing.
