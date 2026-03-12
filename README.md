# @sha3/polymarket-snapshot-collector

Internal service that subscribes to `@sha3/polymarket-snapshot`, stores full snapshots in ClickHouse, and exposes a small HTTP API for market discovery, historical snapshot retrieval, and current in-memory state.

## TL;DR

```bash
npm install
npm run check
npm run start
```

```bash
curl "http://localhost:3000/markets?asset=btc&window=5m"
```

```bash
curl "http://localhost:3000/markets"
```

## Why

- Keep Polymarket snapshot persistence in one process with one storage model.
- Keep market metadata in `market` and store only time-varying snapshot fields in `snapshot`.
- Expose a simple internal API for market lists, stored snapshot playback, and current in-memory state.

## Main Capabilities

- Creates `market` and `snapshot` tables on startup when they do not exist.
- Subscribes internally to the configured asset and window pairs.
- Prevents duplicate writes for the same canonical snapshot identity with a short in-memory deduplication cache.
- Warns and skips conflicting duplicate payloads when the same canonical identity is received again with different data.
- Stores stable market metadata once in `market` and avoids repeating it in every snapshot row.
- Exposes HTTP endpoints for market listing, snapshot retrieval, and current in-memory state.

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

- `asset`: optional, one of `btc`, `eth`, `sol`, `xrp`
- `window`: optional, one of `5m`, `15m`
- `fromDate`: optional ISO timestamp

Example:

```bash
curl "http://192.168.1.10:3000/markets?asset=btc&window=5m&fromDate=2026-03-11T10:00:00.000Z"
```

```bash
curl "http://192.168.1.10:3000/markets?asset=btc"
```

```bash
curl "http://192.168.1.10:3000/markets"
```

Behavior notes:

- when no filters are provided, the endpoint returns all stored markets
- when only `asset` is provided, only that asset is returned across all windows
- when only `window` is provided, only that window is returned across all assets
- when both are provided, both filters are applied

### `GET /markets/:slug/snapshots`

Example:

```bash
curl "http://192.168.1.10:3000/markets/btc-5m-example/snapshots"
```

Behavior notes:

- rows are returned in ascending `generatedAt`
- `5m` markets may contain at most `600` snapshots
- `15m` markets may contain at most `1800` snapshots

### `GET /state`

Returns the current in-memory state for all supported `asset/window` pairs.

Behavior notes:

- memory-backed, not reconstructed from ClickHouse
- always returns exactly `8` entries in `markets`
- entry order is stable: `btc/5m`, `btc/15m`, `eth/5m`, `eth/15m`, `sol/5m`, `sol/15m`, `xrp/5m`, `xrp/15m`
- entries with no active market still exist with `market: null` and `latestSnapshot: null`

Example:

```bash
curl "http://192.168.1.10:3000/state"
```

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

### `StatePayload`

```ts
type StatePayload = {
  generatedAt: string;
  markets: Array<{
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
    marketDirection: "UP" | "DOWN" | "UNKNOWN";
    latestSnapshotAgeMs: number | null;
    isStale: boolean;
  }>;
};
```

Field notes:

- `generatedAt`: server-side timestamp for the state payload
- `markets`: stable list of one entry per supported `asset/window`
- `snapshotCount`: number of snapshots seen for the currently active market in that entry
- `latestSnapshot`: latest successfully ingested snapshot summary for that entry
- `marketDirection`: derived from `chainlinkPrice` versus `priceToBeat`
- `latestSnapshotAgeMs`: age of the latest snapshot when the payload was produced
- `isStale`: whether the entry has no latest snapshot or the latest snapshot age exceeds the configured stale threshold

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
- `config.STATE_STALE_AFTER_MS`: age threshold used to mark state entries as stale. Default `1000` ms.
- `config.SNAPSHOT_INSERT_BATCH_MAX_SIZE`: maximum number of snapshot rows grouped into one ClickHouse insert. Default `512`.
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
- `src/snapshot/`: snapshot persistence, query logic, deduplication, in-memory state store, and exported payload types
- `test/`: deterministic node:test coverage

## Troubleshooting

### ClickHouse connection failures

Verify `CLICKHOUSE_URL`, `CLICKHOUSE_USERNAME`, and `CLICKHOUSE_PASSWORD`. To use the LAN instance set `CLICKHOUSE_URL=http://192.168.1.2:8123`.

### API is not reachable from another machine

Verify that `HTTP_HOST` is still `0.0.0.0` and that the machine firewall allows inbound traffic on the configured port.

### Duplicate snapshot writes

The canonical identity is `market_slug + asset + window + generated_at`. The service prevents duplicate writes in the ingestion path with a short in-memory deduplication cache and skips conflicting duplicate payloads with a warning.

ClickHouse `MergeTree` ordering keys are not unique constraints, so the storage engine does not enforce row uniqueness by itself. If duplicate rows already exist because data was inserted outside this service or before deduplication was in place, clean that data from ClickHouse before relying on the API responses.

### Snapshot count consistency failures

`5m` markets must stay at or below `600` rows and `15m` markets must stay at or below `1800` rows.

## AI Workflow

- Read `AGENTS.md`, `ai/contract.json`, and the assistant adapter before editing code.
- Keep managed files read-only unless the task is a standards update.
- Run `npm run standards:check` and `npm run check` before finishing.
