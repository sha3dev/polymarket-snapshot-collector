import "dotenv/config";

import type { SnapshotAsset, SnapshotWindow } from "@sha3/polymarket-snapshot";

const ENV = process.env;
const DEFAULT_SUPPORTED_ASSETS = ["btc", "eth", "sol", "xrp"] satisfies SnapshotAsset[];
const DEFAULT_SUPPORTED_WINDOWS = ["5m", "15m"] satisfies SnapshotWindow[];

const config = {
  RESPONSE_CONTENT_TYPE: ENV.RESPONSE_CONTENT_TYPE || "application/json",
  HTTP_HOST: ENV.HTTP_HOST || "0.0.0.0",
  DEFAULT_PORT: Number(ENV.PORT || 3000),
  SERVICE_NAME: ENV.SERVICE_NAME || "@sha3/polymarket-snapshot-collector",
  CLICKHOUSE_URL: ENV.CLICKHOUSE_URL || "http://localhost:8123",
  CLICKHOUSE_DATABASE: ENV.CLICKHOUSE_DATABASE || "default",
  CLICKHOUSE_USERNAME: ENV.CLICKHOUSE_USERNAME || "default",
  CLICKHOUSE_PASSWORD: ENV.CLICKHOUSE_PASSWORD || "default",
  CLICKHOUSE_MARKET_TABLE: ENV.CLICKHOUSE_MARKET_TABLE || "market",
  CLICKHOUSE_SNAPSHOT_TABLE: ENV.CLICKHOUSE_SNAPSHOT_TABLE || "snapshot",
  SNAPSHOT_INTERVAL_MS: Number(ENV.SNAPSHOT_INTERVAL_MS || 500),
  SNAPSHOT_DEDUPLICATION_TTL_MS: Number(ENV.SNAPSHOT_DEDUPLICATION_TTL_MS || 120000),
  SNAPSHOT_DEDUPLICATION_MAX_KEYS: Number(ENV.SNAPSHOT_DEDUPLICATION_MAX_KEYS || 20000),
  SUPPORTED_ASSETS:
    (ENV.SUPPORTED_ASSETS?.split(",")
      .map((asset) => asset.trim())
      .filter(Boolean) as SnapshotAsset[] | undefined) || DEFAULT_SUPPORTED_ASSETS,
  SUPPORTED_WINDOWS:
    (ENV.SUPPORTED_WINDOWS?.split(",")
      .map((window) => window.trim())
      .filter(Boolean) as SnapshotWindow[] | undefined) || DEFAULT_SUPPORTED_WINDOWS,
} as const;

export default config;
