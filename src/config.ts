import "dotenv/config";

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

const ENV = process.env;
const DEFAULT_SUPPORTED_ASSETS = ["btc", "eth", "sol", "xrp"] satisfies CryptoSymbol[];
const DEFAULT_SUPPORTED_WINDOWS = ["5m", "15m"] satisfies CryptoMarketWindow[];

const config = {
  RESPONSE_CONTENT_TYPE: ENV.RESPONSE_CONTENT_TYPE || "application/json",
  HTTP_HOST: ENV.HTTP_HOST || "0.0.0.0",
  HTTP_PORT: Number(ENV.HTTP_PORT || ENV.PORT || 3000),
  SERVICE_NAME: ENV.SERVICE_NAME || "@sha3/polymarket-snapshot-collector",
  CLICKHOUSE_URL: ENV.CLICKHOUSE_URL || "http://localhost:8123",
  CLICKHOUSE_USER: ENV.CLICKHOUSE_USER || ENV.CLICKHOUSE_USERNAME || "default",
  CLICKHOUSE_PASSWORD: ENV.CLICKHOUSE_PASSWORD || "default",
  CLICKHOUSE_DATABASE: ENV.CLICKHOUSE_DATABASE || "default",
  CLICKHOUSE_MARKET_TABLE: ENV.CLICKHOUSE_MARKET_TABLE || "market",
  CLICKHOUSE_SNAPSHOT_TABLE: ENV.CLICKHOUSE_SNAPSHOT_TABLE || "snapshot",
  SUPPORTED_ASSETS:
    (ENV.SUPPORTED_ASSETS?.split(",")
      .map((asset) => asset.trim())
      .filter(Boolean) as CryptoSymbol[] | undefined) || DEFAULT_SUPPORTED_ASSETS,
  SUPPORTED_WINDOWS:
    (ENV.SUPPORTED_WINDOWS?.split(",")
      .map((window) => window.trim())
      .filter(Boolean) as CryptoMarketWindow[] | undefined) || DEFAULT_SUPPORTED_WINDOWS,
  SNAPSHOT_INTERVAL_MS: Number(ENV.SNAPSHOT_INTERVAL_MS || 500),
  SNAPSHOT_BATCH_SIZE: Number(ENV.SNAPSHOT_BATCH_SIZE || ENV.SNAPSHOT_INSERT_BATCH_MAX_SIZE || 512),
  SNAPSHOT_FLUSH_INTERVAL_MS: Number(ENV.SNAPSHOT_FLUSH_INTERVAL_MS || 1000),
} as const;

export default config;
