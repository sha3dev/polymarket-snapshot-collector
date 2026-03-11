/**
 * @section imports:internals
 */

import config from "../config.ts";
import type { ClickhouseClientService } from "./clickhouse-client.service.ts";

const SNAPSHOT_COLUMN_DEFINITIONS = [
  "asset LowCardinality(String)",
  "window LowCardinality(String)",
  "market_slug String",
  "generated_at DateTime64(3, 'UTC')",
  "up_asset_id Nullable(String)",
  "up_price Nullable(Float64)",
  "up_order_book Nullable(String)",
  "up_event_ts Nullable(Int64)",
  "down_asset_id Nullable(String)",
  "down_price Nullable(Float64)",
  "down_order_book Nullable(String)",
  "down_event_ts Nullable(Int64)",
  "binance_price Nullable(Float64)",
  "binance_order_book Nullable(String)",
  "binance_event_ts Nullable(Int64)",
  "coinbase_price Nullable(Float64)",
  "coinbase_order_book Nullable(String)",
  "coinbase_event_ts Nullable(Int64)",
  "kraken_price Nullable(Float64)",
  "kraken_order_book Nullable(String)",
  "kraken_event_ts Nullable(Int64)",
  "okx_price Nullable(Float64)",
  "okx_order_book Nullable(String)",
  "okx_event_ts Nullable(Int64)",
  "chainlink_price Nullable(Float64)",
  "chainlink_order_book Nullable(String)",
  "chainlink_event_ts Nullable(Int64)",
  "inserted_at DateTime64(3, 'UTC')",
].join(",\n        ");

/**
 * @section types
 */

type ClickhouseSchemaServiceOptions = { clickhouseClientService: ClickhouseClientService };

/**
 * @section private:properties
 */

export class ClickhouseSchemaService {
  private readonly clickhouseClientService: ClickhouseClientService;

  /**
   * @section constructor
   */

  public constructor(options: ClickhouseSchemaServiceOptions) {
    this.clickhouseClientService = options.clickhouseClientService;
  }

  /**
   * @section private:methods
   */

  private buildCreateMarketTableQuery(): string {
    const query = `
      CREATE TABLE IF NOT EXISTS ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE} (
        slug String,
        market_id Nullable(String),
        market_condition_id Nullable(String),
        asset LowCardinality(String),
        window LowCardinality(String),
        price_to_beat Nullable(Float64),
        market_start DateTime64(3, 'UTC'),
        market_end DateTime64(3, 'UTC'),
        inserted_at DateTime64(3, 'UTC')
      )
      ENGINE = MergeTree
      ORDER BY (asset, window, market_start, slug)
    `;
    return query;
  }

  private buildCreateSnapshotTableQuery(): string {
    const query = `
      CREATE TABLE IF NOT EXISTS ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE} (
        ${SNAPSHOT_COLUMN_DEFINITIONS}
      )
      ENGINE = MergeTree
      PARTITION BY toDate(generated_at)
      ORDER BY (market_slug, generated_at, asset, window)
    `;
    return query;
  }

  /**
   * @section public:methods
   */

  public async ensureSchema(): Promise<void> {
    await this.clickhouseClientService.command(this.buildCreateMarketTableQuery());
    await this.clickhouseClientService.command(this.buildCreateSnapshotTableQuery());
  }
}
