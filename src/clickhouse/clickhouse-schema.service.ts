/**
 * @section imports:internals
 */

import config from "../config.ts";
import type { SnapshotFieldCatalogService } from "../snapshot/snapshot-field-catalog.service.ts";
import type { ClickhouseClientService } from "./clickhouse-client.service.ts";

/**
 * @section types
 */

type ClickhouseSchemaServiceOptions = {
  clickhouseClientService: ClickhouseClientService;
  snapshotFieldCatalogService: SnapshotFieldCatalogService;
};

const MARKET_TABLE_COLUMNS = [
  "slug String",
  "asset LowCardinality(String)",
  "window LowCardinality(String)",
  "price_to_beat Nullable(Float64)",
  "market_start DateTime64(3, 'UTC')",
  "market_end DateTime64(3, 'UTC')",
  "inserted_at DateTime64(3, 'UTC')",
].join(",\n        ");

const LEGACY_SNAPSHOT_COLUMNS = [
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

const MIGRATION_STATE_COLUMNS = [
  "migration_name String",
  "phase LowCardinality(String)",
  "last_completed_day Nullable(Date)",
  "is_completed UInt8",
  "error_message Nullable(String)",
  "updated_at DateTime64(3, 'UTC')",
].join(",\n        ");

/**
 * @section class
 */

export class ClickhouseSchemaService {
  /**
   * @section private:attributes
   */

  private readonly clickhouseClientService: ClickhouseClientService;
  private readonly snapshotFieldCatalogService: SnapshotFieldCatalogService;

  /**
   * @section constructor
   */

  public constructor(options: ClickhouseSchemaServiceOptions) {
    this.clickhouseClientService = options.clickhouseClientService;
    this.snapshotFieldCatalogService = options.snapshotFieldCatalogService;
  }

  /**
   * @section private:methods
   */

  private buildCreateSnapshotTableQuery(): string {
    const snapshotColumnDefinitions = this.snapshotFieldCatalogService.readSnapshotColumnDefinitions().join(",\n        ");
    const query = `
      CREATE TABLE IF NOT EXISTS ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE} (
        id UUID,
        generated_at DateTime64(3, 'UTC'),
        ${snapshotColumnDefinitions},
        inserted_at DateTime64(3, 'UTC')
      )
      ENGINE = MergeTree
      PARTITION BY toDate(generated_at)
      ORDER BY (generated_at, id)
    `;
    return query;
  }

  /**
   * @section public:methods
   */

  public async ensureSchema(): Promise<void> {
    const schemaQueries = [
      `
        CREATE TABLE IF NOT EXISTS ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE} (
          ${MARKET_TABLE_COLUMNS}
        )
        ENGINE = MergeTree
        ORDER BY (asset, window, market_start, slug)
      `,
      `
        CREATE TABLE IF NOT EXISTS ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_LEGACY_SNAPSHOT_TABLE} (
          ${LEGACY_SNAPSHOT_COLUMNS}
        )
        ENGINE = MergeTree
        PARTITION BY toDate(generated_at)
        ORDER BY (market_slug, generated_at, asset, window)
      `,
      this.buildCreateSnapshotTableQuery(),
      `
        CREATE TABLE IF NOT EXISTS ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MIGRATION_STATE_TABLE} (
          ${MIGRATION_STATE_COLUMNS}
        )
        ENGINE = MergeTree
        ORDER BY (migration_name, updated_at)
      `,
    ];
    for (const schemaQuery of schemaQueries) {
      await this.clickhouseClientService.command(schemaQuery);
    }
  }
}
