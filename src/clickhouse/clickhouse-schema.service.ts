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
      this.buildCreateSnapshotTableQuery(),
    ];
    for (const schemaQuery of schemaQueries) {
      await this.clickhouseClientService.command(schemaQuery);
    }
  }
}
