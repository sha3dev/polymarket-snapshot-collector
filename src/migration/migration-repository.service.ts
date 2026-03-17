/**
 * @section imports:internals
 */

import type { ClickhouseClientService } from "../clickhouse/clickhouse-client.service.ts";
import config from "../config.ts";
import type { SnapshotFieldCatalogService } from "../snapshot/snapshot-field-catalog.service.ts";
import type { CountRow, LegacySnapshotRangeRow, MigrationPhase, MigrationState, MigrationStateRecord } from "./migration.types.ts";

/**
 * @section consts
 */

const MIGRATION_NAME = "snapshot_v2_backfill";
const ASSET_FIELD_MAPPINGS = [
  ["binance_price", "binance_price"],
  ["binance_order_book", "binance_order_book_json"],
  ["binance_event_ts", "binance_event_ts"],
  ["coinbase_price", "coinbase_price"],
  ["coinbase_order_book", "coinbase_order_book_json"],
  ["coinbase_event_ts", "coinbase_event_ts"],
  ["kraken_price", "kraken_price"],
  ["kraken_order_book", "kraken_order_book_json"],
  ["kraken_event_ts", "kraken_event_ts"],
  ["okx_price", "okx_price"],
  ["okx_order_book", "okx_order_book_json"],
  ["okx_event_ts", "okx_event_ts"],
  ["chainlink_price", "chainlink_price"],
  ["chainlink_event_ts", "chainlink_event_ts"],
] as const;

const PAIR_FIELD_MAPPINGS = [
  ["market_slug", "slug"],
  ["market_start", "market_start"],
  ["market_end", "market_end"],
  ["market_price_to_beat", "price_to_beat"],
  ["up_asset_id", "up_asset_id"],
  ["up_price", "up_price"],
  ["up_order_book", "up_order_book_json"],
  ["up_event_ts", "up_event_ts"],
  ["down_asset_id", "down_asset_id"],
  ["down_price", "down_price"],
  ["down_order_book", "down_order_book_json"],
  ["down_event_ts", "down_event_ts"],
] as const;

/**
 * @section types
 */

type MigrationRepositoryServiceOptions = {
  clickhouseClientService: ClickhouseClientService;
  snapshotFieldCatalogService: SnapshotFieldCatalogService;
};

/**
 * @section class
 */

export class MigrationRepositoryService {
  /**
   * @section private:attributes
   */

  private readonly clickhouseClientService: ClickhouseClientService;
  private readonly snapshotFieldCatalogService: SnapshotFieldCatalogService;

  /**
   * @section constructor
   */

  public constructor(options: MigrationRepositoryServiceOptions) {
    this.clickhouseClientService = options.clickhouseClientService;
    this.snapshotFieldCatalogService = options.snapshotFieldCatalogService;
  }

  /**
   * @section private:methods
   */

  private buildSqlStringLiteral(value: string): string {
    const sqlStringLiteral = `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
    return sqlStringLiteral;
  }

  private mapMigrationState(migrationStateRecord: MigrationStateRecord): MigrationState {
    const migrationState: MigrationState = {
      migrationName: migrationStateRecord.migration_name,
      phase: migrationStateRecord.phase,
      lastCompletedDay: migrationStateRecord.last_completed_day,
      isCompleted: migrationStateRecord.is_completed === 1,
      errorMessage: migrationStateRecord.error_message,
      updatedAt: new Date(`${migrationStateRecord.updated_at.replace(" ", "T")}Z`).toISOString(),
    };
    return migrationState;
  }

  private async readCount(query: string): Promise<number> {
    const rows = await this.clickhouseClientService.queryJsonRows<CountRow>(query);
    const count = rows[0]?.count || 0;
    return count;
  }

  private buildLegacyPairExpression(asset: "btc" | "eth" | "sol" | "xrp", window: "5m" | "15m", legacyColumnName: string): string {
    const condition = `asset = ${this.buildSqlStringLiteral(asset)} AND window = ${this.buildSqlStringLiteral(window)}`;
    const legacyPairExpression = `if(countIf(${condition}) = 0, NULL, anyIf(${legacyColumnName}, ${condition}))`;
    return legacyPairExpression;
  }

  private buildLegacyAssetExpression(asset: "btc" | "eth" | "sol" | "xrp", legacyColumnName: string): string {
    const condition = `asset = ${this.buildSqlStringLiteral(asset)}`;
    const priority = "tuple(multiIf(window = '5m', 1, window = '15m', 2, 99), inserted_at)";
    const legacyAssetExpression = `if(countIf(${condition}) = 0, NULL, argMinIf(${legacyColumnName}, ${priority}, ${condition}))`;
    return legacyAssetExpression;
  }

  private buildSnapshotFieldSelectClauses(): string[] {
    const snapshotFieldSelectClauses: string[] = [];
    for (const supportedAsset of this.snapshotFieldCatalogService.readSupportedAssets()) {
      for (const [legacyColumnName, fieldSuffix] of ASSET_FIELD_MAPPINGS) {
        snapshotFieldSelectClauses.push(`${this.buildLegacyAssetExpression(supportedAsset, legacyColumnName)} AS ${supportedAsset}_${fieldSuffix}`);
      }
      for (const supportedWindow of this.snapshotFieldCatalogService.readSupportedWindows()) {
        const pairPrefix = this.snapshotFieldCatalogService.readPairPrefix(supportedAsset, supportedWindow);
        for (const [legacyColumnName, fieldSuffix] of PAIR_FIELD_MAPPINGS) {
          snapshotFieldSelectClauses.push(
            `${this.buildLegacyPairExpression(supportedAsset, supportedWindow, legacyColumnName)} AS ${pairPrefix}_${fieldSuffix}`,
          );
        }
      }
    }
    return snapshotFieldSelectClauses;
  }

  /**
   * @section public:methods
   */

  public async readLatestMigrationState(): Promise<MigrationState | null> {
    const rows = await this.clickhouseClientService.queryJsonRows<MigrationStateRecord>(`
      SELECT migration_name, phase, last_completed_day, is_completed, error_message, updated_at
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MIGRATION_STATE_TABLE}
      WHERE migration_name = ${this.buildSqlStringLiteral(MIGRATION_NAME)}
      ORDER BY updated_at DESC
      LIMIT 1
    `);
    const migrationState = rows.length > 0 ? this.mapMigrationState(rows[0] as MigrationStateRecord) : null;
    return migrationState;
  }

  public async insertMigrationState(phase: MigrationPhase, lastCompletedDay: string | null, isCompleted: boolean, errorMessage: string | null): Promise<void> {
    await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_MIGRATION_STATE_TABLE, [
      {
        migration_name: MIGRATION_NAME,
        phase,
        last_completed_day: lastCompletedDay,
        is_completed: isCompleted ? 1 : 0,
        error_message: errorMessage,
        updated_at: new Date().toISOString().replace("T", " ").replace("Z", ""),
      },
    ]);
  }

  public async readLegacySnapshotRange(cutoffDay: string): Promise<{ minDay: string | null; maxDay: string | null }> {
    const rows = await this.clickhouseClientService.queryJsonRows<LegacySnapshotRangeRow>(`
      SELECT min(toDate(generated_at)) AS min_day, max(toDate(generated_at)) AS max_day
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_LEGACY_SNAPSHOT_TABLE}
      WHERE toDate(generated_at) < toDate(${this.buildSqlStringLiteral(cutoffDay)})
    `);
    const range = { minDay: rows[0]?.min_day || null, maxDay: rows[0]?.max_day || null };
    return range;
  }

  public async validateLegacySnapshotUniqueness(day: string): Promise<void> {
    const duplicateCount = await this.readCount(`
      SELECT count() AS count
      FROM
      (
        SELECT asset, window, generated_at, count() AS duplicate_count
        FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_LEGACY_SNAPSHOT_TABLE}
        WHERE toDate(generated_at) = toDate(${this.buildSqlStringLiteral(day)})
        GROUP BY asset, window, generated_at
        HAVING duplicate_count > 1
      )
    `);
    if (duplicateCount > 0) {
      throw new Error(`legacy snapshot uniqueness validation failed for ${day}: found ${duplicateCount} duplicate asset/window/generated_at row(s)`);
    }
  }

  public async migrateDay(day: string): Promise<void> {
    const snapshotFieldSelectClauses = this.buildSnapshotFieldSelectClauses().join(",\n        ");
    await this.clickhouseClientService.command(`
      ALTER TABLE ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE}
      DROP PARTITION toDate(${this.buildSqlStringLiteral(day)})
    `);
    await this.clickhouseClientService.command(`
      INSERT INTO ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE}
      SELECT
        generateUUIDv4() AS id,
        generated_at,
        ${snapshotFieldSelectClauses},
        now64(3) AS inserted_at
      FROM
      (
        SELECT
          legacy_snapshot.*,
          market.price_to_beat AS market_price_to_beat,
          market.market_start AS market_start,
          market.market_end AS market_end
        FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_LEGACY_SNAPSHOT_TABLE} AS legacy_snapshot
        LEFT JOIN ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE} AS market
          ON legacy_snapshot.market_slug = market.slug
        WHERE toDate(legacy_snapshot.generated_at) = toDate(${this.buildSqlStringLiteral(day)})
      )
      GROUP BY generated_at
      ORDER BY generated_at ASC
    `);
  }

  public async validateMigratedDay(day: string): Promise<void> {
    const expectedCount = await this.readCount(`
      SELECT count() AS count
      FROM
      (
        SELECT generated_at
        FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_LEGACY_SNAPSHOT_TABLE}
        WHERE toDate(generated_at) = toDate(${this.buildSqlStringLiteral(day)})
        GROUP BY generated_at
      )
    `);
    const actualCount = await this.readCount(`
      SELECT count() AS count
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE}
      WHERE toDate(generated_at) = toDate(${this.buildSqlStringLiteral(day)})
    `);
    if (expectedCount !== actualCount) {
      throw new Error(`snapshot_v2 validation failed for ${day}: expected ${expectedCount} row(s) and found ${actualCount}`);
    }
  }
}
