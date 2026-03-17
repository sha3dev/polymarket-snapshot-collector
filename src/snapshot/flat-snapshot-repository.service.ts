/**
 * @section imports:externals
 */

import { randomUUID } from "node:crypto";

import type { Snapshot } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import type { ClickhouseClientService } from "../clickhouse/clickhouse-client.service.ts";
import config from "../config.ts";
import LOGGER from "../logger.ts";
import type { SnapshotFieldCatalogService } from "./snapshot-field-catalog.service.ts";
import type { SnapshotFieldValue, StoredSnapshot } from "./snapshot.types.ts";

/**
 * @section types
 */

type FlatSnapshotRepositoryServiceOptions = {
  clickhouseClientService: ClickhouseClientService;
  snapshotFieldCatalogService: SnapshotFieldCatalogService;
};

type SnapshotReadOptions = {
  fromDate: string;
  toDate: string;
  limit: number;
  marketSlug: string | null;
};

type FlatSnapshotStorageRow = {
  id: string;
  generated_at: string;
  inserted_at: string;
} & Record<string, SnapshotFieldValue>;

/**
 * @section class
 */

export class FlatSnapshotRepositoryService {
  /**
   * @section private:attributes
   */

  private readonly clickhouseClientService: ClickhouseClientService;
  private readonly snapshotFieldCatalogService: SnapshotFieldCatalogService;

  /**
   * @section constructor
   */

  public constructor(options: FlatSnapshotRepositoryServiceOptions) {
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

  private formatClickhouseDateTime(value: string | number): string {
    const clickhouseDateTime = new Date(value).toISOString().replace("T", " ").replace("Z", "");
    return clickhouseDateTime;
  }

  private normalizeClickhouseDateTime(value: string): string {
    const normalizedDateTime = new Date(`${value.replace(" ", "T")}Z`).toISOString();
    return normalizedDateTime;
  }

  private shouldMapDateField(fieldName: string): boolean {
    const isDateField = fieldName.endsWith("_market_start") || fieldName.endsWith("_market_end");
    return isDateField;
  }

  private mapInsertValue(fieldName: string, snapshot: Snapshot): SnapshotFieldValue {
    const snapshotValue = snapshot[fieldName];
    let mappedValue: SnapshotFieldValue = null;
    if (typeof snapshotValue === "number") {
      mappedValue = snapshotValue;
    }
    if (typeof snapshotValue === "string") {
      mappedValue = this.shouldMapDateField(fieldName) ? this.formatClickhouseDateTime(snapshotValue) : snapshotValue;
    }
    return mappedValue;
  }

  private buildInsertRow(snapshot: Snapshot): Record<string, SnapshotFieldValue> {
    const insertRow: Record<string, SnapshotFieldValue> = {
      id: randomUUID(),
      generated_at: this.formatClickhouseDateTime(snapshot.generated_at),
      inserted_at: this.formatClickhouseDateTime(Date.now()),
    };
    for (const fieldName of this.snapshotFieldCatalogService.readSnapshotFieldNames()) {
      insertRow[fieldName] = this.mapInsertValue(fieldName, snapshot);
    }
    return insertRow;
  }

  private mapStoredSnapshot(flatSnapshotStorageRow: FlatSnapshotStorageRow): StoredSnapshot {
    const storedSnapshot: StoredSnapshot = {
      id: flatSnapshotStorageRow.id,
      generated_at: new Date(`${flatSnapshotStorageRow.generated_at.replace(" ", "T")}Z`).getTime(),
      inserted_at: this.normalizeClickhouseDateTime(flatSnapshotStorageRow.inserted_at),
    };
    for (const fieldName of this.snapshotFieldCatalogService.readSnapshotFieldNames()) {
      const fieldValue = flatSnapshotStorageRow[fieldName] ?? null;
      storedSnapshot[fieldName] =
        typeof fieldValue === "string" && this.shouldMapDateField(fieldName) ? this.normalizeClickhouseDateTime(fieldValue) : fieldValue;
    }
    return storedSnapshot;
  }

  /**
   * @section public:methods
   */

  public async insertSnapshots(snapshots: readonly Snapshot[]): Promise<void> {
    if (snapshots.length > 0) {
      const insertRows = snapshots.map((snapshot) => this.buildInsertRow(snapshot));
      try {
        await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_SNAPSHOT_TABLE, insertRows);
      } catch (error) {
        LOGGER.error(`snapshot batch insert failed for ${insertRows.length} row(s): ${error instanceof Error ? error.message : String(error)}`);
        throw error;
      }
    }
  }

  public async listSnapshots(options: SnapshotReadOptions): Promise<StoredSnapshot[]> {
    const marketSlugClause =
      options.marketSlug === null
        ? ""
        : ` AND (${this.snapshotFieldCatalogService
            .readSlugFieldNames()
            .map((fieldName) => `${fieldName} = ${this.buildSqlStringLiteral(options.marketSlug || "")}`)
            .join(" OR ")})`;
    const snapshotSelectClause = ["id", "generated_at", ...this.snapshotFieldCatalogService.readSnapshotFieldNames(), "inserted_at"].join(", ");
    const query = `
      SELECT ${snapshotSelectClause}
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE}
      WHERE generated_at >= toDateTime64(${this.buildSqlStringLiteral(this.formatClickhouseDateTime(options.fromDate))}, 3, 'UTC')
        AND generated_at <= toDateTime64(${this.buildSqlStringLiteral(this.formatClickhouseDateTime(options.toDate))}, 3, 'UTC')
        ${marketSlugClause}
      ORDER BY generated_at ASC, id ASC
      LIMIT ${options.limit}
    `;
    const flatSnapshotStorageRows = await this.clickhouseClientService.queryJsonRows<FlatSnapshotStorageRow>(query);
    const storedSnapshots = flatSnapshotStorageRows.map((flatSnapshotStorageRow) => this.mapStoredSnapshot(flatSnapshotStorageRow));
    return storedSnapshots;
  }
}
