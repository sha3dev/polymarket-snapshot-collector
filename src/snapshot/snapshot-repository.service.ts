/**
 * @section imports:externals
 */

import type { Snapshot } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import type { ClickhouseClientService } from "../clickhouse/clickhouse-client.service.ts";
import config from "../config.ts";
import LOGGER from "../logger.ts";
import type { SnapshotDuplicateRow, SnapshotStorageRow } from "./snapshot.types.ts";

/**
 * @section types
 */

type SnapshotRepositoryServiceOptions = { clickhouseClientService: ClickhouseClientService; maxBatchSize?: number; maxBatchWaitMs?: number };
type SnapshotInsertRow = SnapshotStorageRow & { inserted_at: string };
type SnapshotRepositoryDebugMetrics = {
  pendingInsertCount: number;
  totalInsertedSnapshotCount: number;
  totalFlushCount: number;
  lastFlushDurationMs: number;
  lastFlushBatchSize: number;
  isFlushActive: boolean;
};

/**
 * @section private:properties
 */

export class SnapshotRepositoryService {
  private readonly clickhouseClientService: ClickhouseClientService;
  private totalInsertedSnapshotCount = 0;
  private totalFlushCount = 0;
  private lastFlushDurationMs = 0;
  private lastFlushBatchSize = 0;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotRepositoryServiceOptions) {
    this.clickhouseClientService = options.clickhouseClientService;
  }

  /**
   * @section private:methods
   */

  private serializeOrderBook(orderBook: Snapshot["upOrderBook"] | Snapshot["binanceOrderBook"]): string | null {
    const serializedOrderBook = orderBook ? JSON.stringify(orderBook) : null;
    return serializedOrderBook;
  }

  private buildSqlStringLiteral(value: string): string {
    const sqlStringLiteral = `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
    return sqlStringLiteral;
  }

  private buildOutcomeFields(snapshot: Snapshot): Pick<SnapshotInsertRow, "up_asset_id" | "up_price" | "up_order_book" | "up_event_ts" | "down_asset_id" | "down_price" | "down_order_book" | "down_event_ts"> {
    return {
      up_asset_id: snapshot.upAssetId,
      up_price: snapshot.upPrice,
      up_order_book: this.serializeOrderBook(snapshot.upOrderBook),
      up_event_ts: snapshot.upEventTs,
      down_asset_id: snapshot.downAssetId,
      down_price: snapshot.downPrice,
      down_order_book: this.serializeOrderBook(snapshot.downOrderBook),
      down_event_ts: snapshot.downEventTs,
    };
  }

  private buildProviderFields(snapshot: Snapshot): Pick<SnapshotInsertRow, "binance_price" | "binance_order_book" | "binance_event_ts" | "coinbase_price" | "coinbase_order_book" | "coinbase_event_ts" | "kraken_price" | "kraken_order_book" | "kraken_event_ts" | "okx_price" | "okx_order_book" | "okx_event_ts" | "chainlink_price" | "chainlink_order_book" | "chainlink_event_ts"> {
    return {
      binance_price: snapshot.binancePrice,
      binance_order_book: this.serializeOrderBook(snapshot.binanceOrderBook),
      binance_event_ts: snapshot.binanceEventTs,
      coinbase_price: snapshot.coinbasePrice,
      coinbase_order_book: this.serializeOrderBook(snapshot.coinbaseOrderBook),
      coinbase_event_ts: snapshot.coinbaseEventTs,
      kraken_price: snapshot.krakenPrice,
      kraken_order_book: this.serializeOrderBook(snapshot.krakenOrderBook),
      kraken_event_ts: snapshot.krakenEventTs,
      okx_price: snapshot.okxPrice,
      okx_order_book: this.serializeOrderBook(snapshot.okxOrderBook),
      okx_event_ts: snapshot.okxEventTs,
      chainlink_price: snapshot.chainlinkPrice,
      chainlink_order_book: this.serializeOrderBook(snapshot.chainlinkOrderBook),
      chainlink_event_ts: snapshot.chainlinkEventTs,
    };
  }

  private buildInsertRow(snapshot: Snapshot): SnapshotInsertRow {
    return {
      asset: snapshot.asset, window: snapshot.window, market_slug: snapshot.marketSlug || "", generated_at: new Date(snapshot.generatedAt).toISOString().replace("T", " ").replace("Z", ""),
      ...this.buildOutcomeFields(snapshot),
      ...this.buildProviderFields(snapshot),
      inserted_at: new Date().toISOString().replace("T", " ").replace("Z", ""),
    };
  }

  private async insertSnapshotRows(snapshotInsertRows: readonly SnapshotInsertRow[]): Promise<void> {
    const flushStartedAtMs = Date.now();
    try {
      await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_SNAPSHOT_TABLE, snapshotInsertRows);
      this.totalInsertedSnapshotCount += snapshotInsertRows.length;
      this.totalFlushCount += 1;
      this.lastFlushDurationMs = Date.now() - flushStartedAtMs;
      this.lastFlushBatchSize = snapshotInsertRows.length;
      if (config.ENABLE_PERF_LOGS) {
        LOGGER.info(`snapshot batch flushed rows=${snapshotInsertRows.length} pending_after=0 flush_ms=${this.lastFlushDurationMs}`);
      }
    } catch (error) {
      LOGGER.error(`snapshot batch insert failed for ${snapshotInsertRows.length} row(s): ${error instanceof Error ? error.message : String(error)}`);
      throw error;
    }
  }

  /**
   * @section public:methods
   */

  public async insertSnapshot(snapshot: Snapshot): Promise<void> {
    await this.insertSnapshots([snapshot]);
  }

  public async insertSnapshots(snapshots: readonly Snapshot[]): Promise<void> {
    const snapshotInsertRows = snapshots.map((snapshot) => this.buildInsertRow(snapshot));
    await this.insertSnapshotRows(snapshotInsertRows);
  }

  public async close(): Promise<void> {
    await Promise.resolve();
  }

  public readDebugMetrics(): SnapshotRepositoryDebugMetrics {
    const debugMetrics: SnapshotRepositoryDebugMetrics = {
      pendingInsertCount: 0,
      totalInsertedSnapshotCount: this.totalInsertedSnapshotCount,
      totalFlushCount: this.totalFlushCount,
      lastFlushDurationMs: this.lastFlushDurationMs,
      lastFlushBatchSize: this.lastFlushBatchSize,
      isFlushActive: false,
    };
    return debugMetrics;
  }

  public async listDuplicateSnapshotsBySlug(slug: string): Promise<SnapshotDuplicateRow[]> {
    const query = `
      SELECT market_slug, asset, window, generated_at, count(*) AS duplicate_count
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE}
      WHERE market_slug = ${this.buildSqlStringLiteral(slug)}
      GROUP BY market_slug, asset, window, generated_at
      HAVING duplicate_count > 1
      ORDER BY generated_at ASC
    `;
    return await this.clickhouseClientService.queryJsonRows<SnapshotDuplicateRow>(query);
  }

  public async listSnapshotsBySlug(slug: string): Promise<SnapshotStorageRow[]> {
    const query = `
      SELECT
        asset, window, market_slug, generated_at,
        up_asset_id, up_price, up_order_book, up_event_ts,
        down_asset_id, down_price, down_order_book, down_event_ts,
        binance_price, binance_order_book, binance_event_ts,
        coinbase_price, coinbase_order_book, coinbase_event_ts,
        kraken_price, kraken_order_book, kraken_event_ts,
        okx_price, okx_order_book, okx_event_ts,
        chainlink_price, chainlink_order_book, chainlink_event_ts
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_SNAPSHOT_TABLE}
      WHERE market_slug = ${this.buildSqlStringLiteral(slug)}
      ORDER BY generated_at ASC
    `;
    return await this.clickhouseClientService.queryJsonRows<SnapshotStorageRow>(query);
  }
}
