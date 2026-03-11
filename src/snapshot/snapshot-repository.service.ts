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
type PendingSnapshotInsert = { row: SnapshotInsertRow; resolve: () => void; reject: (error: unknown) => void };

/**
 * @section private:properties
 */

export class SnapshotRepositoryService {
  private readonly clickhouseClientService: ClickhouseClientService;
  private readonly maxBatchSize: number;
  private readonly maxBatchWaitMs: number;
  private readonly pendingSnapshotInserts: PendingSnapshotInsert[] = [];
  private activeFlushPromise: Promise<void> | null = null;
  private flushTimer: NodeJS.Timeout | null = null;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotRepositoryServiceOptions) {
    this.clickhouseClientService = options.clickhouseClientService;
    this.maxBatchSize = options.maxBatchSize ?? config.SNAPSHOT_INSERT_BATCH_MAX_SIZE;
    this.maxBatchWaitMs = options.maxBatchWaitMs ?? config.SNAPSHOT_INSERT_BATCH_MAX_WAIT_MS;
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

  private clearFlushTimer(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private readNextBatch(): PendingSnapshotInsert[] {
    const nextBatch = this.pendingSnapshotInserts.splice(0, this.maxBatchSize);
    return nextBatch;
  }

  private async flushPendingBatchLoop(): Promise<void> {
    while (this.pendingSnapshotInserts.length > 0) {
      const pendingBatch = this.readNextBatch();
      const batchRows = pendingBatch.map((pendingInsert) => pendingInsert.row);
      try {
        await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_SNAPSHOT_TABLE, batchRows);
        for (const pendingInsert of pendingBatch) {
          pendingInsert.resolve();
        }
      } catch (error) {
        LOGGER.error(`snapshot batch insert failed for ${pendingBatch.length} row(s): ${error instanceof Error ? error.message : String(error)}`);
        for (const pendingInsert of pendingBatch) {
          pendingInsert.reject(error);
        }
      }
    }
  }

  private async flushPendingBatch(): Promise<void> {
    let flushPromise = this.activeFlushPromise;
    if (!flushPromise && this.pendingSnapshotInserts.length > 0) {
      this.clearFlushTimer();
      flushPromise = this.flushPendingBatchLoop()
        .finally(() => {
          this.activeFlushPromise = null;
        });
      this.activeFlushPromise = flushPromise;
    }
    if (flushPromise) {
      await flushPromise;
    }
  }

  private async queueSnapshotInsert(snapshotInsertRow: SnapshotInsertRow): Promise<void> {
    const insertPromise = new Promise<void>((resolve, reject) => {
      this.pendingSnapshotInserts.push({ row: snapshotInsertRow, resolve, reject });
      const shouldFlushImmediately = this.pendingSnapshotInserts.length >= this.maxBatchSize;
      if (shouldFlushImmediately) {
        this.clearFlushTimer();
        void this.flushPendingBatch();
      }
      if (!shouldFlushImmediately && !this.flushTimer) {
        this.flushTimer = setTimeout(() => {
          void this.flushPendingBatch();
        }, this.maxBatchWaitMs);
      }
    });
    return await insertPromise;
  }

  /**
   * @section public:methods
   */

  public async insertSnapshot(snapshot: Snapshot): Promise<void> {
    const snapshotInsertRow = this.buildInsertRow(snapshot);
    await this.queueSnapshotInsert(snapshotInsertRow);
  }

  public async close(): Promise<void> {
    this.clearFlushTimer();
    await this.flushPendingBatch();
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
