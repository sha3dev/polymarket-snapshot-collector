/**
 * @section imports:externals
 */

import type { Snapshot } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import type { ClickhouseClientService } from "../clickhouse/clickhouse-client.service.ts";
import config from "../config.ts";
import type { MarketLookupOptions, MarketRecord } from "./market.types.ts";

/**
 * @section types
 */

type MarketRepositoryServiceOptions = { clickhouseClientService: ClickhouseClientService };

type MarketRow = {
  slug: string;
  market_id: string | null;
  market_condition_id: string | null;
  asset: string;
  window: string;
  price_to_beat: number | null;
  market_start: string;
  market_end: string;
};

/**
 * @section private:attributes
 */

export class MarketRepositoryService {
  private readonly clickhouseClientService: ClickhouseClientService;
  private readonly pendingPriceToBeatBackfills = new Set<string>();
  private readonly cachedMarketRecordBySlug = new Map<string, MarketRecord>();

  /**
   * @section constructor
   */

  public constructor(options: MarketRepositoryServiceOptions) {
    this.clickhouseClientService = options.clickhouseClientService;
  }

  /**
   * @section private:methods
   */

  private mapMarketRow(row: MarketRow): MarketRecord {
    const marketStart = new Date(`${row.market_start.replace(" ", "T")}Z`).toISOString();
    const marketEnd = new Date(`${row.market_end.replace(" ", "T")}Z`).toISOString();
    const marketRecord: MarketRecord = {
      slug: row.slug,
      marketId: row.market_id,
      marketConditionId: row.market_condition_id,
      asset: row.asset as MarketRecord["asset"],
      window: row.window as MarketRecord["window"],
      priceToBeat: row.price_to_beat,
      marketStart,
      marketEnd,
    };
    return marketRecord;
  }

  private buildSqlStringLiteral(value: string): string {
    const sqlStringLiteral = `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
    return sqlStringLiteral;
  }

  private buildMarketRecordFromSnapshot(snapshot: Snapshot): MarketRecord {
    const marketRecord: MarketRecord = {
      slug: snapshot.marketSlug || "",
      marketId: snapshot.marketId,
      marketConditionId: snapshot.marketConditionId,
      asset: snapshot.asset,
      window: snapshot.window,
      priceToBeat: snapshot.priceToBeat,
      marketStart: new Date(snapshot.marketStart || 0).toISOString(),
      marketEnd: new Date(snapshot.marketEnd || 0).toISOString(),
    };
    return marketRecord;
  }

  private hasFinalPriceToBeat(marketRecord: MarketRecord | null): boolean {
    const hasFinalValue = marketRecord?.priceToBeat !== null && marketRecord?.priceToBeat !== undefined;
    return hasFinalValue;
  }

  private async loadStoredMarketRecord(snapshotSlug: string): Promise<MarketRecord | null> {
    const existingRows = await this.clickhouseClientService.queryJsonRows<MarketRow>(`
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
      WHERE slug = ${this.buildSqlStringLiteral(snapshotSlug)}
      ORDER BY market_start ASC
      LIMIT 1
    `);
    const existingMarketRow = existingRows[0] || null;
    const marketRecord = existingMarketRow ? this.mapMarketRow(existingMarketRow) : null;
    if (marketRecord) {
      this.cachedMarketRecordBySlug.set(marketRecord.slug, marketRecord);
    }
    return marketRecord;
  }

  private async insertMarketRecord(snapshot: Snapshot, snapshotSlug: string): Promise<MarketRecord> {
    const marketRow: MarketRow = {
      slug: snapshotSlug,
      market_id: snapshot.marketId,
      market_condition_id: snapshot.marketConditionId,
      asset: snapshot.asset,
      window: snapshot.window,
      price_to_beat: snapshot.priceToBeat,
      market_start: new Date(snapshot.marketStart || 0).toISOString().replace("T", " ").replace("Z", ""),
      market_end: new Date(snapshot.marketEnd || 0).toISOString().replace("T", " ").replace("Z", ""),
    };
    const insertedAt = new Date().toISOString().replace("T", " ").replace("Z", "");
    await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_MARKET_TABLE, [{ ...marketRow, inserted_at: insertedAt }]);
    const marketRecord = this.buildMarketRecordFromSnapshot(snapshot);
    this.cachedMarketRecordBySlug.set(marketRecord.slug, marketRecord);
    return marketRecord;
  }

  private async backfillPriceToBeatIfNeeded(marketRecord: MarketRecord, snapshot: Snapshot): Promise<MarketRecord> {
    let nextMarketRecord = marketRecord;
    if (
      marketRecord.priceToBeat === null &&
      snapshot.priceToBeat !== null &&
      snapshot.priceToBeat !== undefined &&
      !this.pendingPriceToBeatBackfills.has(marketRecord.slug)
    ) {
      this.pendingPriceToBeatBackfills.add(marketRecord.slug);
      try {
        await this.clickhouseClientService.command(`
          ALTER TABLE ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
          UPDATE price_to_beat = ${snapshot.priceToBeat}
          WHERE slug = ${this.buildSqlStringLiteral(marketRecord.slug)} AND price_to_beat IS NULL
        `);
        nextMarketRecord = { ...marketRecord, priceToBeat: snapshot.priceToBeat };
        this.cachedMarketRecordBySlug.set(nextMarketRecord.slug, nextMarketRecord);
      } catch (error) {
        this.pendingPriceToBeatBackfills.delete(marketRecord.slug);
        throw error;
      }
    }
    return nextMarketRecord;
  }

  /**
   * @section public:methods
   */

  public async ensureMarketStored(snapshot: Snapshot): Promise<MarketRecord> {
    const snapshotSlug = snapshot.marketSlug || "";
    let marketRecord = this.cachedMarketRecordBySlug.get(snapshotSlug) || null;
    if (!marketRecord) {
      marketRecord = await this.loadStoredMarketRecord(snapshotSlug);
    }
    if (!marketRecord) {
      marketRecord = await this.insertMarketRecord(snapshot, snapshotSlug);
    }
    marketRecord = await this.backfillPriceToBeatIfNeeded(marketRecord, snapshot);
    return marketRecord;
  }

  public async findMarketBySlug(slug: string): Promise<MarketRecord | null> {
    const cachedMarketRecord = this.cachedMarketRecordBySlug.get(slug) || null;
    let marketRecord = cachedMarketRecord;
    const shouldRefreshFromClickhouse = !this.hasFinalPriceToBeat(cachedMarketRecord);
    if (shouldRefreshFromClickhouse) {
      const rows = await this.clickhouseClientService.queryJsonRows<MarketRow>(`
        SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
        FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
        WHERE slug = ${this.buildSqlStringLiteral(slug)}
        ORDER BY market_start ASC
        LIMIT 1
      `);
      marketRecord = rows.length > 0 ? this.mapMarketRow(rows[0] as MarketRow) : null;
      if (marketRecord) {
        this.cachedMarketRecordBySlug.set(marketRecord.slug, marketRecord);
      }
    }
    return marketRecord;
  }

  public async listMarkets(options: MarketLookupOptions): Promise<MarketRecord[]> {
    const assetClause = options.asset ? ` AND asset = ${this.buildSqlStringLiteral(options.asset)}` : "";
    const windowClause = options.window ? ` AND window = ${this.buildSqlStringLiteral(options.window)}` : "";
    const fromDateClause = options.fromDate
      ? ` AND market_start >= toDateTime64(${this.buildSqlStringLiteral(new Date(options.fromDate).toISOString().replace("T", " ").replace("Z", ""))}, 3, 'UTC')`
      : "";
    const rows = await this.clickhouseClientService.queryJsonRows<MarketRow>(`
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
      WHERE 1 = 1${assetClause}${windowClause}${fromDateClause}
      ORDER BY market_start ASC
    `);
    const marketRecords = rows.map((row) => this.mapMarketRow(row));
    for (const marketRecord of marketRecords) {
      this.cachedMarketRecordBySlug.set(marketRecord.slug, marketRecord);
    }
    return marketRecords;
  }
}
