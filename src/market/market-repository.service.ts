/**
 * @section imports:internals
 */

import type { ClickhouseClientService } from "../clickhouse/clickhouse-client.service.ts";
import config from "../config.ts";
import type { MarketLookupOptions, MarketRecord, MarketSnapshotRecord } from "./market.types.ts";

/**
 * @section types
 */

type MarketRepositoryServiceOptions = { clickhouseClientService: ClickhouseClientService };

type MarketRow = {
  slug: string;
  asset: string;
  window: string;
  price_to_beat: number | null;
  market_start: string;
  market_end: string;
};

/**
 * @section class
 */

export class MarketRepositoryService {
  /**
   * @section private:attributes
   */

  private readonly clickhouseClientService: ClickhouseClientService;
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

  private buildSqlStringLiteral(value: string): string {
    const sqlStringLiteral = `'${value.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
    return sqlStringLiteral;
  }

  private formatClickhouseDateTime(value: string): string {
    const clickhouseDateTime = new Date(value).toISOString().replace("T", " ").replace("Z", "");
    return clickhouseDateTime;
  }

  private normalizeClickhouseDateTime(value: string): string {
    const normalizedDateTime = new Date(`${value.replace(" ", "T")}Z`).toISOString();
    return normalizedDateTime;
  }

  private mapMarketRow(marketRow: MarketRow): MarketRecord {
    const marketRecord: MarketRecord = {
      slug: marketRow.slug,
      asset: marketRow.asset as MarketRecord["asset"],
      window: marketRow.window as MarketRecord["window"],
      priceToBeat: marketRow.price_to_beat,
      marketStart: this.normalizeClickhouseDateTime(marketRow.market_start),
      marketEnd: this.normalizeClickhouseDateTime(marketRow.market_end),
    };
    return marketRecord;
  }

  private async loadStoredMarketRecord(slug: string): Promise<MarketRecord | null> {
    const rows = await this.clickhouseClientService.queryJsonRows<MarketRow>(`
      SELECT slug, asset, window, price_to_beat, market_start, market_end
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
      WHERE slug = ${this.buildSqlStringLiteral(slug)}
      ORDER BY market_start ASC
      LIMIT 1
    `);
    const marketRecord = rows.length > 0 ? this.mapMarketRow(rows[0] as MarketRow) : null;
    if (marketRecord !== null) {
      this.cachedMarketRecordBySlug.set(marketRecord.slug, marketRecord);
    }
    return marketRecord;
  }

  private async insertMarketRecord(marketSnapshotRecord: MarketSnapshotRecord): Promise<MarketRecord> {
    const insertedAt = new Date().toISOString().replace("T", " ").replace("Z", "");
    await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_MARKET_TABLE, [
      {
        slug: marketSnapshotRecord.slug,
        asset: marketSnapshotRecord.asset,
        window: marketSnapshotRecord.window,
        price_to_beat: marketSnapshotRecord.priceToBeat,
        market_start: this.formatClickhouseDateTime(marketSnapshotRecord.marketStart),
        market_end: this.formatClickhouseDateTime(marketSnapshotRecord.marketEnd),
        inserted_at: insertedAt,
      },
    ]);
    const marketRecord: MarketRecord = {
      slug: marketSnapshotRecord.slug,
      asset: marketSnapshotRecord.asset,
      window: marketSnapshotRecord.window,
      priceToBeat: marketSnapshotRecord.priceToBeat,
      marketStart: marketSnapshotRecord.marketStart,
      marketEnd: marketSnapshotRecord.marketEnd,
    };
    this.cachedMarketRecordBySlug.set(marketRecord.slug, marketRecord);
    return marketRecord;
  }

  /**
   * @section public:methods
   */

  public async ensureMarketStored(marketSnapshotRecord: MarketSnapshotRecord): Promise<MarketRecord> {
    let marketRecord = this.cachedMarketRecordBySlug.get(marketSnapshotRecord.slug) || null;
    if (marketRecord === null) {
      marketRecord = await this.loadStoredMarketRecord(marketSnapshotRecord.slug);
    }
    if (marketRecord === null) {
      marketRecord = await this.insertMarketRecord(marketSnapshotRecord);
    }
    return marketRecord;
  }

  public async listMarkets(options: MarketLookupOptions): Promise<MarketRecord[]> {
    const assetClause = options.asset ? ` AND asset = ${this.buildSqlStringLiteral(options.asset)}` : "";
    const windowClause = options.window ? ` AND window = ${this.buildSqlStringLiteral(options.window)}` : "";
    const fromDateClause = options.fromDate
      ? ` AND market_start >= toDateTime64(${this.buildSqlStringLiteral(this.formatClickhouseDateTime(options.fromDate))}, 3, 'UTC')`
      : "";
    const rows = await this.clickhouseClientService.queryJsonRows<MarketRow>(`
      SELECT slug, asset, window, price_to_beat, market_start, market_end
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
      WHERE 1 = 1${assetClause}${windowClause}${fromDateClause}
      ORDER BY market_start ASC
    `);
    const marketRecords = rows.map((marketRow) => this.mapMarketRow(marketRow));
    for (const marketRecord of marketRecords) {
      this.cachedMarketRecordBySlug.set(marketRecord.slug, marketRecord);
    }
    return marketRecords;
  }
}
