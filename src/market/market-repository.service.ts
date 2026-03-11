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

  private buildFindMarketBySlugQuery(slug: string): string {
    const escapedSlug = this.buildSqlStringLiteral(slug);
    const query = `
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
      WHERE slug = ${escapedSlug}
      ORDER BY market_start ASC
      LIMIT 1
    `;
    return query;
  }

  private buildListMarketsQuery(options: MarketLookupOptions): string {
    const fromDateClause = options.fromDate
      ? ` AND market_start >= toDateTime64(${this.buildSqlStringLiteral(new Date(options.fromDate).toISOString().replace("T", " ").replace("Z", ""))}, 3, 'UTC')`
      : "";
    const query = `
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
      WHERE asset = ${this.buildSqlStringLiteral(options.asset)}
        AND window = ${this.buildSqlStringLiteral(options.window)}${fromDateClause}
      ORDER BY market_start ASC
    `;
    return query;
  }

  private buildInsertMarketRow(snapshot: Snapshot): MarketRow {
    const marketRow: MarketRow = {
      slug: snapshot.marketSlug || "",
      market_id: snapshot.marketId,
      market_condition_id: snapshot.marketConditionId,
      asset: snapshot.asset,
      window: snapshot.window,
      price_to_beat: snapshot.priceToBeat,
      market_start: new Date(snapshot.marketStart || 0).toISOString().replace("T", " ").replace("Z", ""),
      market_end: new Date(snapshot.marketEnd || 0).toISOString().replace("T", " ").replace("Z", ""),
    };
    return marketRow;
  }

  /**
   * @section public:methods
   */

  public async ensureMarketStored(snapshot: Snapshot): Promise<void> {
    const existingRows = await this.clickhouseClientService.queryJsonRows<MarketRow>(this.buildFindMarketBySlugQuery(snapshot.marketSlug || ""));
    const existingMarketRow = existingRows[0] || null;
    const hasExistingMarket = existingMarketRow !== null;
    if (!hasExistingMarket) {
      const marketRow = this.buildInsertMarketRow(snapshot);
      const insertedAt = new Date().toISOString().replace("T", " ").replace("Z", "");
      await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_MARKET_TABLE, [{ ...marketRow, inserted_at: insertedAt }]);
    }
    if (
      existingMarketRow &&
      existingMarketRow.price_to_beat === null &&
      snapshot.priceToBeat !== null &&
      snapshot.priceToBeat !== undefined &&
      !this.pendingPriceToBeatBackfills.has(existingMarketRow.slug)
    ) {
      this.pendingPriceToBeatBackfills.add(existingMarketRow.slug);
      try {
        await this.clickhouseClientService.command(`
          ALTER TABLE ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
          UPDATE price_to_beat = ${snapshot.priceToBeat}
          WHERE slug = ${this.buildSqlStringLiteral(existingMarketRow.slug)} AND price_to_beat IS NULL
        `);
      } catch (error) {
        this.pendingPriceToBeatBackfills.delete(existingMarketRow.slug);
        throw error;
      }
    }
  }

  public async findMarketBySlug(slug: string): Promise<MarketRecord | null> {
    const rows = await this.clickhouseClientService.queryJsonRows<MarketRow>(this.buildFindMarketBySlugQuery(slug));
    const marketRecord = rows.length > 0 ? this.mapMarketRow(rows[0] as MarketRow) : null;
    return marketRecord;
  }

  public async listMarkets(options: MarketLookupOptions): Promise<MarketRecord[]> {
    const rows = await this.clickhouseClientService.queryJsonRows<MarketRow>(this.buildListMarketsQuery(options));
    const marketRecords = rows.map((row) => this.mapMarketRow(row));
    return marketRecords;
  }
}
