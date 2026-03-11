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

type ExistingMarketRow = { slug: string };

/**
 * @section private:properties
 */

export class MarketRepositoryService {
  private readonly clickhouseClientService: ClickhouseClientService;

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
    const marketRecord: MarketRecord = {
      slug: row.slug,
      marketId: row.market_id,
      marketConditionId: row.market_condition_id,
      asset: row.asset as MarketRecord["asset"],
      window: row.window as MarketRecord["window"],
      priceToBeat: row.price_to_beat,
      marketStart: new Date(row.market_start).toISOString(),
      marketEnd: new Date(row.market_end).toISOString(),
    };
    return marketRecord;
  }

  private buildFindMarketBySlugQuery(slug: string): string {
    const escapedSlug = JSON.stringify(slug);
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
    const fromDateClause = options.fromDate ? ` AND market_start >= toDateTime64(${JSON.stringify(options.fromDate)}, 3, 'UTC')` : "";
    const query = `
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM ${config.CLICKHOUSE_DATABASE}.${config.CLICKHOUSE_MARKET_TABLE}
      WHERE asset = ${JSON.stringify(options.asset)}
        AND window = ${JSON.stringify(options.window)}${fromDateClause}
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
      market_start: snapshot.marketStart || new Date(0).toISOString(),
      market_end: snapshot.marketEnd || new Date(0).toISOString(),
    };
    return marketRow;
  }

  /**
   * @section public:methods
   */

  public async ensureMarketStored(snapshot: Snapshot): Promise<void> {
    const existingRows = await this.clickhouseClientService.queryJsonRows<ExistingMarketRow>(this.buildFindMarketBySlugQuery(snapshot.marketSlug || ""));
    const hasExistingMarket = existingRows.length > 0;
    if (!hasExistingMarket) {
      const marketRow = this.buildInsertMarketRow(snapshot);
      await this.clickhouseClientService.insertJsonRows(config.CLICKHOUSE_MARKET_TABLE, [{ ...marketRow, inserted_at: new Date().toISOString() }]);
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
