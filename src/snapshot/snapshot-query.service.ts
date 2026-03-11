/**
 * @section imports:externals
 */

import type { Snapshot } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import type { MarketRepositoryService } from "../market/market-repository.service.ts";
import type { MarketRecord } from "../market/market.types.ts";
import { MarketNotFoundService } from "./market-not-found.service.ts";
import { SnapshotConsistencyService } from "./snapshot-consistency.service.ts";
import type { SnapshotRepositoryService } from "./snapshot-repository.service.ts";
import type {
  DashboardMarketDirection,
  DashboardPayload,
  DashboardWidget,
  DashboardWidgetSnapshot,
  MarketListPayload,
  MarketSnapshotsPayload,
  MarketSummary,
  SnapshotStorageRow,
} from "./snapshot.types.ts";

/**
 * @section types
 */

type SnapshotQueryServiceOptions = {
  marketRepositoryService: MarketRepositoryService;
  snapshotRepositoryService: SnapshotRepositoryService;
};

/**
 * @section private:properties
 */

export class SnapshotQueryService {
  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly snapshotRepositoryService: SnapshotRepositoryService;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotQueryServiceOptions) {
    this.marketRepositoryService = options.marketRepositoryService;
    this.snapshotRepositoryService = options.snapshotRepositoryService;
  }

  /**
   * @section private:methods
   */

  private parseOrderBook(serializedOrderBook: string | null): Snapshot["upOrderBook"] | Snapshot["binanceOrderBook"] {
    const parsedOrderBook = serializedOrderBook ? JSON.parse(serializedOrderBook) : null;
    return parsedOrderBook;
  }

  private parseClickhouseUtcDateTime(value: string): Date {
    const parsedDate = new Date(`${value.replace(" ", "T")}Z`);
    return parsedDate;
  }

  private mapOutcomeSnapshotFields(
    snapshotRow: SnapshotStorageRow,
  ): Pick<Snapshot, "priceToBeat" | "upAssetId" | "upPrice" | "upOrderBook" | "upEventTs" | "downAssetId" | "downPrice" | "downOrderBook" | "downEventTs"> {
    const outcomeSnapshotFields = {
      priceToBeat: snapshotRow.price_to_beat,
      upAssetId: snapshotRow.up_asset_id,
      upPrice: snapshotRow.up_price,
      upOrderBook: this.parseOrderBook(snapshotRow.up_order_book) as Snapshot["upOrderBook"],
      upEventTs: snapshotRow.up_event_ts,
      downAssetId: snapshotRow.down_asset_id,
      downPrice: snapshotRow.down_price,
      downOrderBook: this.parseOrderBook(snapshotRow.down_order_book) as Snapshot["downOrderBook"],
      downEventTs: snapshotRow.down_event_ts,
    };
    return outcomeSnapshotFields;
  }

  private mapProviderSnapshotFields(
    snapshotRow: SnapshotStorageRow,
  ): Pick<
    Snapshot,
    | "binancePrice"
    | "binanceOrderBook"
    | "binanceEventTs"
    | "coinbasePrice"
    | "coinbaseOrderBook"
    | "coinbaseEventTs"
    | "krakenPrice"
    | "krakenOrderBook"
    | "krakenEventTs"
    | "okxPrice"
    | "okxOrderBook"
    | "okxEventTs"
    | "chainlinkPrice"
    | "chainlinkOrderBook"
    | "chainlinkEventTs"
  > {
    const providerSnapshotFields = {
      binancePrice: snapshotRow.binance_price,
      binanceOrderBook: this.parseOrderBook(snapshotRow.binance_order_book) as Snapshot["binanceOrderBook"],
      binanceEventTs: snapshotRow.binance_event_ts,
      coinbasePrice: snapshotRow.coinbase_price,
      coinbaseOrderBook: this.parseOrderBook(snapshotRow.coinbase_order_book) as Snapshot["coinbaseOrderBook"],
      coinbaseEventTs: snapshotRow.coinbase_event_ts,
      krakenPrice: snapshotRow.kraken_price,
      krakenOrderBook: this.parseOrderBook(snapshotRow.kraken_order_book) as Snapshot["krakenOrderBook"],
      krakenEventTs: snapshotRow.kraken_event_ts,
      okxPrice: snapshotRow.okx_price,
      okxOrderBook: this.parseOrderBook(snapshotRow.okx_order_book) as Snapshot["okxOrderBook"],
      okxEventTs: snapshotRow.okx_event_ts,
      chainlinkPrice: snapshotRow.chainlink_price,
      chainlinkOrderBook: this.parseOrderBook(snapshotRow.chainlink_order_book) as Snapshot["chainlinkOrderBook"],
      chainlinkEventTs: snapshotRow.chainlink_event_ts,
    };
    return providerSnapshotFields;
  }

  private mapSnapshotRow(snapshotRow: SnapshotStorageRow): Snapshot {
    const snapshot: Snapshot = {
      asset: snapshotRow.asset as Snapshot["asset"],
      window: snapshotRow.window as Snapshot["window"],
      generatedAt: this.parseClickhouseUtcDateTime(snapshotRow.generated_at).getTime(),
      marketId: snapshotRow.market_id,
      marketSlug: snapshotRow.market_slug,
      marketConditionId: snapshotRow.market_condition_id,
      marketStart: this.parseClickhouseUtcDateTime(snapshotRow.market_start).toISOString(),
      marketEnd: this.parseClickhouseUtcDateTime(snapshotRow.market_end).toISOString(),
      ...this.mapOutcomeSnapshotFields(snapshotRow),
      ...this.mapProviderSnapshotFields(snapshotRow),
    };
    return snapshot;
  }

  private mapMarketSummary(marketRecord: MarketRecord): MarketSummary {
    const marketSummary: MarketSummary = {
      slug: marketRecord.slug,
      window: marketRecord.window,
      asset: marketRecord.asset,
      priceToBeat: marketRecord.priceToBeat,
      marketStart: marketRecord.marketStart,
      marketEnd: marketRecord.marketEnd,
    };
    return marketSummary;
  }

  private mapDashboardWidgetSnapshot(snapshot: Snapshot): DashboardWidgetSnapshot {
    const dashboardWidgetSnapshot: DashboardWidgetSnapshot = {
      generatedAt: snapshot.generatedAt,
      priceToBeat: snapshot.priceToBeat,
      upPrice: snapshot.upPrice,
      downPrice: snapshot.downPrice,
      chainlinkPrice: snapshot.chainlinkPrice,
      binancePrice: snapshot.binancePrice,
      coinbasePrice: snapshot.coinbasePrice,
      krakenPrice: snapshot.krakenPrice,
      okxPrice: snapshot.okxPrice,
    };
    return dashboardWidgetSnapshot;
  }

  private buildEmptyDashboardWidget(asset: MarketSummary["asset"], window: MarketSummary["window"]): DashboardWidget {
    const marketDirection = "UNKNOWN" as const;
    const dashboardWidget = { asset, window, market: null, snapshotCount: 0, latestSnapshot: null, marketDirection, latestSnapshotAgeMs: null, isStale: true };
    return dashboardWidget;
  }

  private buildDashboardWidgetAge(latestSnapshot: Snapshot | null): number | null {
    const latestSnapshotAgeMs = latestSnapshot ? Date.now() - latestSnapshot.generatedAt : null;
    return latestSnapshotAgeMs;
  }

  private readDashboardMarketDirection(latestSnapshot: Snapshot | null): DashboardMarketDirection {
    let marketDirection: DashboardMarketDirection = "UNKNOWN";
    const referencePrice = latestSnapshot?.chainlinkPrice;
    const priceToBeat = latestSnapshot?.priceToBeat;
    const canReadDirection = referencePrice !== null && referencePrice !== undefined && priceToBeat !== null && priceToBeat !== undefined;
    if (canReadDirection) {
      marketDirection = referencePrice >= priceToBeat ? "UP" : "DOWN";
    }
    return marketDirection;
  }

  private buildDashboardWidget(asset: MarketSummary["asset"], window: MarketSummary["window"], payload: MarketSnapshotsPayload | null): DashboardWidget {
    const latestSnapshot = payload?.snapshots[payload.snapshots.length - 1] || null;
    const marketDirection = this.readDashboardMarketDirection(latestSnapshot);
    const latestSnapshotAgeMs = this.buildDashboardWidgetAge(latestSnapshot);
    const isStale = latestSnapshotAgeMs === null || latestSnapshotAgeMs > config.SNAPSHOT_INTERVAL_MS * 4;
    const dashboardWidget: DashboardWidget = payload
      ? {
          asset,
          window,
          market: {
            slug: payload.slug,
            asset: payload.asset,
            window: payload.window,
            priceToBeat: latestSnapshot?.priceToBeat || null,
            marketStart: payload.marketStart,
            marketEnd: payload.marketEnd,
          },
          snapshotCount: payload.snapshots.length,
          latestSnapshot: latestSnapshot ? this.mapDashboardWidgetSnapshot(latestSnapshot) : null,
          marketDirection,
          latestSnapshotAgeMs,
          isStale,
        }
      : this.buildEmptyDashboardWidget(asset, window);
    return dashboardWidget;
  }

  private readMaxSnapshots(window: MarketSummary["window"]): number {
    const maxSnapshots = window === "5m" ? 600 : 1800;
    return maxSnapshots;
  }

  /**
   * @section public:methods
   */

  public async listMarkets(options: Parameters<MarketRepositoryService["listMarkets"]>[0]): Promise<MarketListPayload> {
    const marketRecords = await this.marketRepositoryService.listMarkets(options);
    const payload: MarketListPayload = { markets: marketRecords.map((marketRecord) => this.mapMarketSummary(marketRecord)) };
    return payload;
  }

  public async readMarketSnapshots(slug: string): Promise<MarketSnapshotsPayload> {
    const marketRecord = await this.marketRepositoryService.findMarketBySlug(slug);
    if (!marketRecord) {
      throw new MarketNotFoundService(slug);
    }
    const duplicateRows = await this.snapshotRepositoryService.listDuplicateSnapshotsBySlug(slug);
    if (duplicateRows.length > 0) {
      throw new SnapshotConsistencyService(`duplicate snapshot identities found for slug ${slug}`);
    }
    const snapshotRows = await this.snapshotRepositoryService.listSnapshotsBySlug(slug);
    const maxSnapshots = this.readMaxSnapshots(marketRecord.window);
    if (snapshotRows.length > maxSnapshots) {
      throw new SnapshotConsistencyService(`snapshot count ${snapshotRows.length} exceeds max ${maxSnapshots} for slug ${slug}`);
    }
    const payload: MarketSnapshotsPayload = {
      slug: marketRecord.slug,
      asset: marketRecord.asset,
      window: marketRecord.window,
      marketStart: marketRecord.marketStart,
      marketEnd: marketRecord.marketEnd,
      snapshots: snapshotRows.map((snapshotRow) => this.mapSnapshotRow(snapshotRow)),
    };
    return payload;
  }

  public async readDashboard(): Promise<DashboardPayload> {
    const widgets: DashboardWidget[] = [];
    for (const asset of config.SUPPORTED_ASSETS) {
      for (const window of config.SUPPORTED_WINDOWS) {
        const marketRecords = await this.marketRepositoryService.listMarkets({ asset, window, fromDate: null });
        const latestMarketRecord = marketRecords[marketRecords.length - 1] || null;
        const marketSnapshotsPayload = latestMarketRecord ? await this.readMarketSnapshots(latestMarketRecord.slug) : null;
        widgets.push(this.buildDashboardWidget(asset, window, marketSnapshotsPayload));
      }
    }
    const payload: DashboardPayload = { generatedAt: new Date().toISOString(), widgets };
    return payload;
  }
}
