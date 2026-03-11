/**
 * @section imports:externals
 */

import type { Snapshot, SnapshotAsset, SnapshotWindow } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import type { MarketRecord } from "../market/market.types.ts";
import type { DashboardMarketDirection, DashboardPayload, DashboardWidget, DashboardWidgetSnapshot, MarketSummary } from "./snapshot.types.ts";

/**
 * @section types
 */

type DashboardStateServiceOptions = {
  staleAfterMs: number;
  supportedAssets: readonly SnapshotAsset[];
  supportedWindows: readonly SnapshotWindow[];
};

type DashboardWidgetState = Omit<DashboardWidget, "latestSnapshotAgeMs" | "isStale">;

/**
 * @section private:properties
 */

export class DashboardStateService {
  private readonly staleAfterMs: number;
  private readonly supportedAssets: readonly SnapshotAsset[];
  private readonly supportedWindows: readonly SnapshotWindow[];
  private readonly widgetStateByKey = new Map<string, DashboardWidgetState>();

  /**
   * @section constructor
   */

  public constructor(options: DashboardStateServiceOptions) {
    this.staleAfterMs = options.staleAfterMs;
    this.supportedAssets = options.supportedAssets;
    this.supportedWindows = options.supportedWindows;
  }

  /**
   * @section factory
   */

  public static createDefault(): DashboardStateService {
    const supportedAssets = [...config.SUPPORTED_ASSETS];
    const supportedWindows = [...config.SUPPORTED_WINDOWS];
    return new DashboardStateService({ staleAfterMs: config.DASHBOARD_STALE_AFTER_MS, supportedAssets, supportedWindows });
  }

  /**
   * @section private:methods
   */

  private buildWidgetKey(asset: SnapshotAsset, window: SnapshotWindow): string {
    const widgetKey = `${asset}:${window}`;
    return widgetKey;
  }

  private readMarketDirection(widgetSnapshot: DashboardWidgetSnapshot | null): DashboardMarketDirection {
    let marketDirection: DashboardMarketDirection = "UNKNOWN";
    const referencePrice = widgetSnapshot?.chainlinkPrice;
    const priceToBeat = widgetSnapshot?.priceToBeat;
    const canReadMarketDirection = referencePrice !== null && referencePrice !== undefined && priceToBeat !== null && priceToBeat !== undefined;
    if (canReadMarketDirection) {
      marketDirection = referencePrice >= priceToBeat ? "UP" : "DOWN";
    }
    return marketDirection;
  }

  private buildWidgetState(marketRecord: MarketRecord, snapshot: Snapshot, currentWidgetState: DashboardWidgetState | null): DashboardWidgetState {
    const isSameMarket = currentWidgetState?.market?.slug === marketRecord.slug;
    const widgetSnapshot: DashboardWidgetSnapshot = {
      generatedAt: snapshot.generatedAt,
      priceToBeat: marketRecord.priceToBeat,
      upPrice: snapshot.upPrice,
      downPrice: snapshot.downPrice,
      chainlinkPrice: snapshot.chainlinkPrice,
      binancePrice: snapshot.binancePrice,
      coinbasePrice: snapshot.coinbasePrice,
      krakenPrice: snapshot.krakenPrice,
      okxPrice: snapshot.okxPrice,
    };
    return {
      asset: marketRecord.asset,
      window: marketRecord.window,
      market: {
        slug: marketRecord.slug,
        asset: marketRecord.asset,
        window: marketRecord.window,
        priceToBeat: marketRecord.priceToBeat,
        marketStart: marketRecord.marketStart,
        marketEnd: marketRecord.marketEnd,
      } satisfies MarketSummary,
      snapshotCount: isSameMarket && currentWidgetState ? currentWidgetState.snapshotCount + 1 : 1,
      latestSnapshot: widgetSnapshot,
      marketDirection: this.readMarketDirection(widgetSnapshot),
    };
  }

  /**
   * @section public:methods
   */

  public updateSnapshot(marketRecord: MarketRecord, snapshot: Snapshot): void {
    const widgetKey = this.buildWidgetKey(marketRecord.asset, marketRecord.window);
    const currentWidgetState = this.widgetStateByKey.get(widgetKey) || null;
    const nextWidgetState = this.buildWidgetState(marketRecord, snapshot, currentWidgetState);
    this.widgetStateByKey.set(widgetKey, nextWidgetState);
  }

  public readDashboard(): DashboardPayload {
    const widgets: DashboardWidget[] = [];
    for (const asset of this.supportedAssets) {
      for (const window of this.supportedWindows) {
        const widgetState = this.widgetStateByKey.get(this.buildWidgetKey(asset, window)) || null;
        const latestSnapshotAgeMs = widgetState?.latestSnapshot ? Date.now() - widgetState.latestSnapshot.generatedAt : null;
        const marketDirection = "UNKNOWN" as const;
        const widget = widgetState
          ? { ...widgetState, latestSnapshotAgeMs, isStale: latestSnapshotAgeMs === null || latestSnapshotAgeMs > this.staleAfterMs }
          : { asset, window, market: null, snapshotCount: 0, latestSnapshot: null, marketDirection, latestSnapshotAgeMs: null, isStale: true };
        widgets.push(widget);
      }
    }
    const payload: DashboardPayload = { generatedAt: new Date().toISOString(), widgets };
    return payload;
  }
}
