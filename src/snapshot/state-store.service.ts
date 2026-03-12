/**
 * @section imports:externals
 */

import type { Snapshot, SnapshotAsset, SnapshotWindow } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import type { MarketRecord } from "../market/market.types.ts";
import type { MarketSummary, StateMarketDirection, StateMarketEntry, StateMarketSnapshotSummary, StatePayload } from "./snapshot.types.ts";

/**
 * @section types
 */

type StateStoreServiceOptions = {
  staleAfterMs: number;
  supportedAssets: readonly SnapshotAsset[];
  supportedWindows: readonly SnapshotWindow[];
};

type StateMarketEntryState = Omit<StateMarketEntry, "latestSnapshotAgeMs" | "isStale">;

/**
 * @section private:properties
 */

export class StateStoreService {
  private readonly staleAfterMs: number;
  private readonly supportedAssets: readonly SnapshotAsset[];
  private readonly supportedWindows: readonly SnapshotWindow[];
  private readonly marketStateByKey = new Map<string, StateMarketEntryState>();

  /**
   * @section constructor
   */

  public constructor(options: StateStoreServiceOptions) {
    this.staleAfterMs = options.staleAfterMs;
    this.supportedAssets = options.supportedAssets;
    this.supportedWindows = options.supportedWindows;
  }

  /**
   * @section factory
   */

  public static createDefault(): StateStoreService {
    const supportedAssets = [...config.SUPPORTED_ASSETS];
    const supportedWindows = [...config.SUPPORTED_WINDOWS];
    return new StateStoreService({ staleAfterMs: config.STATE_STALE_AFTER_MS, supportedAssets, supportedWindows });
  }

  /**
   * @section private:methods
   */

  private buildMarketKey(asset: SnapshotAsset, window: SnapshotWindow): string {
    const marketKey = `${asset}:${window}`;
    return marketKey;
  }

  private readMarketDirection(snapshotSummary: StateMarketSnapshotSummary | null): StateMarketDirection {
    let marketDirection: StateMarketDirection = "UNKNOWN";
    const referencePrice = snapshotSummary?.chainlinkPrice;
    const priceToBeat = snapshotSummary?.priceToBeat;
    const canReadMarketDirection = referencePrice !== null && referencePrice !== undefined && priceToBeat !== null && priceToBeat !== undefined;
    if (canReadMarketDirection) {
      marketDirection = referencePrice >= priceToBeat ? "UP" : "DOWN";
    }
    return marketDirection;
  }

  private buildStateEntry(marketRecord: MarketRecord, snapshot: Snapshot, currentMarketState: StateMarketEntryState | null): StateMarketEntryState {
    const isSameMarket = currentMarketState?.market?.slug === marketRecord.slug;
    const latestSnapshot: StateMarketSnapshotSummary = {
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
        prevPriceToBeat: marketRecord.prevPriceToBeat,
      } satisfies MarketSummary,
      snapshotCount: isSameMarket && currentMarketState ? currentMarketState.snapshotCount + 1 : 1,
      latestSnapshot,
      marketDirection: this.readMarketDirection(latestSnapshot),
    };
  }

  /**
   * @section public:methods
   */

  public updateSnapshot(marketRecord: MarketRecord, snapshot: Snapshot): void {
    const marketKey = this.buildMarketKey(marketRecord.asset, marketRecord.window);
    const currentMarketState = this.marketStateByKey.get(marketKey) || null;
    const nextMarketState = this.buildStateEntry(marketRecord, snapshot, currentMarketState);
    this.marketStateByKey.set(marketKey, nextMarketState);
  }

  public readState(): StatePayload {
    const markets: StateMarketEntry[] = [];
    for (const asset of this.supportedAssets) {
      for (const window of this.supportedWindows) {
        const marketState = this.marketStateByKey.get(this.buildMarketKey(asset, window)) || null;
        const latestSnapshotAgeMs = marketState?.latestSnapshot ? Date.now() - marketState.latestSnapshot.generatedAt : null;
        const marketDirection = "UNKNOWN" as const;
        const marketEntry = marketState
          ? { ...marketState, latestSnapshotAgeMs, isStale: latestSnapshotAgeMs === null || latestSnapshotAgeMs > this.staleAfterMs }
          : { asset, window, market: null, snapshotCount: 0, latestSnapshot: null, marketDirection, latestSnapshotAgeMs: null, isStale: true };
        markets.push(marketEntry);
      }
    }
    const payload: StatePayload = { generatedAt: new Date().toISOString(), markets };
    return payload;
  }
}
