/**
 * @section imports:externals
 */

import type { Snapshot } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import LOGGER from "../logger.ts";
import type { SnapshotFieldCatalogService } from "../snapshot/snapshot-field-catalog.service.ts";
import type { MarketRepositoryService } from "./market-repository.service.ts";
import type { MarketSnapshotRecord } from "./market.types.ts";

/**
 * @section types
 */

type MarketSyncServiceOptions = {
  marketRepositoryService: MarketRepositoryService;
  snapshotFieldCatalogService: SnapshotFieldCatalogService;
};

/**
 * @section class
 */

export class MarketSyncService {
  /**
   * @section private:attributes
   */

  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly snapshotFieldCatalogService: SnapshotFieldCatalogService;
  private readonly cachedSlugByPairKey = new Map<string, string>();

  /**
   * @section constructor
   */

  public constructor(options: MarketSyncServiceOptions) {
    this.marketRepositoryService = options.marketRepositoryService;
    this.snapshotFieldCatalogService = options.snapshotFieldCatalogService;
  }

  /**
   * @section factory
   */

  public static createDefault(options: MarketSyncServiceOptions): MarketSyncService {
    const marketSyncService = new MarketSyncService(options);
    return marketSyncService;
  }

  /**
   * @section private:methods
   */

  private buildPairKey(asset: "btc" | "eth" | "sol" | "xrp", window: "5m" | "15m"): string {
    const pairKey = `${asset}:${window}`;
    return pairKey;
  }

  private readSnapshotString(snapshot: Snapshot, fieldName: string): string | null {
    const snapshotValue = snapshot[fieldName];
    const stringValue = typeof snapshotValue === "string" ? snapshotValue : null;
    return stringValue;
  }

  private readSnapshotNumber(snapshot: Snapshot, fieldName: string): number | null {
    const snapshotValue = snapshot[fieldName];
    const numberValue = typeof snapshotValue === "number" ? snapshotValue : null;
    return numberValue;
  }

  private buildMarketSnapshotRecord(snapshot: Snapshot, asset: "btc" | "eth" | "sol" | "xrp", window: "5m" | "15m"): MarketSnapshotRecord | null {
    const pairPrefix = this.snapshotFieldCatalogService.readPairPrefix(asset, window);
    const slug = this.readSnapshotString(snapshot, `${pairPrefix}_slug`);
    const marketStart = this.readSnapshotString(snapshot, `${pairPrefix}_market_start`);
    const marketEnd = this.readSnapshotString(snapshot, `${pairPrefix}_market_end`);
    let marketSnapshotRecord: MarketSnapshotRecord | null = null;
    const hasMarketSnapshotRecord = slug !== null && marketStart !== null && marketEnd !== null;
    if (hasMarketSnapshotRecord) {
      marketSnapshotRecord = {
        slug,
        asset,
        window,
        priceToBeat: this.readSnapshotNumber(snapshot, `${pairPrefix}_price_to_beat`),
        marketStart,
        marketEnd,
      };
    }
    return marketSnapshotRecord;
  }

  private warnMissingMarketMetadata(asset: "btc" | "eth" | "sol" | "xrp", window: "5m" | "15m", slug: string): void {
    LOGGER.warn(`market sync skipped for ${asset}/${window} slug=${slug}: snapshot is missing market_start or market_end`);
  }

  /**
   * @section public:methods
   */

  public async syncSnapshot(snapshot: Snapshot): Promise<void> {
    for (const supportedAsset of this.snapshotFieldCatalogService.readSupportedAssets()) {
      for (const supportedWindow of this.snapshotFieldCatalogService.readSupportedWindows()) {
        const pairKey = this.buildPairKey(supportedAsset, supportedWindow);
        const pairPrefix = this.snapshotFieldCatalogService.readPairPrefix(supportedAsset, supportedWindow);
        const slug = this.readSnapshotString(snapshot, `${pairPrefix}_slug`);
        const cachedSlug = this.cachedSlugByPairKey.get(pairKey) || null;
        const shouldStoreMarket = slug !== null && slug !== cachedSlug;
        if (shouldStoreMarket) {
          const marketSnapshotRecord = this.buildMarketSnapshotRecord(snapshot, supportedAsset, supportedWindow);
          if (marketSnapshotRecord === null) {
            this.warnMissingMarketMetadata(supportedAsset, supportedWindow, slug);
          } else {
            await this.marketRepositoryService.ensureMarketStored(marketSnapshotRecord);
            this.cachedSlugByPairKey.set(pairKey, slug);
          }
        }
      }
    }
  }
}
