/**
 * @section imports:externals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

/**
 * @section imports:internals
 */

import config from "../config.ts";

/**
 * @section types
 */

type SnapshotFieldCatalogServiceOptions = {
  supportedAssets: readonly CryptoSymbol[];
  supportedWindows: readonly CryptoMarketWindow[];
};

/**
 * @section class
 */

export class SnapshotFieldCatalogService {
  /**
   * @section private:attributes
   */

  private readonly supportedAssets: readonly CryptoSymbol[];
  private readonly supportedWindows: readonly CryptoMarketWindow[];

  /**
   * @section constructor
   */

  public constructor(options: SnapshotFieldCatalogServiceOptions) {
    this.supportedAssets = options.supportedAssets;
    this.supportedWindows = options.supportedWindows;
  }

  /**
   * @section factory
   */

  public static createDefault(): SnapshotFieldCatalogService {
    const snapshotFieldCatalogService = new SnapshotFieldCatalogService({
      supportedAssets: [...config.SUPPORTED_ASSETS],
      supportedWindows: [...config.SUPPORTED_WINDOWS],
    });
    return snapshotFieldCatalogService;
  }

  /**
   * @section private:methods
   */

  private buildAssetFieldNames(asset: CryptoSymbol): string[] {
    const assetFieldNames = [
      `${asset}_binance_price`,
      `${asset}_binance_order_book_json`,
      `${asset}_binance_event_ts`,
      `${asset}_coinbase_price`,
      `${asset}_coinbase_order_book_json`,
      `${asset}_coinbase_event_ts`,
      `${asset}_kraken_price`,
      `${asset}_kraken_order_book_json`,
      `${asset}_kraken_event_ts`,
      `${asset}_okx_price`,
      `${asset}_okx_order_book_json`,
      `${asset}_okx_event_ts`,
      `${asset}_chainlink_price`,
      `${asset}_chainlink_event_ts`,
    ];
    return assetFieldNames;
  }

  private buildPairFieldNames(asset: CryptoSymbol, window: CryptoMarketWindow): string[] {
    const pairPrefix = this.readPairPrefix(asset, window);
    const pairFieldNames = [
      `${pairPrefix}_slug`,
      `${pairPrefix}_market_start`,
      `${pairPrefix}_market_end`,
      `${pairPrefix}_price_to_beat`,
      `${pairPrefix}_up_asset_id`,
      `${pairPrefix}_up_price`,
      `${pairPrefix}_up_order_book_json`,
      `${pairPrefix}_up_event_ts`,
      `${pairPrefix}_down_asset_id`,
      `${pairPrefix}_down_price`,
      `${pairPrefix}_down_order_book_json`,
      `${pairPrefix}_down_event_ts`,
    ];
    return pairFieldNames;
  }

  private buildColumnDefinition(fieldName: string): string {
    let columnDefinition = `${fieldName} Nullable(String)`;
    if (fieldName.endsWith("_market_start") || fieldName.endsWith("_market_end")) {
      columnDefinition = `${fieldName} Nullable(DateTime64(3, 'UTC'))`;
    }
    if (fieldName.endsWith("_price") || fieldName.endsWith("_price_to_beat")) {
      columnDefinition = `${fieldName} Nullable(Float64)`;
    }
    if (fieldName.endsWith("_event_ts")) {
      columnDefinition = `${fieldName} Nullable(Int64)`;
    }
    return columnDefinition;
  }

  /**
   * @section public:methods
   */

  public readPairPrefix(asset: CryptoSymbol, window: CryptoMarketWindow): string {
    const pairPrefix = `${asset}_${window}`;
    return pairPrefix;
  }

  public readSnapshotFieldNames(): string[] {
    const snapshotFieldNames: string[] = [];
    for (const supportedAsset of this.supportedAssets) {
      snapshotFieldNames.push(...this.buildAssetFieldNames(supportedAsset));
      for (const supportedWindow of this.supportedWindows) {
        snapshotFieldNames.push(...this.buildPairFieldNames(supportedAsset, supportedWindow));
      }
    }
    return snapshotFieldNames;
  }

  public readSnapshotColumnDefinitions(): string[] {
    const snapshotColumnDefinitions = this.readSnapshotFieldNames().map((fieldName) => this.buildColumnDefinition(fieldName));
    return snapshotColumnDefinitions;
  }

  public readSlugFieldNames(): string[] {
    const slugFieldNames: string[] = [];
    for (const supportedAsset of this.supportedAssets) {
      for (const supportedWindow of this.supportedWindows) {
        slugFieldNames.push(`${this.readPairPrefix(supportedAsset, supportedWindow)}_slug`);
      }
    }
    return slugFieldNames;
  }

  public readSupportedAssets(): readonly CryptoSymbol[] {
    return this.supportedAssets;
  }

  public readSupportedWindows(): readonly CryptoMarketWindow[] {
    return this.supportedWindows;
  }
}
