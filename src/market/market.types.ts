/**
 * @section imports:externals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

/**
 * @section types
 */

export type MarketRecord = {
  slug: string;
  asset: CryptoSymbol;
  window: CryptoMarketWindow;
  priceToBeat: number | null;
  marketStart: string;
  marketEnd: string;
};

export type MarketLookupOptions = {
  asset: CryptoSymbol | null;
  window: CryptoMarketWindow | null;
  fromDate: string | null;
};

export type MarketSnapshotRecord = {
  slug: string;
  asset: CryptoSymbol;
  window: CryptoMarketWindow;
  priceToBeat: number | null;
  marketStart: string;
  marketEnd: string;
};
