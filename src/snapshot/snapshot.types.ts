/**
 * @section imports:externals
 */

import type { CryptoMarketWindow, CryptoSymbol } from "@sha3/polymarket";

/**
 * @section types
 */

export type SnapshotFieldValue = number | string | null;

export type StoredSnapshot = {
  id: string;
  generated_at: number;
  inserted_at: string;
} & Record<string, SnapshotFieldValue>;

export type SnapshotRangePayload = {
  fromDate: string | null;
  toDate: string;
  marketSlug: string | null;
  snapshots: StoredSnapshot[];
};

export type MarketSummary = {
  slug: string;
  asset: CryptoSymbol;
  window: CryptoMarketWindow;
  priceToBeat: number | null;
  marketStart: string;
  marketEnd: string;
};

export type MarketListPayload = { markets: MarketSummary[] };
