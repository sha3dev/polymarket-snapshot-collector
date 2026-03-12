/**
 * @section imports:externals
 */

import type { SnapshotAsset, SnapshotWindow } from "@sha3/polymarket-snapshot";

/**
 * @section types
 */

export type MarketRecord = {
  slug: string;
  marketId: string | null;
  marketConditionId: string | null;
  asset: SnapshotAsset;
  window: SnapshotWindow;
  priceToBeat: number | null;
  marketStart: string;
  marketEnd: string;
};

export type MarketLookupOptions = { asset: SnapshotAsset | null; window: SnapshotWindow | null; fromDate: string | null };
