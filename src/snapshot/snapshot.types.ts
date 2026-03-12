/**
 * @section imports:externals
 */

import type { Snapshot, SnapshotAsset, SnapshotWindow } from "@sha3/polymarket-snapshot";

/**
 * @section types
 */

export type MarketSummary = {
  slug: string;
  window: SnapshotWindow;
  asset: SnapshotAsset;
  priceToBeat: number | null;
  marketStart: string;
  marketEnd: string;
  prevPriceToBeat: number[];
};

export type MarketListPayload = { markets: MarketSummary[] };

export type MarketSnapshotsPayload = {
  slug: string;
  asset: SnapshotAsset;
  window: SnapshotWindow;
  marketStart: string;
  marketEnd: string;
  snapshots: Snapshot[];
};

export type StateMarketSnapshotSummary = {
  generatedAt: number;
  priceToBeat: number | null;
  upPrice: number | null;
  downPrice: number | null;
  chainlinkPrice: number | null;
  binancePrice: number | null;
  coinbasePrice: number | null;
  krakenPrice: number | null;
  okxPrice: number | null;
};

export type StateMarketDirection = "UP" | "DOWN" | "UNKNOWN";

export type StateMarketEntry = {
  asset: SnapshotAsset;
  window: SnapshotWindow;
  market: MarketSummary | null;
  snapshotCount: number;
  latestSnapshot: StateMarketSnapshotSummary | null;
  marketDirection: StateMarketDirection;
  latestSnapshotAgeMs: number | null;
  isStale: boolean;
};

export type StatePayload = {
  generatedAt: string;
  markets: StateMarketEntry[];
};

export type SnapshotStorageRow = {
  asset: string;
  window: string;
  market_slug: string;
  generated_at: string;
  up_asset_id: string | null;
  up_price: number | null;
  up_order_book: string | null;
  up_event_ts: number | null;
  down_asset_id: string | null;
  down_price: number | null;
  down_order_book: string | null;
  down_event_ts: number | null;
  binance_price: number | null;
  binance_order_book: string | null;
  binance_event_ts: number | null;
  coinbase_price: number | null;
  coinbase_order_book: string | null;
  coinbase_event_ts: number | null;
  kraken_price: number | null;
  kraken_order_book: string | null;
  kraken_event_ts: number | null;
  okx_price: number | null;
  okx_order_book: string | null;
  okx_event_ts: number | null;
  chainlink_price: number | null;
  chainlink_order_book: string | null;
  chainlink_event_ts: number | null;
};

export type SnapshotDuplicateRow = {
  market_slug: string;
  asset: string;
  window: string;
  generated_at: string;
  duplicate_count: number;
};
