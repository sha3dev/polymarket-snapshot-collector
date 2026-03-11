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

export type SnapshotIdentity = { marketSlug: string; asset: SnapshotAsset; window: SnapshotWindow; generatedAt: number };

export type SnapshotFingerprintEntry = { fingerprint: string; storedAt: number };

export type SnapshotStorageRow = {
  asset: string;
  window: string;
  market_slug: string;
  generated_at: string;
  market_id: string | null;
  market_condition_id: string | null;
  market_start: string;
  market_end: string;
  price_to_beat: number | null;
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
