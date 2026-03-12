import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { Snapshot } from "@sha3/polymarket-snapshot";
import type { MarketRecord } from "../src/market/market.types.ts";
import { StateStoreService } from "../src/snapshot/state-store.service.ts";

const BASE_MARKET_RECORD: MarketRecord = {
  slug: "btc-5m",
  asset: "btc",
  window: "5m",
  priceToBeat: 100,
  marketId: "m1",
  marketConditionId: "c1",
  marketStart: "2026-03-11T10:00:00.000Z",
  marketEnd: "2026-03-11T10:05:00.000Z",
};

const BASE_SNAPSHOT: Snapshot = {
  asset: "btc",
  window: "5m",
  generatedAt: Date.parse("2026-03-11T10:00:00.000Z"),
  marketId: "m1",
  marketSlug: "btc-5m",
  marketConditionId: "c1",
  marketStart: "2026-03-11T10:00:00.000Z",
  marketEnd: "2026-03-11T10:05:00.000Z",
  priceToBeat: 100,
  upAssetId: "u1",
  upPrice: 0.55,
  upOrderBook: null,
  upEventTs: 1,
  downAssetId: "d1",
  downPrice: 0.45,
  downOrderBook: null,
  downEventTs: 2,
  binancePrice: 100,
  binanceOrderBook: null,
  binanceEventTs: 3,
  coinbasePrice: 100,
  coinbaseOrderBook: null,
  coinbaseEventTs: 4,
  krakenPrice: 100,
  krakenOrderBook: null,
  krakenEventTs: 5,
  okxPrice: 100,
  okxOrderBook: null,
  okxEventTs: 6,
  chainlinkPrice: 101,
  chainlinkOrderBook: null,
  chainlinkEventTs: 7,
};

test("StateStoreService serves memory-backed state in stable order", () => {
  const realNow = Date.now;
  const supportedAssets = ["btc", "eth", "sol", "xrp"] as const;
  const supportedWindows = ["5m", "15m"] as const;
  const stateStoreService = new StateStoreService({ staleAfterMs: 10000, supportedAssets, supportedWindows });
  try {
    Date.now = () => Date.parse("2026-03-11T10:00:08.000Z");
    stateStoreService.updateSnapshot(BASE_MARKET_RECORD, BASE_SNAPSHOT);

    const statePayload = stateStoreService.readState();

    assert.equal(statePayload.markets.length, 8);
    assert.equal(statePayload.markets[0]?.asset, "btc");
    assert.equal(statePayload.markets[0]?.window, "5m");
    assert.equal(statePayload.markets[0]?.latestSnapshot?.upPrice, 0.55);
    assert.equal(statePayload.markets[0]?.latestSnapshot?.downPrice, 0.45);
    assert.equal(statePayload.markets[0]?.marketDirection, "UP");
    assert.equal(statePayload.markets[0]?.latestSnapshotAgeMs, 8000);
    assert.equal(statePayload.markets[0]?.isStale, false);
    assert.equal(statePayload.markets[1]?.market, null);
  } finally {
    Date.now = realNow;
  }
});

test("StateStoreService resets snapshot count when market changes within an entry", () => {
  const supportedAssets = ["btc", "eth", "sol", "xrp"] as const;
  const supportedWindows = ["5m", "15m"] as const;
  const stateStoreService = new StateStoreService({ staleAfterMs: 10000, supportedAssets, supportedWindows });

  stateStoreService.updateSnapshot(BASE_MARKET_RECORD, BASE_SNAPSHOT);
  stateStoreService.updateSnapshot(
    { ...BASE_MARKET_RECORD, slug: "btc-5m-next", marketStart: "2026-03-11T10:05:00.000Z", marketEnd: "2026-03-11T10:10:00.000Z" },
    { ...BASE_SNAPSHOT, marketSlug: "btc-5m-next" },
  );

  const statePayload = stateStoreService.readState();

  assert.equal(statePayload.markets[0]?.market?.slug, "btc-5m-next");
  assert.equal(statePayload.markets[0]?.snapshotCount, 1);
});
