import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { Snapshot } from "@sha3/polymarket-snapshot";
import type { MarketRecord } from "../src/market/market.types.ts";
import { DashboardStateService } from "../src/snapshot/dashboard-state.service.ts";

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

test("DashboardStateService serves dashboard widgets from memory", () => {
  const realNow = Date.now;
  const supportedAssets = ["btc", "eth", "sol", "xrp"] as const;
  const supportedWindows = ["5m", "15m"] as const;
  const dashboardStateService = new DashboardStateService({ staleAfterMs: 10000, supportedAssets, supportedWindows });
  try {
    Date.now = () => Date.parse("2026-03-11T10:00:08.000Z");
    dashboardStateService.updateSnapshot(BASE_MARKET_RECORD, BASE_SNAPSHOT);

    const dashboardPayload = dashboardStateService.readDashboard();

    assert.equal(dashboardPayload.widgets[0]?.latestSnapshot?.upPrice, 0.55);
    assert.equal(dashboardPayload.widgets[0]?.latestSnapshot?.downPrice, 0.45);
    assert.equal(dashboardPayload.widgets[0]?.marketDirection, "UP");
    assert.equal(dashboardPayload.widgets[0]?.latestSnapshotAgeMs, 8000);
    assert.equal(dashboardPayload.widgets[0]?.isStale, false);
  } finally {
    Date.now = realNow;
  }
});

test("DashboardStateService resets snapshot count when market changes within a widget", () => {
  const supportedAssets = ["btc", "eth", "sol", "xrp"] as const;
  const supportedWindows = ["5m", "15m"] as const;
  const dashboardStateService = new DashboardStateService({ staleAfterMs: 10000, supportedAssets, supportedWindows });

  dashboardStateService.updateSnapshot(BASE_MARKET_RECORD, BASE_SNAPSHOT);
  dashboardStateService.updateSnapshot(
    { ...BASE_MARKET_RECORD, slug: "btc-5m-next", marketStart: "2026-03-11T10:05:00.000Z", marketEnd: "2026-03-11T10:10:00.000Z" },
    { ...BASE_SNAPSHOT, marketSlug: "btc-5m-next" },
  );

  const dashboardPayload = dashboardStateService.readDashboard();

  assert.equal(dashboardPayload.widgets[0]?.market?.slug, "btc-5m-next");
  assert.equal(dashboardPayload.widgets[0]?.snapshotCount, 1);
});
