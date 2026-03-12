import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { Snapshot } from "@sha3/polymarket-snapshot";
import { SnapshotCollectorService } from "../src/collector/snapshot-collector.service.ts";
import { StateStoreService } from "../src/snapshot/state-store.service.ts";

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
  upPrice: 0.5,
  upOrderBook: null,
  upEventTs: 1,
  downAssetId: "d1",
  downPrice: 0.5,
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
  chainlinkPrice: 100,
  chainlinkOrderBook: null,
  chainlinkEventTs: 7,
};

function buildSnapshot(overrides: Partial<Snapshot> = {}): Snapshot {
  return { ...BASE_SNAPSHOT, ...overrides };
}

test("SnapshotCollectorService subscribes once and persists complete snapshots", async () => {
  const addedListeners: unknown[] = [];
  const storedMarkets: string[] = [];
  const storedSnapshotBatches: string[][] = [];
  const stateStoreService = new StateStoreService({ staleAfterMs: 10000, supportedAssets: ["btc", "eth", "sol", "xrp"], supportedWindows: ["5m", "15m"] });
  let listener: ((snapshot: Snapshot) => void) | null = null;
  const snapshotCollectorService = new SnapshotCollectorService({
    marketRepositoryService: {
      async ensureMarketStored(snapshot: { marketSlug: string | null; asset: "btc"; window: "5m" }) {
        storedMarkets.push(snapshot.marketSlug || "");
        return { slug: snapshot.marketSlug || "", asset: snapshot.asset, window: snapshot.window, priceToBeat: 100, prevPriceToBeat: [99, 98], marketId: "m1", marketConditionId: "c1", marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" };
      },
    } as never,
    snapshotRepositoryService: { async insertSnapshots(snapshots: Array<{ marketSlug: string | null }>) { storedSnapshotBatches.push(snapshots.map((snapshot) => snapshot.marketSlug || "")); } } as never,
    stateStoreService,
    snapshotRuntime: {
      addSnapshotListener(options: { listener: (snapshot: Snapshot) => void }) {
        addedListeners.push(options);
        listener = options.listener;
      },
      removeSnapshotListener() {},
      async disconnect() {},
    },
  });

  snapshotCollectorService.start();
  snapshotCollectorService.start();
  const currentListener = listener as unknown as (snapshot: Snapshot) => void;
  currentListener(buildSnapshot());
  await Promise.resolve();
  await Promise.resolve();
  await snapshotCollectorService.stop();

  assert.equal(addedListeners.length, 1);
  assert.deepEqual(storedMarkets, ["btc-5m"]);
  assert.deepEqual(storedSnapshotBatches, [["btc-5m"]]);
  assert.equal(stateStoreService.readState().markets[0]?.snapshotCount, 1);
});

test("SnapshotCollectorService skips incomplete snapshots", async () => {
  const storedSnapshotBatches: string[][] = [];
  const stateStoreService = new StateStoreService({ staleAfterMs: 10000, supportedAssets: ["btc", "eth", "sol", "xrp"], supportedWindows: ["5m", "15m"] });
  let listener: ((snapshot: Snapshot) => void) | null = null;
  const snapshotCollectorService = new SnapshotCollectorService({
    marketRepositoryService: { async ensureMarketStored() { return { slug: "unused", asset: "btc", window: "5m", priceToBeat: null, prevPriceToBeat: [], marketId: null, marketConditionId: null, marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" }; } } as never,
    snapshotRepositoryService: { async insertSnapshots(snapshots: Array<{ marketSlug: string | null }>) { storedSnapshotBatches.push(snapshots.map((snapshot) => snapshot.marketSlug || "")); } } as never,
    stateStoreService,
    snapshotRuntime: {
      addSnapshotListener(options: { listener: (snapshot: Snapshot) => void }) {
        listener = options.listener;
      },
      removeSnapshotListener() {},
      async disconnect() {},
    },
  });

  snapshotCollectorService.start();
  const currentListener = listener as unknown as (snapshot: Snapshot) => void;
  currentListener(buildSnapshot({ marketSlug: null }));
  await Promise.resolve();
  await Promise.resolve();
  await snapshotCollectorService.stop();

  assert.deepEqual(storedSnapshotBatches, []);
});

test("SnapshotCollectorService updates dashboard before batch insert finishes", async () => {
  const stateStoreService = new StateStoreService({ staleAfterMs: 10000, supportedAssets: ["btc", "eth", "sol", "xrp"], supportedWindows: ["5m", "15m"] });
  let listener: ((snapshot: Snapshot) => void) | null = null;
  let resolveInsert: (() => void) | undefined;
  const insertPromise = new Promise<void>((resolve) => { resolveInsert = resolve; });
  const snapshotCollectorService = new SnapshotCollectorService({
    marketRepositoryService: {
      async ensureMarketStored(snapshot: { marketSlug: string | null; asset: "btc"; window: "5m" }) {
        return { slug: snapshot.marketSlug || "", asset: snapshot.asset, window: snapshot.window, priceToBeat: 100, prevPriceToBeat: [99, 98], marketId: "m1", marketConditionId: "c1", marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" };
      },
    } as never,
    snapshotRepositoryService: { async insertSnapshots() { await insertPromise; } } as never,
    stateStoreService,
    snapshotRuntime: {
      addSnapshotListener(options: { listener: (snapshot: Snapshot) => void }) {
        listener = options.listener;
      },
      removeSnapshotListener() {},
      async disconnect() {},
    },
  });

  snapshotCollectorService.start();
  const currentListener = listener as unknown as (snapshot: Snapshot) => void;
  currentListener(buildSnapshot());
  await Promise.resolve();
  await Promise.resolve();
  await new Promise<void>((resolve) => {
    setImmediate(resolve);
  });

  assert.equal(stateStoreService.readState().markets[0]?.snapshotCount, 1);

  if (resolveInsert) {
    resolveInsert();
  }
  await snapshotCollectorService.stop();
});

test("SnapshotCollectorService skips snapshots at the market end timestamp", async () => {
  const storedSnapshotBatches: string[][] = [];
  const storedMarkets: string[] = [];
  const stateStoreService = new StateStoreService({ staleAfterMs: 10000, supportedAssets: ["btc", "eth", "sol", "xrp"], supportedWindows: ["5m", "15m"] });
  let listener: ((snapshot: Snapshot) => void) | null = null;
  const snapshotCollectorService = new SnapshotCollectorService({
    marketRepositoryService: {
      async ensureMarketStored(snapshot: { marketSlug: string | null; asset: "btc"; window: "5m" }) {
        storedMarkets.push(snapshot.marketSlug || "");
        return { slug: snapshot.marketSlug || "", asset: snapshot.asset, window: snapshot.window, priceToBeat: 100, prevPriceToBeat: [99, 98], marketId: "m1", marketConditionId: "c1", marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" };
      },
    } as never,
    snapshotRepositoryService: {
      async insertSnapshots(snapshots: Array<{ marketSlug: string | null }>) {
        storedSnapshotBatches.push(snapshots.map((snapshot) => snapshot.marketSlug || ""));
      },
    } as never,
    stateStoreService,
    snapshotRuntime: {
      addSnapshotListener(options: { listener: (snapshot: Snapshot) => void }) {
        listener = options.listener;
      },
      removeSnapshotListener() {},
      async disconnect() {},
    },
  });

  snapshotCollectorService.start();
  const currentListener = listener as unknown as (snapshot: Snapshot) => void;
  currentListener(buildSnapshot({ generatedAt: Date.parse("2026-03-11T10:05:00.000Z") }));
  await Promise.resolve();
  await Promise.resolve();
  await snapshotCollectorService.stop();

  assert.deepEqual(storedMarkets, ["btc-5m"]);
  assert.deepEqual(storedSnapshotBatches, []);
  assert.equal(stateStoreService.readState().markets[0]?.snapshotCount, 0);
});
