import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { Snapshot } from "@sha3/polymarket-snapshot";
import { SnapshotCollectorService } from "../src/collector/snapshot-collector.service.ts";
import { SnapshotDeduplicationService } from "../src/snapshot/snapshot-deduplication.service.ts";

const BASE_SNAPSHOT: Snapshot = {
  asset: "btc",
  window: "5m",
  generatedAt: 1741687200000,
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

test("SnapshotDeduplicationService skips identical duplicate replays", () => {
  const snapshotDeduplicationService = new SnapshotDeduplicationService({ ttlMs: 120000, maxKeys: 5 });
  const snapshot = buildSnapshot();

  assert.equal(snapshotDeduplicationService.shouldPersist(snapshot), true);
  assert.equal(snapshotDeduplicationService.shouldPersist(snapshot), false);
});

test("SnapshotDeduplicationService rejects duplicate replays with different payloads", () => {
  const snapshotDeduplicationService = new SnapshotDeduplicationService({ ttlMs: 120000, maxKeys: 5 });

  snapshotDeduplicationService.shouldPersist(buildSnapshot());

  assert.throws(() => {
    snapshotDeduplicationService.shouldPersist(buildSnapshot({ binancePrice: 101 }));
  }, /duplicate snapshot identity/);
});

test("SnapshotDeduplicationService keeps 5m and 15m snapshots separate", () => {
  const snapshotDeduplicationService = new SnapshotDeduplicationService({ ttlMs: 120000, maxKeys: 5 });

  assert.equal(snapshotDeduplicationService.shouldPersist(buildSnapshot({ window: "5m" })), true);
  assert.equal(snapshotDeduplicationService.shouldPersist(buildSnapshot({ window: "15m" })), true);
});

test("SnapshotCollectorService subscribes once and persists complete snapshots", async () => {
  const addedListeners: unknown[] = [];
  const storedMarkets: string[] = [];
  const storedSnapshots: string[] = [];
  let listener: ((snapshot: Snapshot) => void) | null = null;
  const snapshotCollectorService = new SnapshotCollectorService({
    marketRepositoryService: { async ensureMarketStored(snapshot: { marketSlug: string | null }) { storedMarkets.push(snapshot.marketSlug || ""); } } as never,
    snapshotRepositoryService: { async insertSnapshot(snapshot: { marketSlug: string | null }) { storedSnapshots.push(snapshot.marketSlug || ""); } } as never,
    snapshotDeduplicationService: new SnapshotDeduplicationService({ ttlMs: 120000, maxKeys: 5 }),
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

  assert.equal(addedListeners.length, 1);
  assert.deepEqual(storedMarkets, ["btc-5m"]);
  assert.deepEqual(storedSnapshots, ["btc-5m"]);
});

test("SnapshotCollectorService skips incomplete snapshots", async () => {
  const storedSnapshots: string[] = [];
  let listener: ((snapshot: Snapshot) => void) | null = null;
  const snapshotCollectorService = new SnapshotCollectorService({
    marketRepositoryService: { async ensureMarketStored() {} } as never,
    snapshotRepositoryService: { async insertSnapshot(snapshot: { marketSlug: string | null }) { storedSnapshots.push(snapshot.marketSlug || ""); } } as never,
    snapshotDeduplicationService: new SnapshotDeduplicationService({ ttlMs: 120000, maxKeys: 5 }),
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

  assert.deepEqual(storedSnapshots, []);
});
