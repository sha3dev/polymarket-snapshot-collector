import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { Snapshot } from "@sha3/polymarket-snapshot";
import { SnapshotCollectorService } from "../src/collector/snapshot-collector.service.ts";

const BASE_SNAPSHOT: Snapshot = {
  generated_at: Date.parse("2026-03-11T10:00:00.000Z"),
  btc_binance_price: 100,
  btc_binance_order_book_json: null,
  btc_binance_event_ts: 1,
  btc_5m_slug: "btc-5m",
  btc_5m_market_start: "2026-03-11T10:00:00.000Z",
  btc_5m_market_end: "2026-03-11T10:05:00.000Z",
  btc_5m_price_to_beat: 100,
  btc_5m_up_asset_id: "u1",
  btc_5m_up_price: 0.5,
  btc_5m_up_order_book_json: null,
  btc_5m_up_event_ts: 2,
  btc_5m_down_asset_id: "d1",
  btc_5m_down_price: 0.5,
  btc_5m_down_order_book_json: null,
  btc_5m_down_event_ts: 3,
};

function buildSnapshot(overrides: Partial<Snapshot> = {}): Snapshot {
  const snapshot: Snapshot = { ...BASE_SNAPSHOT };
  for (const [fieldName, fieldValue] of Object.entries(overrides)) {
    if (fieldValue !== undefined) {
      snapshot[fieldName] = fieldValue;
    }
  }
  return snapshot;
}

test("SnapshotCollectorService persists batched snapshots and syncs markets", async () => {
  const storedBatchSizes: number[] = [];
  const syncedGeneratedAts: number[] = [];
  let listener = (_snapshot: Snapshot): void => {
    throw new Error("snapshot listener was not registered");
  };
  const snapshotCollectorService = new SnapshotCollectorService({
    flatSnapshotRepositoryService: {
      async insertSnapshots(snapshots: readonly Snapshot[]) {
        storedBatchSizes.push(snapshots.length);
      },
    } as never,
    marketSyncService: {
      async syncSnapshot(snapshot: Snapshot) {
        syncedGeneratedAts.push(snapshot.generated_at);
      },
    } as never,
    snapshotRuntime: {
      addSnapshotListener(options: { listener: (snapshot: Snapshot) => void }) {
        listener = options.listener;
      },
      removeSnapshotListener() {},
      async disconnect() {},
    },
  });

  snapshotCollectorService.start();
  listener(buildSnapshot());
  listener(buildSnapshot({ generated_at: Date.parse("2026-03-11T10:00:00.500Z") }));
  await snapshotCollectorService.stop();

  assert.deepEqual(storedBatchSizes, [2]);
  assert.deepEqual(syncedGeneratedAts, [Date.parse("2026-03-11T10:00:00.000Z"), Date.parse("2026-03-11T10:00:00.500Z")]);
});

test("SnapshotCollectorService continues after market sync errors", async () => {
  const storedBatchSizes: number[] = [];
  let listener = (_snapshot: Snapshot): void => {
    throw new Error("snapshot listener was not registered");
  };
  const snapshotCollectorService = new SnapshotCollectorService({
    flatSnapshotRepositoryService: {
      async insertSnapshots(snapshots: readonly Snapshot[]) {
        storedBatchSizes.push(snapshots.length);
      },
    } as never,
    marketSyncService: {
      async syncSnapshot() {
        throw new Error("market sync failed");
      },
    } as never,
    snapshotRuntime: {
      addSnapshotListener(options: { listener: (snapshot: Snapshot) => void }) {
        listener = options.listener;
      },
      removeSnapshotListener() {},
      async disconnect() {},
    },
  });

  snapshotCollectorService.start();
  listener(buildSnapshot());
  await snapshotCollectorService.stop();

  assert.deepEqual(storedBatchSizes, [1]);
});
