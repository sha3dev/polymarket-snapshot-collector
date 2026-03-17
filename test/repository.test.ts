import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { CommandParams, InsertParams } from "@clickhouse/client";
import type { Snapshot } from "@sha3/polymarket-snapshot";
import { ClickhouseClientService } from "../src/clickhouse/clickhouse-client.service.ts";
import { ClickhouseSchemaService } from "../src/clickhouse/clickhouse-schema.service.ts";
import { MarketSyncService } from "../src/market/market-sync.service.ts";
import type { MarketRecord } from "../src/market/market.types.ts";
import { FlatSnapshotRepositoryService } from "../src/snapshot/flat-snapshot-repository.service.ts";
import { SnapshotFieldCatalogService } from "../src/snapshot/snapshot-field-catalog.service.ts";
import { SnapshotQueryService } from "../src/snapshot/snapshot-query.service.ts";

type ClickhouseJsonResultSet = { json<T>(): Promise<T[]>; close(): void };
type ClickhouseDriver = {
  close(): Promise<void>;
  command(params: CommandParams): Promise<unknown>;
  insert<T>(params: InsertParams<unknown, T>): Promise<unknown>;
  query(params: { query: string; format: "JSONEachRow" }): Promise<ClickhouseJsonResultSet>;
};

const MARKET_RECORD: MarketRecord = {
  slug: "btc-5m",
  asset: "btc",
  window: "5m",
  priceToBeat: 100,
  marketStart: "2026-03-11T10:00:00.000Z",
  marketEnd: "2026-03-11T10:05:00.000Z",
};

const BASE_SNAPSHOT: Snapshot = {
  generated_at: 1741687200000,
  btc_binance_price: 100,
  btc_binance_order_book_json: '{"bids":[],"asks":[]}',
  btc_binance_event_ts: 1,
  btc_coinbase_price: 101,
  btc_coinbase_order_book_json: null,
  btc_coinbase_event_ts: 2,
  btc_kraken_price: 99,
  btc_kraken_order_book_json: null,
  btc_kraken_event_ts: 3,
  btc_okx_price: 102,
  btc_okx_order_book_json: null,
  btc_okx_event_ts: 4,
  btc_chainlink_price: 100,
  btc_chainlink_event_ts: 5,
  btc_5m_slug: "btc-5m",
  btc_5m_market_start: "2026-03-11T10:00:00.000Z",
  btc_5m_market_end: "2026-03-11T10:05:00.000Z",
  btc_5m_price_to_beat: 100,
  btc_5m_up_asset_id: "u1",
  btc_5m_up_price: 0.5,
  btc_5m_up_order_book_json: null,
  btc_5m_up_event_ts: 6,
  btc_5m_down_asset_id: "d1",
  btc_5m_down_price: 0.5,
  btc_5m_down_order_book_json: null,
  btc_5m_down_event_ts: 7,
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

function normalizeQuery(query: string): string {
  const normalizedQuery = query.replace(/\s+/g, " ").trim();
  return normalizedQuery;
}

function buildDriverDouble(): {
  commands: string[];
  inserts: Array<{ table: string; rows: unknown[] }>;
  queries: string[];
  queryResults: Map<string, unknown[]>;
  clickhouseDriver: ClickhouseDriver;
} {
  const commands: string[] = [];
  const inserts: Array<{ table: string; rows: unknown[] }> = [];
  const queries: string[] = [];
  const queryResults = new Map<string, unknown[]>();
  const clickhouseDriver: ClickhouseDriver = {
    async close() {},
    async command(params: CommandParams) {
      commands.push(params.query);
      return {};
    },
    async insert<T>(params: InsertParams<unknown, T>) {
      inserts.push({ table: params.table, rows: Array.isArray(params.values) ? [...params.values] : [] });
      return {};
    },
    async query(params: { query: string; format: "JSONEachRow" }) {
      queries.push(params.query);
      return {
        async json<T>() {
          const matchedEntry = [...queryResults.entries()].find(([query]) => normalizeQuery(query) === normalizeQuery(params.query));
          return (matchedEntry?.[1] || []) as T[];
        },
        close() {},
      };
    },
  };
  return { commands, inserts, queries, queryResults, clickhouseDriver };
}

test("SnapshotFieldCatalogService includes price_to_beat and market date fields", () => {
  const snapshotFieldCatalogService = SnapshotFieldCatalogService.createDefault();
  const snapshotFieldNames = snapshotFieldCatalogService.readSnapshotFieldNames();
  const snapshotColumnDefinitions = snapshotFieldCatalogService.readSnapshotColumnDefinitions();

  assert.ok(snapshotFieldNames.includes("btc_5m_market_start"));
  assert.ok(snapshotFieldNames.includes("btc_5m_price_to_beat"));
  assert.ok(snapshotFieldNames.includes("eth_15m_down_event_ts"));
  assert.ok(snapshotColumnDefinitions.includes("btc_5m_market_start Nullable(DateTime64(3, 'UTC'))"));
});

test("ClickhouseSchemaService creates market, legacy, flat snapshot, and migration tables", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({
    clickhouseDriver: driverDouble.clickhouseDriver,
    databaseName: "default",
  });
  const clickhouseSchemaService = new ClickhouseSchemaService({
    clickhouseClientService,
    snapshotFieldCatalogService: SnapshotFieldCatalogService.createDefault(),
  });

  await clickhouseSchemaService.ensureSchema();

  assert.equal(driverDouble.commands.length, 4);
  assert.match(driverDouble.commands[0] || "", /CREATE TABLE IF NOT EXISTS default\.market/);
  assert.match(driverDouble.commands[2] || "", /btc_5m_market_start Nullable\(DateTime64\(3, 'UTC'\)\)/);
});

test("FlatSnapshotRepositoryService inserts flat snapshots with id and inserted_at", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({
    clickhouseDriver: driverDouble.clickhouseDriver,
    databaseName: "default",
  });
  const flatSnapshotRepositoryService = new FlatSnapshotRepositoryService({
    clickhouseClientService,
    snapshotFieldCatalogService: SnapshotFieldCatalogService.createDefault(),
  });

  await flatSnapshotRepositoryService.insertSnapshots([buildSnapshot()]);

  const insertedRow = driverDouble.inserts[0]?.rows[0] as Record<string, unknown>;
  assert.equal(typeof insertedRow.id, "string");
  assert.equal(typeof insertedRow.inserted_at, "string");
  assert.equal(insertedRow.btc_5m_price_to_beat, 100);
  assert.equal(typeof insertedRow.btc_5m_market_start, "string");
});

test("FlatSnapshotRepositoryService filters snapshots by marketSlug", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({
    clickhouseDriver: driverDouble.clickhouseDriver,
    databaseName: "default",
  });
  const flatSnapshotRepositoryService = new FlatSnapshotRepositoryService({
    clickhouseClientService,
    snapshotFieldCatalogService: SnapshotFieldCatalogService.createDefault(),
  });

  await flatSnapshotRepositoryService.listSnapshots({
    fromDate: "2026-03-11T10:00:00.000Z",
    toDate: "2026-03-11T10:01:00.000Z",
    limit: 25,
    marketSlug: "btc-5m",
  });

  assert.match(driverDouble.queries[0] || "", /btc_5m_slug = 'btc-5m'/);
  assert.match(driverDouble.queries[0] || "", /eth_15m_slug = 'btc-5m'/);
});

test("MarketSyncService stores a new slug once and uses snapshot metadata only", async () => {
  const ensuredMarkets: unknown[] = [];
  const marketSyncService = new MarketSyncService({
    marketRepositoryService: {
      async ensureMarketStored(marketSnapshotRecord: unknown) {
        ensuredMarkets.push(marketSnapshotRecord);
        return MARKET_RECORD;
      },
    } as never,
    snapshotFieldCatalogService: SnapshotFieldCatalogService.createDefault(),
  });

  await marketSyncService.syncSnapshot(buildSnapshot());
  await marketSyncService.syncSnapshot(buildSnapshot());

  assert.equal(ensuredMarkets.length, 1);
  assert.equal((ensuredMarkets[0] as { marketStart: string }).marketStart, "2026-03-11T10:00:00.000Z");
});

test("SnapshotQueryService lists markets and reads snapshots with marketSlug", async () => {
  const capturedSnapshotReads: Array<{ fromDate: string; toDate: string; limit: number; marketSlug: string | null }> = [];
  const snapshotQueryService = new SnapshotQueryService({
    marketRepositoryService: {
      async listMarkets() {
        return [MARKET_RECORD];
      },
    } as never,
    flatSnapshotRepositoryService: {
      async listSnapshots(options: { fromDate: string; toDate: string; limit: number; marketSlug: string | null }) {
        capturedSnapshotReads.push(options);
        return [
          {
            id: "s1",
            generated_at: 1741687200000,
            inserted_at: "2026-03-11T10:00:00.100Z",
            btc_5m_slug: "btc-5m",
            btc_5m_market_start: "2026-03-11T10:00:00.000Z",
          },
        ];
      },
    } as never,
  });

  const marketListPayload = await snapshotQueryService.listMarkets({ asset: "btc", window: "5m", fromDate: null });
  const snapshotRangePayload = await snapshotQueryService.readSnapshots({
    fromDate: "2026-03-11T10:00:00.000Z",
    toDate: "2026-03-11T10:01:00.000Z",
    limit: 10,
    marketSlug: "btc-5m",
  });

  assert.equal(marketListPayload.markets[0]?.slug, "btc-5m");
  assert.deepEqual(capturedSnapshotReads, [
    {
      fromDate: "2026-03-11T10:00:00.000Z",
      toDate: "2026-03-11T10:01:00.000Z",
      limit: 10,
      marketSlug: "btc-5m",
    },
  ]);
  assert.equal(snapshotRangePayload.marketSlug, "btc-5m");
});
