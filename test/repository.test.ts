import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { OrderBookSnapshot } from "@sha3/crypto";
import type { OrderBook } from "@sha3/polymarket";
import type { Snapshot } from "@sha3/polymarket-snapshot";
import { ClickhouseClientService } from "../src/clickhouse/clickhouse-client.service.ts";
import { ClickhouseSchemaService } from "../src/clickhouse/clickhouse-schema.service.ts";
import type { ClickhouseDriver } from "../src/clickhouse/clickhouse.types.ts";
import { MarketRepositoryService } from "../src/market/market-repository.service.ts";
import type { MarketRecord } from "../src/market/market.types.ts";
import { SnapshotQueryService } from "../src/snapshot/snapshot-query.service.ts";
import { SnapshotRepositoryService } from "../src/snapshot/snapshot-repository.service.ts";

const POLYMARKET_ORDER_BOOK: OrderBook = { asks: [], bids: [] };
const PROVIDER_ORDER_BOOK: OrderBookSnapshot = { type: "orderbook", provider: "binance", symbol: "btc", ts: 1, asks: [], bids: [] };
const DASHBOARD_MARKET_RECORD: MarketRecord = { slug: "btc-5m", asset: "btc", window: "5m", priceToBeat: 100, marketId: "m1", marketConditionId: "c1", marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" };
const DASHBOARD_SNAPSHOT_ROW = {
  asset: "btc",
  window: "5m",
  market_slug: "btc-5m",
  generated_at: "2026-03-11 10:00:00.000",
  up_asset_id: "u1",
  up_price: 0.55,
  up_order_book: null,
  up_event_ts: 1,
  down_asset_id: "d1",
  down_price: 0.45,
  down_order_book: null,
  down_event_ts: 2,
  binance_price: 100,
  binance_order_book: null,
  binance_event_ts: 3,
  coinbase_price: 100,
  coinbase_order_book: null,
  coinbase_event_ts: 4,
  kraken_price: 100,
  kraken_order_book: null,
  kraken_event_ts: 5,
  okx_price: 100,
  okx_order_book: null,
  okx_event_ts: 6,
  chainlink_price: 100,
  chainlink_order_book: null,
  chainlink_event_ts: 7,
};

function buildDriverDouble() {
  const commands: string[] = [];
  const inserts: Array<{ table: string; rows: unknown[] }> = [];
  const queries: string[] = [];
  const queryResults = new Map<string, unknown[]>();
  const clickhouseDriver: ClickhouseDriver = {
    async close() {},
    async command(params) {
      commands.push(params.query);
      return {};
    },
    async insert(params) {
      inserts.push({ table: params.table, rows: Array.isArray(params.values) ? [...params.values] : [] });
      return {};
    },
    async query(params) {
      queries.push(params.query);
      return {
        async json<T>() {
          const normalizedQuery = params.query.replace(/\s+/g, " ").trim();
          const matchedEntry = [...queryResults.entries()].find(([query]) => query.replace(/\s+/g, " ").trim() === normalizedQuery);
          return (matchedEntry?.[1] || []) as T[];
        },
        close() {},
      };
    },
  };
  return { commands, inserts, queries, queryResults, clickhouseDriver };
}

test("ClickhouseSchemaService creates market and snapshot tables", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const clickhouseSchemaService = new ClickhouseSchemaService({ clickhouseClientService });

  await clickhouseSchemaService.ensureSchema();

  assert.equal(driverDouble.commands.length, 2);
  assert.match(driverDouble.commands[0] || "", /CREATE TABLE IF NOT EXISTS default\.market/);
  assert.match(driverDouble.commands[1] || "", /CREATE TABLE IF NOT EXISTS default\.snapshot/);
});

test("MarketRepositoryService inserts market only when missing", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });
  const snapshot: Snapshot = {
    asset: "btc",
    window: "5m",
    generatedAt: 1741687200000,
    marketId: "m1",
    marketSlug: "btc-5m",
    marketConditionId: "c1",
    marketStart: "2026-03-11T10:00:00.000Z",
    marketEnd: "2026-03-11T10:05:00.000Z",
    priceToBeat: 100,
    upAssetId: null,
    upPrice: null,
    upOrderBook: null,
    upEventTs: null,
    downAssetId: null,
    downPrice: null,
    downOrderBook: null,
    downEventTs: null,
    binancePrice: null,
    binanceOrderBook: null,
    binanceEventTs: null,
    coinbasePrice: null,
    coinbaseOrderBook: null,
    coinbaseEventTs: null,
    krakenPrice: null,
    krakenOrderBook: null,
    krakenEventTs: null,
    okxPrice: null,
    okxOrderBook: null,
    okxEventTs: null,
    chainlinkPrice: null,
    chainlinkOrderBook: null,
    chainlinkEventTs: null,
  };

  await marketRepositoryService.ensureMarketStored(snapshot);
  await marketRepositoryService.ensureMarketStored(snapshot);

  assert.equal(driverDouble.inserts.length, 1);
  assert.equal(driverDouble.queries.length, 1);
  assert.equal((driverDouble.inserts[0]?.rows[0] as { market_start: string }).market_start, "2026-03-11 10:00:00.000");
});

test("MarketRepositoryService backfills price_to_beat once when market already exists", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });
  const snapshot: Snapshot = {
    asset: "btc",
    window: "5m",
    generatedAt: 1741687200000,
    marketId: "m1",
    marketSlug: "btc-5m",
    marketConditionId: "c1",
    marketStart: "2026-03-11T10:00:00.000Z",
    marketEnd: "2026-03-11T10:05:00.000Z",
    priceToBeat: 100,
    upAssetId: null,
    upPrice: null,
    upOrderBook: null,
    upEventTs: null,
    downAssetId: null,
    downPrice: null,
    downOrderBook: null,
    downEventTs: null,
    binancePrice: null,
    binanceOrderBook: null,
    binanceEventTs: null,
    coinbasePrice: null,
    coinbaseOrderBook: null,
    coinbaseEventTs: null,
    krakenPrice: null,
    krakenOrderBook: null,
    krakenEventTs: null,
    okxPrice: null,
    okxOrderBook: null,
    okxEventTs: null,
    chainlinkPrice: null,
    chainlinkOrderBook: null,
    chainlinkEventTs: null,
  };
  const findMarketBySlugQuery = `
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM default.market
      WHERE slug = 'btc-5m'
      ORDER BY market_start ASC
      LIMIT 1
    `;
  driverDouble.queryResults.set(findMarketBySlugQuery, [{ slug: "btc-5m", market_id: "m1", market_condition_id: "c1", asset: "btc", window: "5m", price_to_beat: null, market_start: "2026-03-11 10:00:00.000", market_end: "2026-03-11 10:05:00.000" }]);

  await marketRepositoryService.ensureMarketStored(snapshot);
  await marketRepositoryService.ensureMarketStored(snapshot);

  assert.equal(driverDouble.inserts.length, 0);
  assert.equal(driverDouble.commands.length, 1);
  assert.match(driverDouble.commands[0] || "", /ALTER TABLE default\.market/);
  assert.match(driverDouble.commands[0] || "", /UPDATE price_to_beat = 100/);
});

test("MarketRepositoryService reads ClickHouse datetimes as UTC", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });
  const query = `
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM default.market
      WHERE slug = 'btc-5m'
      ORDER BY market_start ASC
      LIMIT 1
    `;
  driverDouble.queryResults.set(query, [{ slug: "btc-5m", market_id: "m1", market_condition_id: "c1", asset: "btc", window: "5m", price_to_beat: 100, market_start: "2026-03-11 14:10:00.000", market_end: "2026-03-11 14:15:00.000" }]);

  const marketRecord = await marketRepositoryService.findMarketBySlug("btc-5m");

  assert.equal(marketRecord?.marketStart, "2026-03-11T14:10:00.000Z");
  assert.equal(marketRecord?.marketEnd, "2026-03-11T14:15:00.000Z");
});

test("MarketRepositoryService trusts final cached markets and refreshes non-final cached markets", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });
  const finalSnapshot: Snapshot = {
    asset: "btc",
    window: "5m",
    generatedAt: 1741687200000,
    marketId: "m1",
    marketSlug: "btc-5m",
    marketConditionId: "c1",
    marketStart: "2026-03-11T10:00:00.000Z",
    marketEnd: "2026-03-11T10:05:00.000Z",
    priceToBeat: 100,
    upAssetId: null,
    upPrice: null,
    upOrderBook: null,
    upEventTs: null,
    downAssetId: null,
    downPrice: null,
    downOrderBook: null,
    downEventTs: null,
    binancePrice: null,
    binanceOrderBook: null,
    binanceEventTs: null,
    coinbasePrice: null,
    coinbaseOrderBook: null,
    coinbaseEventTs: null,
    krakenPrice: null,
    krakenOrderBook: null,
    krakenEventTs: null,
    okxPrice: null,
    okxOrderBook: null,
    okxEventTs: null,
    chainlinkPrice: null,
    chainlinkOrderBook: null,
    chainlinkEventTs: null,
  };
  const nonFinalSnapshot = { ...finalSnapshot, marketSlug: "btc-15m", window: "15m" as const, priceToBeat: null };
  const nonFinalQuery = `
      SELECT slug, market_id, market_condition_id, asset, window, price_to_beat, market_start, market_end
      FROM default.market
      WHERE slug = 'btc-15m'
      ORDER BY market_start ASC
      LIMIT 1
    `;
  driverDouble.queryResults.set(nonFinalQuery, [{ slug: "btc-15m", market_id: "m2", market_condition_id: "c2", asset: "btc", window: "15m", price_to_beat: 200, market_start: "2026-03-11 10:00:00.000", market_end: "2026-03-11 10:15:00.000" }]);

  await marketRepositoryService.ensureMarketStored(finalSnapshot);
  await marketRepositoryService.findMarketBySlug("btc-5m");
  await marketRepositoryService.ensureMarketStored(nonFinalSnapshot);
  const refreshedMarketRecord = await marketRepositoryService.findMarketBySlug("btc-15m");

  assert.equal(driverDouble.queries.length, 2);
  assert.equal(refreshedMarketRecord?.priceToBeat, 200);
});

test("SnapshotRepositoryService serializes order books on insert", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const snapshotRepositoryService = new SnapshotRepositoryService({ clickhouseClientService, maxBatchSize: 1, maxBatchWaitMs: 0 });

  await snapshotRepositoryService.insertSnapshot({
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
    upOrderBook: POLYMARKET_ORDER_BOOK,
    upEventTs: 1,
    downAssetId: "d1",
    downPrice: 0.5,
    downOrderBook: POLYMARKET_ORDER_BOOK,
    downEventTs: 2,
    binancePrice: 100,
    binanceOrderBook: PROVIDER_ORDER_BOOK,
    binanceEventTs: 3,
    coinbasePrice: 100,
    coinbaseOrderBook: { ...PROVIDER_ORDER_BOOK, provider: "coinbase", ts: 4 },
    coinbaseEventTs: 4,
    krakenPrice: 100,
    krakenOrderBook: { ...PROVIDER_ORDER_BOOK, provider: "kraken", ts: 5 },
    krakenEventTs: 5,
    okxPrice: 100,
    okxOrderBook: { ...PROVIDER_ORDER_BOOK, provider: "okx", ts: 6 },
    okxEventTs: 6,
    chainlinkPrice: 100,
    chainlinkOrderBook: { ...PROVIDER_ORDER_BOOK, provider: "chainlink", ts: 7 },
    chainlinkEventTs: 7,
  });

  const insertedRow = driverDouble.inserts[0]?.rows[0] as { up_order_book: string; binance_order_book: string };
  assert.equal(typeof insertedRow.up_order_book, "string");
  assert.equal(typeof insertedRow.binance_order_book, "string");
  assert.equal((driverDouble.inserts[0]?.rows[0] as { generated_at: string }).generated_at, "2025-03-11 10:00:00.000");
});

test("SnapshotRepositoryService batches nearby snapshot inserts into one ClickHouse insert", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const snapshotRepositoryService = new SnapshotRepositoryService({ clickhouseClientService, maxBatchSize: 2, maxBatchWaitMs: 50 });
  const baseSnapshot: Snapshot = {
    asset: "btc",
    window: "5m",
    generatedAt: 1741687200000,
    marketId: "m1",
    marketSlug: "btc-5m",
    marketConditionId: "c1",
    marketStart: "2026-03-11T10:00:00.000Z",
    marketEnd: "2026-03-11T10:05:00.000Z",
    priceToBeat: 100,
    upAssetId: null,
    upPrice: null,
    upOrderBook: null,
    upEventTs: null,
    downAssetId: null,
    downPrice: null,
    downOrderBook: null,
    downEventTs: null,
    binancePrice: null,
    binanceOrderBook: null,
    binanceEventTs: null,
    coinbasePrice: null,
    coinbaseOrderBook: null,
    coinbaseEventTs: null,
    krakenPrice: null,
    krakenOrderBook: null,
    krakenEventTs: null,
    okxPrice: null,
    okxOrderBook: null,
    okxEventTs: null,
    chainlinkPrice: null,
    chainlinkOrderBook: null,
    chainlinkEventTs: null,
  };

  const insertPromises = [snapshotRepositoryService.insertSnapshot(baseSnapshot), snapshotRepositoryService.insertSnapshot({ ...baseSnapshot, generatedAt: 1741687200500 })];
  await Promise.all(insertPromises);

  assert.equal(driverDouble.inserts.length, 1);
  assert.equal(driverDouble.inserts[0]?.rows.length, 2);
});

test("SnapshotQueryService detects duplicate canonical identities", async () => {
  const marketRepositoryService = {
    async findMarketBySlug() {
      return {
        slug: "btc-5m",
        asset: "btc",
        window: "5m",
        priceToBeat: 100,
        marketId: "m1",
        marketConditionId: "c1",
        marketStart: "2026-03-11T10:00:00.000Z",
        marketEnd: "2026-03-11T10:05:00.000Z",
      };
    },
  };
  const snapshotRepositoryService = {
    async listDuplicateSnapshotsBySlug() {
      return [{ market_slug: "btc-5m", asset: "btc", window: "5m", generated_at: "2026-03-11T10:00:00.000Z", duplicate_count: 2 }];
    },
    async listSnapshotsBySlug() {
      return [];
    },
  };
  const snapshotQueryService = new SnapshotQueryService({
    marketRepositoryService: marketRepositoryService as never,
    snapshotRepositoryService: snapshotRepositoryService as never,
    dashboardStateService: { readDashboard() { return { generatedAt: "2026-03-11T10:00:00.000Z", widgets: [] }; } } as never,
  });

  await assert.rejects(() => snapshotQueryService.readMarketSnapshots("btc-5m"), /duplicate snapshot identities/);
});

test("SnapshotQueryService uses market table priceToBeat for market summaries", async () => {
  const marketRepositoryService = { async listMarkets() { return [DASHBOARD_MARKET_RECORD]; } };
  const snapshotRepositoryService = {};
  const snapshotQueryService = new SnapshotQueryService({
    marketRepositoryService: marketRepositoryService as never,
    snapshotRepositoryService: snapshotRepositoryService as never,
    dashboardStateService: { readDashboard() { return { generatedAt: "2026-03-11T10:00:00.000Z", widgets: [] }; } } as never,
  });

  const marketListPayload = await snapshotQueryService.listMarkets({ asset: "btc", window: "5m", fromDate: null });

  assert.equal(marketListPayload.markets[0]?.priceToBeat, 100);
});

test("SnapshotQueryService reads snapshot timestamps as UTC", async () => {
  const marketRepositoryService = { async findMarketBySlug() { return DASHBOARD_MARKET_RECORD; } };
  const snapshotRepositoryService = { async listDuplicateSnapshotsBySlug() { return []; }, async listSnapshotsBySlug() { return [DASHBOARD_SNAPSHOT_ROW]; } };
  const snapshotQueryService = new SnapshotQueryService({
    marketRepositoryService: marketRepositoryService as never,
    snapshotRepositoryService: snapshotRepositoryService as never,
    dashboardStateService: { readDashboard() { return { generatedAt: "2026-03-11T10:00:00.000Z", widgets: [] }; } } as never,
  });

  const marketSnapshotsPayload = await snapshotQueryService.readMarketSnapshots("btc-5m");

  assert.equal(marketSnapshotsPayload.snapshots[0]?.generatedAt, Date.parse("2026-03-11T10:00:00.000Z"));
  assert.equal(marketSnapshotsPayload.snapshots[0]?.marketStart, "2026-03-11T10:00:00.000Z");
});
