import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { OrderBookSnapshot } from "@sha3/crypto";
import type { OrderBook } from "@sha3/polymarket";
import type { Snapshot } from "@sha3/polymarket-snapshot";
import { ClickhouseClientService } from "../src/clickhouse/clickhouse-client.service.ts";
import { ClickhouseSchemaService } from "../src/clickhouse/clickhouse-schema.service.ts";
import type { ClickhouseDriver } from "../src/clickhouse/clickhouse.types.ts";
import { MarketRepositoryService } from "../src/market/market-repository.service.ts";
import { SnapshotQueryService } from "../src/snapshot/snapshot-query.service.ts";
import { SnapshotRepositoryService } from "../src/snapshot/snapshot-repository.service.ts";

const POLYMARKET_ORDER_BOOK: OrderBook = { asks: [], bids: [] };
const PROVIDER_ORDER_BOOK: OrderBookSnapshot = { type: "orderbook", provider: "binance", symbol: "btc", ts: 1, asks: [], bids: [] };
const DASHBOARD_MARKET_RECORD = { slug: "btc-5m", asset: "btc", window: "5m", priceToBeat: 100, marketId: "m1", marketConditionId: "c1", marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" };
const DASHBOARD_SNAPSHOT_ROW = {
  asset: "btc",
  window: "5m",
  market_slug: "btc-5m",
  generated_at: "2026-03-11 10:00:00.000",
  market_id: "m1",
  market_condition_id: "c1",
  market_start: "2026-03-11 10:00:00.000",
  market_end: "2026-03-11 10:05:00.000",
  price_to_beat: 100,
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
      return { async json<T>() { return (queryResults.get(params.query) || []) as T[]; }, close() {} };
    },
  };
  return { commands, inserts, queries, queryResults, clickhouseDriver };
}

function buildDashboardMarketRepositoryDouble() {
  return {
    async findMarketBySlug(slug: string) {
      return { ...DASHBOARD_MARKET_RECORD, slug };
    },
    async listMarkets(options: { asset: string; window: string }) {
      return options.asset === "btc" && options.window === "5m" ? [DASHBOARD_MARKET_RECORD] : [];
    },
  };
}

function buildDashboardSnapshotRepositoryDouble() {
  return { async listDuplicateSnapshotsBySlug() { return []; }, async listSnapshotsBySlug() { return [DASHBOARD_SNAPSHOT_ROW]; } };
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

  assert.equal(driverDouble.inserts.length, 1);
  assert.equal((driverDouble.inserts[0]?.rows[0] as { market_start: string }).market_start, "2026-03-11 10:00:00.000");
});

test("MarketRepositoryService quotes slug literals for ClickHouse SQL", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });

  await marketRepositoryService.findMarketBySlug("btc-updown-5m-1773233400");

  assert.match(driverDouble.queries[0] || "", /WHERE slug = 'btc-updown-5m-1773233400'/);
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

test("SnapshotRepositoryService serializes order books on insert", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const snapshotRepositoryService = new SnapshotRepositoryService({ clickhouseClientService });

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
  const snapshotQueryService = new SnapshotQueryService({ marketRepositoryService: marketRepositoryService as never, snapshotRepositoryService: snapshotRepositoryService as never });

  await assert.rejects(() => snapshotQueryService.readMarketSnapshots("btc-5m"), /duplicate snapshot identities/);
});

test("SnapshotQueryService builds dashboard widgets with up and down prices", async () => {
  const marketRepositoryService = buildDashboardMarketRepositoryDouble();
  const snapshotRepositoryService = buildDashboardSnapshotRepositoryDouble();
  const snapshotQueryService = new SnapshotQueryService({ marketRepositoryService: marketRepositoryService as never, snapshotRepositoryService: snapshotRepositoryService as never });

  const dashboardPayload = await snapshotQueryService.readDashboard();

  assert.equal(dashboardPayload.widgets.length > 0, true);
  assert.equal(dashboardPayload.widgets[0]?.latestSnapshot?.upPrice, 0.55);
  assert.equal(dashboardPayload.widgets[0]?.latestSnapshot?.downPrice, 0.45);
  assert.equal(dashboardPayload.widgets[0]?.marketDirection, "UP");
});

test("SnapshotQueryService does not mark recent dashboard widgets as stale too aggressively", async () => {
  const realNow = Date.now;
  try {
    Date.now = () => Date.parse("2026-03-11T10:00:08.000Z");
    const marketRepositoryService = buildDashboardMarketRepositoryDouble();
    const snapshotRepositoryService = buildDashboardSnapshotRepositoryDouble();
    const snapshotQueryService = new SnapshotQueryService({ marketRepositoryService: marketRepositoryService as never, snapshotRepositoryService: snapshotRepositoryService as never });

    const dashboardPayload = await snapshotQueryService.readDashboard();

    assert.equal(dashboardPayload.widgets[0]?.latestSnapshotAgeMs, 8000);
    assert.equal(dashboardPayload.widgets[0]?.isStale, false);
  } finally {
    Date.now = realNow;
  }
});

test("SnapshotQueryService reads snapshot timestamps as UTC", async () => {
  const marketRepositoryService = { async findMarketBySlug() { return DASHBOARD_MARKET_RECORD; } };
  const snapshotRepositoryService = { async listDuplicateSnapshotsBySlug() { return []; }, async listSnapshotsBySlug() { return [DASHBOARD_SNAPSHOT_ROW]; } };
  const snapshotQueryService = new SnapshotQueryService({ marketRepositoryService: marketRepositoryService as never, snapshotRepositoryService: snapshotRepositoryService as never });

  const marketSnapshotsPayload = await snapshotQueryService.readMarketSnapshots("btc-5m");

  assert.equal(marketSnapshotsPayload.snapshots[0]?.generatedAt, Date.parse("2026-03-11T10:00:00.000Z"));
  assert.equal(marketSnapshotsPayload.snapshots[0]?.marketStart, "2026-03-11T10:00:00.000Z");
});
