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
});

test("MarketRepositoryService quotes slug literals for ClickHouse SQL", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({ clickhouseDriver: driverDouble.clickhouseDriver, databaseName: "default" });
  const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });

  await marketRepositoryService.findMarketBySlug("btc-updown-5m-1773233400");

  assert.match(driverDouble.queries[0] || "", /WHERE slug = 'btc-updown-5m-1773233400'/);
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
