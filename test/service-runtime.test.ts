import * as assert from "node:assert/strict";
import { once } from "node:events";
import { test } from "node:test";

import { AppInfoService } from "../src/app-info/app-info.service.ts";
import { HttpServerService } from "../src/http/http-server.service.ts";
import { MarketNotFoundService } from "../src/snapshot/market-not-found.service.ts";
import { SnapshotConsistencyService } from "../src/snapshot/snapshot-consistency.service.ts";
import type { MarketListPayload, MarketSnapshotsPayload } from "../src/snapshot/snapshot.types.ts";

type QueryServiceDouble = {
  listMarkets(options: { asset: string; window: string; fromDate: string | null }): Promise<MarketListPayload>;
  readMarketSnapshots(slug: string): Promise<MarketSnapshotsPayload>;
};

const BASE_MARKET_SNAPSHOTS_PAYLOAD: MarketSnapshotsPayload = {
  slug: "btc-5m",
  asset: "btc",
  window: "5m",
  marketStart: "2026-03-11T10:00:00.000Z",
  marketEnd: "2026-03-11T10:05:00.000Z",
  snapshots: [
    {
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
    },
  ],
};

function buildHttpServerService(queryServiceDouble: QueryServiceDouble): HttpServerService {
  return new HttpServerService({ appInfoService: new AppInfoService({ serviceName: "@sha3/polymarket-snapshot-collector" }), snapshotQueryService: queryServiceDouble as unknown as never });
}

function buildSnapshotPayload(slug: string): MarketSnapshotsPayload {
  const snapshot = BASE_MARKET_SNAPSHOTS_PAYLOAD.snapshots[0];
  if (!snapshot) {
    throw new Error("missing base snapshot");
  }
  return { ...BASE_MARKET_SNAPSHOTS_PAYLOAD, slug, snapshots: [{ ...snapshot, marketSlug: slug }] };
}

async function listen(server: ReturnType<HttpServerService["buildServer"]>): Promise<number> {
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("failed to bind test server");
  }
  return address.port;
}

async function close(server: ReturnType<HttpServerService["buildServer"]>): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

test("HttpServerService serves the status payload", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new Error("not used"); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/`);
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.deepEqual(json, { ok: true, serviceName: "@sha3/polymarket-snapshot-collector" });

  await close(server);
});

test("HttpServerService returns filtered markets", async () => {
  const capturedOptions: Array<{ asset: string; window: string; fromDate: string | null }> = [];
  const httpServerService = buildHttpServerService({
    async listMarkets(options) {
      capturedOptions.push(options);
      return { markets: [{ slug: "btc-5m", asset: "btc", window: "5m", priceToBeat: 100, marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" }] };
    },
    async readMarketSnapshots() { throw new Error("not used"); },
  });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/markets?asset=btc&window=5m&from_date=2026-03-11T10:00:00.000Z`);
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.deepEqual(capturedOptions, [{ asset: "btc", window: "5m", fromDate: "2026-03-11T10:00:00.000Z" }]);
  assert.deepEqual(json, { markets: [{ slug: "btc-5m", asset: "btc", window: "5m", priceToBeat: 100, marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" }] });

  await close(server);
});

test("HttpServerService validates markets query params", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new Error("not used"); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/markets?asset=doge&window=5m`);
  const json = await response.json();

  assert.equal(response.status, 400);
  assert.deepEqual(json, { error: "invalid_request", message: "asset must be one of btc, eth, sol, xrp" });

  await close(server);
});

test("HttpServerService serves market snapshots", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots(slug) { return buildSnapshotPayload(slug); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/markets/btc-5m/snapshots`);
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.slug, "btc-5m");
  assert.equal(json.snapshots.length, 1);

  await close(server);
});

test("HttpServerService maps domain errors", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new MarketNotFoundService("missing"); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const notFoundResponse = await fetch(`http://127.0.0.1:${port}/markets/missing/snapshots`);
  const notFoundJson = await notFoundResponse.json();

  assert.equal(notFoundResponse.status, 404);
  assert.deepEqual(notFoundJson, { error: "market_not_found", message: "market not found for slug missing" });

  await close(server);
});

test("HttpServerService maps consistency errors", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new SnapshotConsistencyService("duplicate rows"); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/markets/btc-5m/snapshots`);
  const json = await response.json();

  assert.equal(response.status, 409);
  assert.deepEqual(json, { error: "snapshot_consistency_error", message: "duplicate rows" });

  await close(server);
});
