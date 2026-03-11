import * as assert from "node:assert/strict";
import { once } from "node:events";
import { test } from "node:test";

import { AppInfoService } from "../src/app-info/app-info.service.ts";
import { HttpServerService } from "../src/http/http-server.service.ts";
import { MarketNotFoundService } from "../src/snapshot/market-not-found.service.ts";
import { SnapshotConsistencyService } from "../src/snapshot/snapshot-consistency.service.ts";
import type { DashboardPayload, MarketListPayload, MarketSnapshotsPayload } from "../src/snapshot/snapshot.types.ts";

type QueryServiceDouble = {
  listMarkets(options: { asset: string; window: string; fromDate: string | null }): Promise<MarketListPayload>;
  readMarketSnapshots(slug: string): Promise<MarketSnapshotsPayload>;
  readDashboard(): Promise<DashboardPayload>;
};

type MarketFilter = { asset: string; window: string; fromDate: string | null };

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

function buildUnusedQueryServiceDouble(): QueryServiceDouble {
  return { async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new Error("not used"); }, async readDashboard() { throw new Error("not used"); } };
}

function buildMarketListPayload(): MarketListPayload {
  return { markets: [{ slug: "btc-5m", asset: "btc", window: "5m", priceToBeat: 100, marketStart: "2026-03-11T10:00:00.000Z", marketEnd: "2026-03-11T10:05:00.000Z" }] };
}

function buildSnapshotPayload(slug: string): MarketSnapshotsPayload {
  const snapshot = BASE_MARKET_SNAPSHOTS_PAYLOAD.snapshots[0];
  if (!snapshot) {
    throw new Error("missing base snapshot");
  }
  return { ...BASE_MARKET_SNAPSHOTS_PAYLOAD, slug, snapshots: [{ ...snapshot, marketSlug: slug }] };
}

function buildDashboardWidget() {
  return {
    asset: "btc" as const,
    window: "5m" as const,
    market: {
      slug: "btc-5m",
      asset: "btc" as const,
      window: "5m" as const,
      priceToBeat: 100,
      marketStart: "2026-03-11T10:00:00.000Z",
      marketEnd: "2026-03-11T10:05:00.000Z",
    },
    snapshotCount: 1,
    latestSnapshot: {
      generatedAt: 1741687200000,
      priceToBeat: 100,
      upPrice: 0.55,
      downPrice: 0.45,
      chainlinkPrice: 100,
      binancePrice: 100,
      coinbasePrice: 100,
      krakenPrice: 100,
      okxPrice: 100,
    },
    marketDirection: "UP" as const,
    latestSnapshotAgeMs: 800,
    isStale: false,
  };
}

function buildDashboardPayload(): DashboardPayload {
  return { generatedAt: "2026-03-11T10:00:00.000Z", widgets: [buildDashboardWidget()] };
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
  const httpServerService = buildHttpServerService(buildUnusedQueryServiceDouble());
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/`);
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.deepEqual(json, { ok: true, serviceName: "@sha3/polymarket-snapshot-collector" });

  await close(server);
});

test("HttpServerService returns filtered markets", async () => {
  const capturedOptions: MarketFilter[] = [];
  const httpServerService = buildHttpServerService({
    async listMarkets(options) {
      capturedOptions.push(options);
      return buildMarketListPayload();
    },
    async readMarketSnapshots() { throw new Error("not used"); },
    async readDashboard() { throw new Error("not used"); },
  });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/markets?asset=btc&window=5m&fromDate=2026-03-11T10:00:00.000Z`);
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.deepEqual(capturedOptions, [{ asset: "btc", window: "5m", fromDate: "2026-03-11T10:00:00.000Z" }]);
  assert.deepEqual(json, buildMarketListPayload());

  await close(server);
});

test("HttpServerService validates markets query params", async () => {
  const httpServerService = buildHttpServerService(buildUnusedQueryServiceDouble());
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/markets?asset=doge&window=5m`);
  const json = await response.json();

  assert.equal(response.status, 400);
  assert.deepEqual(json, { error: "invalid_request", message: "asset must be one of btc, eth, sol, xrp" });

  await close(server);
});

test("HttpServerService serves market snapshots", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots(slug) { return buildSnapshotPayload(slug); }, async readDashboard() { throw new Error("not used"); } });
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
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new MarketNotFoundService("missing"); }, async readDashboard() { throw new Error("not used"); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const notFoundResponse = await fetch(`http://127.0.0.1:${port}/markets/missing/snapshots`);
  const notFoundJson = await notFoundResponse.json();

  assert.equal(notFoundResponse.status, 404);
  assert.deepEqual(notFoundJson, { error: "market_not_found", message: "market not found for slug missing" });

  await close(server);
});

test("HttpServerService maps consistency errors", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new SnapshotConsistencyService("duplicate rows"); }, async readDashboard() { throw new Error("not used"); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/markets/btc-5m/snapshots`);
  const json = await response.json();

  assert.equal(response.status, 409);
  assert.deepEqual(json, { error: "snapshot_consistency_error", message: "duplicate rows" });

  await close(server);
});

test("HttpServerService serves dashboard html", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new Error("not used"); }, async readDashboard() { return buildDashboardPayload(); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/dashboard`);
  const html = await response.text();

  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") || "", /text\/html/);
  assert.match(html, /Snapshot Dashboard/);
  assert.match(html, /dashboard\.css/);
  assert.match(html, /dashboard\.js/);

  await close(server);
});

test("HttpServerService serves dashboard static assets", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new Error("not used"); }, async readDashboard() { return buildDashboardPayload(); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const jsResponse = await fetch(`http://127.0.0.1:${port}/dashboard.js`);
  const cssResponse = await fetch(`http://127.0.0.1:${port}/dashboard.css`);
  const jsBody = await jsResponse.text();
  const cssBody = await cssResponse.text();

  assert.equal(jsResponse.status, 200);
  assert.match(jsResponse.headers.get("content-type") || "", /javascript/);
  assert.match(jsBody, /fetch\("\/dashboard\/state"/);
  assert.match(jsBody, /const STALE_AFTER_MS = 1000/);
  assert.equal(cssResponse.status, 200);
  assert.match(cssResponse.headers.get("content-type") || "", /text\/css/);
  assert.match(cssBody, /\.card/);

  await close(server);
});

test("HttpServerService serves dashboard state", async () => {
  const httpServerService = buildHttpServerService({ async listMarkets() { return { markets: [] }; }, async readMarketSnapshots() { throw new Error("not used"); }, async readDashboard() { return buildDashboardPayload(); } });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/dashboard/state`);
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.equal(json.widgets[0].latestSnapshot.upPrice, 0.55);
  assert.equal(json.widgets[0].latestSnapshot.downPrice, 0.45);
  assert.equal(json.widgets[0].marketDirection, "UP");

  await close(server);
});
