import * as assert from "node:assert/strict";
import { once } from "node:events";
import { test } from "node:test";

import { AppInfoService } from "../src/app-info/app-info.service.ts";
import { HttpServerService } from "../src/http/http-server.service.ts";
import type { MarketListPayload, SnapshotRangePayload } from "../src/snapshot/snapshot.types.ts";

type QueryServiceDouble = {
  listMarkets(options: { asset: string | null; window: string | null; fromDate: string | null }): Promise<MarketListPayload>;
  readSnapshots(options: { fromDate: string; toDate: string; limit: number; marketSlug: string | null }): Promise<SnapshotRangePayload>;
};

function buildHttpServerService(queryServiceDouble: QueryServiceDouble): HttpServerService {
  const httpServerService = new HttpServerService({
    appInfoService: new AppInfoService({ serviceName: "@sha3/polymarket-snapshot-collector" }),
    snapshotQueryService: queryServiceDouble as never,
  });
  return httpServerService;
}

function buildUnusedQueryServiceDouble(): QueryServiceDouble {
  const queryServiceDouble: QueryServiceDouble = {
    async listMarkets() {
      return { markets: [] };
    },
    async readSnapshots() {
      throw new Error("not used");
    },
  };
  return queryServiceDouble;
}

function buildMarketListPayload(): MarketListPayload {
  const marketListPayload: MarketListPayload = {
    markets: [
      {
        slug: "btc-5m",
        asset: "btc",
        window: "5m",
        priceToBeat: 100,
        marketStart: "2026-03-11T10:00:00.000Z",
        marketEnd: "2026-03-11T10:05:00.000Z",
      },
    ],
  };
  return marketListPayload;
}

function buildSnapshotRangePayload(): SnapshotRangePayload {
  const snapshotRangePayload: SnapshotRangePayload = {
    fromDate: "2026-03-11T10:00:00.000Z",
    toDate: "2026-03-11T10:01:00.000Z",
    marketSlug: "btc-5m",
    snapshots: [
      {
        id: "s1",
        generated_at: 1741687200000,
        inserted_at: "2026-03-11T10:00:00.100Z",
        btc_binance_price: 100,
        btc_5m_slug: "btc-5m",
        btc_5m_market_start: "2026-03-11T10:00:00.000Z",
      },
    ],
  };
  return snapshotRangePayload;
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
  const capturedOptions: Array<{ asset: string | null; window: string | null; fromDate: string | null }> = [];
  const httpServerService = buildHttpServerService({
    async listMarkets(options) {
      capturedOptions.push(options);
      return buildMarketListPayload();
    },
    async readSnapshots() {
      throw new Error("not used");
    },
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

test("HttpServerService serves flat snapshots by range and marketSlug", async () => {
  const capturedOptions: Array<{ fromDate: string; toDate: string; limit: number; marketSlug: string | null }> = [];
  const httpServerService = buildHttpServerService({
    async listMarkets() {
      return { markets: [] };
    },
    async readSnapshots(options) {
      capturedOptions.push(options);
      return buildSnapshotRangePayload();
    },
  });
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(
    `http://127.0.0.1:${port}/snapshots?fromDate=2026-03-11T10:00:00.000Z&toDate=2026-03-11T10:01:00.000Z&limit=25&marketSlug=btc-5m`,
  );
  const json = await response.json();

  assert.equal(response.status, 200);
  assert.deepEqual(capturedOptions, [
    {
      fromDate: "2026-03-11T10:00:00.000Z",
      toDate: "2026-03-11T10:01:00.000Z",
      limit: 25,
      marketSlug: "btc-5m",
    },
  ]);
  assert.equal(json.marketSlug, "btc-5m");

  await close(server);
});

test("HttpServerService validates snapshots query params", async () => {
  const httpServerService = buildHttpServerService(buildUnusedQueryServiceDouble());
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/snapshots?fromDate=invalid&limit=0`);
  const json = await response.json();

  assert.equal(response.status, 400);
  assert.deepEqual(json, { error: "invalid_request", message: "fromDate must be a valid ISO-8601 timestamp" });

  await close(server);
});

test("HttpServerService validates date ordering", async () => {
  const httpServerService = buildHttpServerService(buildUnusedQueryServiceDouble());
  const server = httpServerService.buildServer();
  const port = await listen(server);
  const response = await fetch(`http://127.0.0.1:${port}/snapshots?fromDate=2026-03-11T10:02:00.000Z&toDate=2026-03-11T10:01:00.000Z`);
  const json = await response.json();

  assert.equal(response.status, 400);
  assert.deepEqual(json, { error: "invalid_request", message: "fromDate must be less than or equal to toDate" });

  await close(server);
});
