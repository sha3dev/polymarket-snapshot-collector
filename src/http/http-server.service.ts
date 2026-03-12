/**
 * @section imports:externals
 */

import { createAdaptorServer } from "@hono/node-server";
import { Hono } from "hono";
import type { Context } from "hono";
import type { StatusCode } from "hono/utils/http-status";
import type { Server } from "node:http";

/**
 * @section imports:internals
 */

import type { AppInfoService } from "../app-info/app-info.service.ts";
import config from "../config.ts";
import LOGGER from "../logger.ts";
import { MarketNotFoundService } from "../snapshot/market-not-found.service.ts";
import { SnapshotConsistencyService } from "../snapshot/snapshot-consistency.service.ts";
import type { SnapshotQueryService } from "../snapshot/snapshot-query.service.ts";
import type { MarketListPayload, MarketSnapshotsPayload, StatePayload } from "../snapshot/snapshot.types.ts";
import { HttpRequestService } from "./http-request.service.ts";

/**
 * @section types
 */

type HttpServerServiceOptions = { appInfoService: AppInfoService; snapshotQueryService: SnapshotQueryService };

type ErrorPayload = { error: string; message: string };
type MappedError = { statusCode: number; payload: ErrorPayload };

/**
 * @section private:properties
 */

export class HttpServerService {
  private readonly appInfoService: AppInfoService;
  private readonly snapshotQueryService: SnapshotQueryService;

  /**
   * @section constructor
   */

  public constructor(options: HttpServerServiceOptions) {
    this.appInfoService = options.appInfoService;
    this.snapshotQueryService = options.snapshotQueryService;
  }

  /**
   * @section private:methods
   */

  private parseAsset(searchParams: URLSearchParams): "btc" | "eth" | "sol" | "xrp" {
    const asset = searchParams.get("asset");
    const isValidAsset = asset === "btc" || asset === "eth" || asset === "sol" || asset === "xrp";
    if (!isValidAsset) {
      throw new HttpRequestService(400, "invalid_request", "asset must be one of btc, eth, sol, xrp");
    }
    return asset;
  }

  private parseWindow(searchParams: URLSearchParams): "5m" | "15m" {
    const window = searchParams.get("window");
    const isValidWindow = window === "5m" || window === "15m";
    if (!isValidWindow) {
      throw new HttpRequestService(400, "invalid_request", "window must be one of 5m, 15m");
    }
    return window;
  }

  private parseFromDate(searchParams: URLSearchParams): string | null {
    const fromDate = searchParams.get("fromDate");
    const isValidFromDate = fromDate ? !Number.isNaN(new Date(fromDate).getTime()) : true;
    if (!isValidFromDate) {
      throw new HttpRequestService(400, "invalid_request", "fromDate must be a valid ISO-8601 timestamp");
    }
    return fromDate ? new Date(fromDate).toISOString() : null;
  }

  private createApplication(): Hono {
    const application = new Hono();
    application.get("/", (context) => context.newResponse(JSON.stringify(this.appInfoService.buildPayload()), 200 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE }));
    application.get("/state", async (context) => {
      const payload: StatePayload = await this.snapshotQueryService.readState();
      return context.newResponse(JSON.stringify(payload), 200 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE });
    });
    application.get("/markets", async (context) => {
      const searchParams = new URL(context.req.url).searchParams;
      const asset = this.parseAsset(searchParams);
      const window = this.parseWindow(searchParams);
      const fromDate = this.parseFromDate(searchParams);
      const payload: MarketListPayload = await this.snapshotQueryService.listMarkets({ asset, window, fromDate });
      return context.newResponse(JSON.stringify(payload), 200 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE });
    });
    application.get("/markets/:slug/snapshots", async (context) => {
      const slug = decodeURIComponent(context.req.param("slug"));
      const payload: MarketSnapshotsPayload = await this.snapshotQueryService.readMarketSnapshots(slug);
      return context.newResponse(JSON.stringify(payload), 200 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE });
    });
    application.notFound((context) => context.newResponse(JSON.stringify({ error: "invalid_request", message: "route not found" }), 404 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE }));
    application.onError((error, context) => this.handleApplicationError(context, error));
    return application;
  }

  private mapError(error: unknown): MappedError {
    let mappedError: MappedError = { statusCode: 500, payload: { error: "internal_error", message: "internal server error" } };

    if (error instanceof HttpRequestService) {
      mappedError = { statusCode: error.statusCode, payload: { error: error.errorCode, message: error.message } };
    }
    if (error instanceof MarketNotFoundService) {
      mappedError = { statusCode: 404, payload: { error: "market_not_found", message: error.message } };
    }
    if (error instanceof SnapshotConsistencyService) {
      mappedError = { statusCode: 409, payload: { error: "snapshot_consistency_error", message: error.message } };
    }
    return mappedError;
  }

  private handleApplicationError(context: Context, error: unknown): Response {
    const mappedError = this.mapError(error);
    LOGGER.error(`request handling failed: ${error instanceof Error ? error.message : String(error)}`);
    return context.newResponse(JSON.stringify(mappedError.payload), mappedError.statusCode as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE });
  }

  /**
   * @section public:methods
   */

  public buildServer(): Server {
    const application = this.createApplication();
    const server = createAdaptorServer({ fetch: application.fetch, overrideGlobalObjects: false }) as Server;
    return server;
  }
}
