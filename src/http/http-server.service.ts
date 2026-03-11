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
import type { MarketListPayload, MarketSnapshotsPayload } from "../snapshot/snapshot.types.ts";
import { HttpRequestService } from "./http-request.service.ts";

/**
 * @section types
 */

type HttpServerServiceOptions = { appInfoService: AppInfoService; snapshotQueryService: SnapshotQueryService };

type ErrorPayload = { error: string; message: string };

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

  private createErrorPayload(errorCode: string, message: string): ErrorPayload {
    const errorPayload: ErrorPayload = { error: errorCode, message };
    return errorPayload;
  }

  private createJsonResponse(context: Context, statusCode: number, payload: unknown): Response {
    const response = context.newResponse(JSON.stringify(payload), statusCode as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE });
    return response;
  }

  private readFromDate(searchParams: URLSearchParams): string | null {
    const fromDate = searchParams.get("fromDate");
    const fromDateSnakeCase = searchParams.get("from_date");
    let selectedFromDate: string | null = fromDate;
    if (fromDateSnakeCase) {
      if (fromDate && fromDate !== fromDateSnakeCase) {
        throw new HttpRequestService(400, "invalid_request", "fromDate and from_date must match when both are provided");
      }
      selectedFromDate = fromDateSnakeCase;
    }
    return selectedFromDate;
  }

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
    const fromDate = this.readFromDate(searchParams);
    const isValidFromDate = fromDate ? !Number.isNaN(new Date(fromDate).getTime()) : true;
    if (!isValidFromDate) {
      throw new HttpRequestService(400, "invalid_request", "fromDate must be a valid ISO-8601 timestamp");
    }
    return fromDate ? new Date(fromDate).toISOString() : null;
  }

  private async handleMarketsRequest(searchParams: URLSearchParams): Promise<MarketListPayload> {
    const asset = this.parseAsset(searchParams);
    const window = this.parseWindow(searchParams);
    const fromDate = this.parseFromDate(searchParams);
    const payload = await this.snapshotQueryService.listMarkets({ asset, window, fromDate });
    return payload;
  }

  private createApplication(): Hono {
    const application = new Hono();
    application.get("/", (context) => this.createJsonResponse(context, 200, this.appInfoService.buildPayload()));
    application.get("/markets", async (context) => {
      const searchParams = new URL(context.req.url).searchParams;
      const payload = await this.handleMarketsRequest(searchParams);
      return this.createJsonResponse(context, 200, payload);
    });
    application.get("/markets/:slug/snapshots", async (context) => {
      const slug = decodeURIComponent(context.req.param("slug"));
      const payload: MarketSnapshotsPayload = await this.snapshotQueryService.readMarketSnapshots(slug);
      return this.createJsonResponse(context, 200, payload);
    });
    application.notFound((context) => this.createJsonResponse(context, 404, this.createErrorPayload("invalid_request", "route not found")));
    application.onError((error, context) => this.handleApplicationError(context, error));
    return application;
  }

  private mapError(error: unknown): { statusCode: number; payload: ErrorPayload } {
    let mappedError = { statusCode: 500, payload: this.createErrorPayload("internal_error", "internal server error") };

    if (error instanceof HttpRequestService) {
      mappedError = { statusCode: error.statusCode, payload: this.createErrorPayload(error.errorCode, error.message) };
    }
    if (error instanceof MarketNotFoundService) {
      mappedError = { statusCode: 404, payload: this.createErrorPayload("market_not_found", error.message) };
    }
    if (error instanceof SnapshotConsistencyService) {
      mappedError = { statusCode: 409, payload: this.createErrorPayload("snapshot_consistency_error", error.message) };
    }
    return mappedError;
  }

  private handleApplicationError(context: Context, error: unknown): Response {
    const mappedError = this.mapError(error);
    LOGGER.error(`request handling failed: ${error instanceof Error ? error.message : String(error)}`);
    return this.createJsonResponse(context, mappedError.statusCode, mappedError.payload);
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
