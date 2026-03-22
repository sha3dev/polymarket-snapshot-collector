/**
 * @section imports:externals
 */

import { createAdaptorServer } from "@hono/node-server";
import type { ServerType } from "@hono/node-server";
import { Hono } from "hono";
import type { Context } from "hono";
import type { StatusCode } from "hono/utils/http-status";

/**
 * @section imports:internals
 */

import type { AppInfoService } from "../app-info/app-info.service.ts";
import config from "../config.ts";
import LOGGER from "../logger.ts";
import type { SnapshotQueryService } from "../snapshot/snapshot-query.service.ts";
import type { MarketListPayload, SnapshotRangePayload } from "../snapshot/snapshot.types.ts";
import { HttpRequestService } from "./http-request.service.ts";

/**
 * @section types
 */

type HttpServerServiceOptions = {
  appInfoService: AppInfoService;
  snapshotQueryService: SnapshotQueryService;
};

type ErrorPayload = { error: string; message: string };
type MappedError = { statusCode: number; payload: ErrorPayload };
type SnapshotReadOptions = { fromDate: string | null; toDate: string; limit: number; marketSlug: string | null };

/**
 * @section class
 */

export class HttpServerService {
  /**
   * @section private:attributes
   */

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

  private parseAsset(searchParams: URLSearchParams): "btc" | "eth" | "sol" | "xrp" | null {
    const asset = searchParams.get("asset");
    const isMissingAsset = asset === null;
    const isValidAsset = isMissingAsset || asset === "btc" || asset === "eth" || asset === "sol" || asset === "xrp";
    if (!isValidAsset) {
      throw new HttpRequestService(400, "invalid_request", "asset must be one of btc, eth, sol, xrp");
    }
    return asset;
  }

  private parseWindow(searchParams: URLSearchParams): "5m" | "15m" | null {
    const window = searchParams.get("window");
    const isMissingWindow = window === null;
    const isValidWindow = isMissingWindow || window === "5m" || window === "15m";
    if (!isValidWindow) {
      throw new HttpRequestService(400, "invalid_request", "window must be one of 5m, 15m");
    }
    return window;
  }

  private parseOptionalDate(searchParams: URLSearchParams, fieldName: "fromDate" | "toDate"): string | null {
    const rawDate = searchParams.get(fieldName);
    const isValidDate = rawDate ? !Number.isNaN(new Date(rawDate).getTime()) : true;
    if (!isValidDate) {
      throw new HttpRequestService(400, "invalid_request", `${fieldName} must be a valid ISO-8601 timestamp`);
    }
    const parsedDate = rawDate ? new Date(rawDate).toISOString() : null;
    return parsedDate;
  }

  private parseRequiredDate(searchParams: URLSearchParams, fieldName: "fromDate" | "toDate"): string {
    const parsedDate = this.parseOptionalDate(searchParams, fieldName);
    if (parsedDate === null) {
      throw new HttpRequestService(400, "invalid_request", `${fieldName} is required`);
    }
    return parsedDate;
  }

  private parseLimit(searchParams: URLSearchParams): number {
    const rawLimit = searchParams.get("limit");
    const parsedLimit = rawLimit ? Number(rawLimit) : 1000;
    const isValidLimit = Number.isInteger(parsedLimit) && parsedLimit > 0 && parsedLimit <= 5000;
    if (!isValidLimit) {
      throw new HttpRequestService(400, "invalid_request", "limit must be an integer between 1 and 5000");
    }
    return parsedLimit;
  }

  private parseMarketSlug(searchParams: URLSearchParams): string | null {
    const marketSlug = searchParams.get("marketSlug");
    const isInvalidMarketSlug = marketSlug !== null && marketSlug.trim().length === 0;
    if (isInvalidMarketSlug) {
      throw new HttpRequestService(400, "invalid_request", "marketSlug must not be empty");
    }
    return marketSlug;
  }

  private buildSnapshotReadOptions(searchParams: URLSearchParams): SnapshotReadOptions {
    const fromDate = this.parseOptionalDate(searchParams, "fromDate");
    const toDate = this.parseRequiredDate(searchParams, "toDate");
    const hasInvalidDateRange = fromDate !== null && new Date(fromDate).getTime() > new Date(toDate).getTime();
    if (hasInvalidDateRange) {
      throw new HttpRequestService(400, "invalid_request", "fromDate must be less than or equal to toDate");
    }
    const snapshotReadOptions: SnapshotReadOptions = {
      fromDate,
      toDate,
      limit: this.parseLimit(searchParams),
      marketSlug: this.parseMarketSlug(searchParams),
    };
    return snapshotReadOptions;
  }

  private createApplication(): Hono {
    const application = new Hono();
    application.get("/", (context) =>
      context.newResponse(JSON.stringify(this.appInfoService.buildPayload()), 200 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE }),
    );
    application.get("/markets", async (context) => {
      const searchParams = new URL(context.req.url).searchParams;
      const payload: MarketListPayload = await this.snapshotQueryService.listMarkets({
        asset: this.parseAsset(searchParams),
        window: this.parseWindow(searchParams),
        fromDate: this.parseOptionalDate(searchParams, "fromDate"),
      });
      return context.newResponse(JSON.stringify(payload), 200 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE });
    });
    application.get("/snapshots", async (context) => {
      const searchParams = new URL(context.req.url).searchParams;
      const payload: SnapshotRangePayload = await this.snapshotQueryService.readSnapshots(this.buildSnapshotReadOptions(searchParams));
      return context.newResponse(JSON.stringify(payload), 200 as StatusCode, { "content-type": config.RESPONSE_CONTENT_TYPE });
    });
    application.notFound((context) =>
      context.newResponse(JSON.stringify({ error: "invalid_request", message: "route not found" }), 404 as StatusCode, {
        "content-type": config.RESPONSE_CONTENT_TYPE,
      }),
    );
    application.onError((error, context) => this.handleApplicationError(context, error));
    return application;
  }

  private mapError(error: unknown): MappedError {
    let mappedError: MappedError = { statusCode: 500, payload: { error: "internal_error", message: "internal server error" } };
    if (error instanceof HttpRequestService) {
      mappedError = { statusCode: error.statusCode, payload: { error: error.errorCode, message: error.message } };
    }
    return mappedError;
  }

  private handleApplicationError(context: Context, error: unknown): Response {
    const mappedError = this.mapError(error);
    LOGGER.error(`request handling failed: ${error instanceof Error ? error.message : String(error)}`);
    const response = context.newResponse(JSON.stringify(mappedError.payload), mappedError.statusCode as StatusCode, {
      "content-type": config.RESPONSE_CONTENT_TYPE,
    });
    return response;
  }

  /**
   * @section public:methods
   */

  public buildServer(): ServerType {
    const application = this.createApplication();
    const server = createAdaptorServer({ fetch: application.fetch, overrideGlobalObjects: false });
    return server;
  }
}
