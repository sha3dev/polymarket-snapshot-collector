/**
 * @section imports:internals
 */

import type { MarketRepositoryService } from "../market/market-repository.service.ts";
import type { MarketRecord } from "../market/market.types.ts";
import type { FlatSnapshotRepositoryService } from "./flat-snapshot-repository.service.ts";
import type { MarketListPayload, MarketSummary, SnapshotRangePayload } from "./snapshot.types.ts";

/**
 * @section types
 */

type SnapshotQueryServiceOptions = {
  marketRepositoryService: MarketRepositoryService;
  flatSnapshotRepositoryService: FlatSnapshotRepositoryService;
};

type SnapshotReadOptions = {
  fromDate: string | null;
  toDate: string;
  limit: number;
  marketSlug: string | null;
};

/**
 * @section class
 */

export class SnapshotQueryService {
  /**
   * @section private:attributes
   */

  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly flatSnapshotRepositoryService: FlatSnapshotRepositoryService;

  /**
   * @section constructor
   */

  public constructor(options: SnapshotQueryServiceOptions) {
    this.marketRepositoryService = options.marketRepositoryService;
    this.flatSnapshotRepositoryService = options.flatSnapshotRepositoryService;
  }

  /**
   * @section private:methods
   */

  private mapMarketSummary(marketRecord: MarketRecord): MarketSummary {
    const marketSummary: MarketSummary = {
      slug: marketRecord.slug,
      asset: marketRecord.asset,
      window: marketRecord.window,
      priceToBeat: marketRecord.priceToBeat,
      marketStart: marketRecord.marketStart,
      marketEnd: marketRecord.marketEnd,
    };
    return marketSummary;
  }

  /**
   * @section public:methods
   */

  public async listMarkets(options: Parameters<MarketRepositoryService["listMarkets"]>[0]): Promise<MarketListPayload> {
    const marketRecords = await this.marketRepositoryService.listMarkets(options);
    const payload: MarketListPayload = {
      markets: marketRecords.map((marketRecord) => this.mapMarketSummary(marketRecord)),
    };
    return payload;
  }

  public async readSnapshots(options: SnapshotReadOptions): Promise<SnapshotRangePayload> {
    const snapshots = await this.flatSnapshotRepositoryService.listSnapshots(options);
    const payload: SnapshotRangePayload = {
      fromDate: options.fromDate,
      toDate: options.toDate,
      marketSlug: options.marketSlug,
      snapshots,
    };
    return payload;
  }
}
