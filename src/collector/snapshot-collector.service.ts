/**
 * @section imports:externals
 */

import { SnapshotService } from "@sha3/polymarket-snapshot";
import type { Snapshot } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import LOGGER from "../logger.ts";
import type { MarketRepositoryService } from "../market/market-repository.service.ts";
import type { SnapshotDeduplicationService } from "../snapshot/snapshot-deduplication.service.ts";
import type { SnapshotRepositoryService } from "../snapshot/snapshot-repository.service.ts";
import type { SnapshotCollectorRuntime } from "./collector.types.ts";

/**
 * @section types
 */

type SnapshotCollectorServiceOptions = {
  marketRepositoryService: MarketRepositoryService;
  snapshotRepositoryService: SnapshotRepositoryService;
  snapshotDeduplicationService: SnapshotDeduplicationService;
  snapshotRuntime: SnapshotCollectorRuntime;
};

/**
 * @section private:properties
 */

export class SnapshotCollectorService {
  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly snapshotRepositoryService: SnapshotRepositoryService;
  private readonly snapshotDeduplicationService: SnapshotDeduplicationService;
  private readonly snapshotRuntime: SnapshotCollectorRuntime;
  private isStarted = false;
  private readonly snapshotListener = (snapshot: Snapshot): void => {
    void this.persistSnapshot(snapshot);
  };

  /**
   * @section constructor
   */

  public constructor(options: SnapshotCollectorServiceOptions) {
    this.marketRepositoryService = options.marketRepositoryService;
    this.snapshotRepositoryService = options.snapshotRepositoryService;
    this.snapshotDeduplicationService = options.snapshotDeduplicationService;
    this.snapshotRuntime = options.snapshotRuntime;
  }

  /**
   * @section factory
   */

  public static createDefault(options: Omit<SnapshotCollectorServiceOptions, "snapshotRuntime">): SnapshotCollectorService {
    return new SnapshotCollectorService({
      ...options,
      snapshotRuntime: SnapshotService.createDefault({ snapshotIntervalMs: config.SNAPSHOT_INTERVAL_MS, supportedAssets: [...config.SUPPORTED_ASSETS], supportedWindows: [...config.SUPPORTED_WINDOWS] }),
    });
  }

  /**
   * @section private:methods
   */

  private hasPersistableMarketIdentity(snapshot: Snapshot): boolean {
    const hasPersistableIdentity = Boolean(snapshot.marketSlug && snapshot.marketStart && snapshot.marketEnd);
    return hasPersistableIdentity;
  }

  private async persistSnapshot(snapshot: Snapshot): Promise<void> {
    const hasPersistableIdentity = this.hasPersistableMarketIdentity(snapshot);
    if (!hasPersistableIdentity) {
      LOGGER.warn(`skipping snapshot without market identity for ${snapshot.asset}/${snapshot.window} at ${snapshot.generatedAt}`);
    }
    if (hasPersistableIdentity) {
      try {
        const shouldPersist = this.snapshotDeduplicationService.shouldPersist(snapshot);
        if (shouldPersist) {
          await this.marketRepositoryService.ensureMarketStored(snapshot);
          await this.snapshotRepositoryService.insertSnapshot(snapshot);
        }
      } catch (error) {
        LOGGER.error(
          `failed to persist snapshot for ${snapshot.marketSlug || "unknown"} at ${snapshot.generatedAt}: ${error instanceof Error ? error.message : String(error)}`,
        );
        throw error;
      }
    }
  }

  /**
   * @section public:methods
   */

  public start(): void {
    if (!this.isStarted) {
      this.snapshotRuntime.addSnapshotListener({ listener: this.snapshotListener, assets: [...config.SUPPORTED_ASSETS], windows: [...config.SUPPORTED_WINDOWS] });
      this.isStarted = true;
      LOGGER.info("snapshot collector subscribed");
    }
  }

  public async stop(): Promise<void> {
    if (this.isStarted) {
      this.snapshotRuntime.removeSnapshotListener(this.snapshotListener);
      await this.snapshotRuntime.disconnect();
      this.isStarted = false;
    }
  }
}
