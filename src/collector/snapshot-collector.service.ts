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
import type { DashboardStateService } from "../snapshot/dashboard-state.service.ts";
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
  dashboardStateService: DashboardStateService;
  snapshotRuntime: SnapshotCollectorRuntime;
};

/**
 * @section private:properties
 */

export class SnapshotCollectorService {
  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly snapshotRepositoryService: SnapshotRepositoryService;
  private readonly snapshotDeduplicationService: SnapshotDeduplicationService;
  private readonly dashboardStateService: DashboardStateService;
  private readonly snapshotRuntime: SnapshotCollectorRuntime;
  private isStarted = false;
  private readonly pendingSnapshots: Snapshot[] = [];
  private activeDrainPromise: Promise<void> | null = null;
  private readonly snapshotListener = (snapshot: Snapshot): void => {
    this.enqueueSnapshot(snapshot);
  };

  /**
   * @section constructor
   */

  public constructor(options: SnapshotCollectorServiceOptions) {
    this.marketRepositoryService = options.marketRepositoryService;
    this.snapshotRepositoryService = options.snapshotRepositoryService;
    this.snapshotDeduplicationService = options.snapshotDeduplicationService;
    this.dashboardStateService = options.dashboardStateService;
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

  private enqueueSnapshot(snapshot: Snapshot): void {
    this.pendingSnapshots.push(snapshot);
    this.ensureDrainStarted();
  }

  private ensureDrainStarted(): void {
    if (!this.activeDrainPromise) {
      this.activeDrainPromise = this.runDrainPendingSnapshots();
    }
  }

  private async runDrainPendingSnapshots(): Promise<void> {
    try {
      await this.drainPendingSnapshots();
    } catch (error) {
      LOGGER.error(`snapshot drain failed: ${error instanceof Error ? error.message : String(error)}`);
      this.rethrowAsyncError(error);
    } finally {
      this.activeDrainPromise = null;
      if (this.pendingSnapshots.length > 0) {
        this.ensureDrainStarted();
      }
    }
  }

  private async drainPendingSnapshots(): Promise<void> {
    while (this.pendingSnapshots.length > 0) {
      const pendingSnapshot = this.pendingSnapshots.shift() || null;
      if (pendingSnapshot) {
        await this.persistSnapshot(pendingSnapshot);
      }
    }
  }

  private rethrowAsyncError(error: unknown): void {
    queueMicrotask(() => {
      throw error instanceof Error ? error : new Error(String(error));
    });
  }

  private logPersistencePerformance(snapshot: Snapshot, startedAtMs: number, ensureDurationMs: number, insertDurationMs: number, dashboardDurationMs: number): void {
    if (config.ENABLE_PERF_LOGS) {
      const totalDurationMs = Date.now() - startedAtMs;
      LOGGER.info(
        `snapshot persist performance asset=${snapshot.asset} window=${snapshot.window} slug=${snapshot.marketSlug || "unknown"} ensure_market_ms=${ensureDurationMs} insert_snapshot_ms=${insertDurationMs} update_dashboard_ms=${dashboardDurationMs} total_ms=${totalDurationMs}`,
      );
    }
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
          const startedAtMs = Date.now();
          const ensureStartedAtMs = Date.now();
          const marketRecord = await this.marketRepositoryService.ensureMarketStored(snapshot);
          const ensureDurationMs = Date.now() - ensureStartedAtMs;
          const insertStartedAtMs = Date.now();
          await this.snapshotRepositoryService.insertSnapshot(snapshot);
          const insertDurationMs = Date.now() - insertStartedAtMs;
          const dashboardStartedAtMs = Date.now();
          this.dashboardStateService.updateSnapshot(marketRecord, snapshot);
          const dashboardDurationMs = Date.now() - dashboardStartedAtMs;
          this.logPersistencePerformance(snapshot, startedAtMs, ensureDurationMs, insertDurationMs, dashboardDurationMs);
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
    if (this.activeDrainPromise) {
      await this.activeDrainPromise;
    }
  }
}
