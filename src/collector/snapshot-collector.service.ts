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
type PersistedSnapshotEntry = { marketRecord: Awaited<ReturnType<MarketRepositoryService["ensureMarketStored"]>>; snapshot: Snapshot };

/**
 * @section private:properties
 */

export class SnapshotCollectorService {
  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly snapshotRepositoryService: SnapshotRepositoryService;
  private readonly snapshotDeduplicationService: SnapshotDeduplicationService;
  private readonly dashboardStateService: DashboardStateService;
  private readonly snapshotRuntime: SnapshotCollectorRuntime;
  private readonly maxPersistBatchSize = config.SNAPSHOT_INSERT_BATCH_MAX_SIZE;
  private isStarted = false;
  private readonly pendingSnapshots: Snapshot[] = [];
  private activeDrainPromise: Promise<void> | null = null;
  private debugLogTimer: NodeJS.Timeout | null = null;
  private totalReceivedSnapshotCount = 0;
  private totalPersistedSnapshotCount = 0;
  private totalSkippedSnapshotCount = 0;
  private totalFailedSnapshotCount = 0;
  private maxPendingSnapshotCount = 0;
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
    this.totalReceivedSnapshotCount += 1;
    this.pendingSnapshots.push(snapshot);
    this.maxPendingSnapshotCount = Math.max(this.maxPendingSnapshotCount, this.pendingSnapshots.length);
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
      const snapshotBatch = this.pendingSnapshots.splice(0, this.maxPersistBatchSize);
      await this.persistSnapshotBatch(snapshotBatch);
    }
  }

  private rethrowAsyncError(error: unknown): void {
    queueMicrotask(() => {
      throw error instanceof Error ? error : new Error(String(error));
    });
  }

  private startDebugLogging(): void {
    if (config.ENABLE_PERF_LOGS && !this.debugLogTimer) {
      this.debugLogTimer = setInterval(() => {
        this.logDebugMetrics();
      }, 5000);
    }
  }

  private stopDebugLogging(): void {
    if (this.debugLogTimer) {
      clearInterval(this.debugLogTimer);
      this.debugLogTimer = null;
    }
  }

  private logDebugMetrics(): void {
    const repositoryMetrics = this.snapshotRepositoryService.readDebugMetrics();
    const deduplicationMetrics = this.snapshotDeduplicationService.readDebugMetrics();
    LOGGER.info(
      `snapshot collector debug received=${this.totalReceivedSnapshotCount} persisted=${this.totalPersistedSnapshotCount} skipped=${this.totalSkippedSnapshotCount} failed=${this.totalFailedSnapshotCount} pending_snapshots=${this.pendingSnapshots.length} max_pending_snapshots=${this.maxPendingSnapshotCount} pending_inserts=${repositoryMetrics.pendingInsertCount} total_flushes=${repositoryMetrics.totalFlushCount} last_flush_rows=${repositoryMetrics.lastFlushBatchSize} last_flush_ms=${repositoryMetrics.lastFlushDurationMs} flush_active=${repositoryMetrics.isFlushActive} dedup_keys=${deduplicationMetrics.fingerprintKeyCount} dedup_last_cleanup_at=${deduplicationMetrics.lastCleanupAtMs}`,
    );
  }

  private handleSkippedSnapshot(snapshot: Snapshot): void {
    this.totalSkippedSnapshotCount += 1;
    LOGGER.warn(`skipping snapshot without market identity for ${snapshot.asset}/${snapshot.window} at ${snapshot.generatedAt}`);
  }

  private async persistCompleteSnapshot(snapshot: Snapshot): Promise<PersistedSnapshotEntry | null> {
    let persistedSnapshot: PersistedSnapshotEntry | null = null;
    const shouldPersist = this.snapshotDeduplicationService.shouldPersist(snapshot);
    if (shouldPersist) {
      const marketRecord = await this.marketRepositoryService.ensureMarketStored(snapshot);
      persistedSnapshot = { marketRecord, snapshot };
    } else {
      this.totalSkippedSnapshotCount += 1;
    }
    return persistedSnapshot;
  }

  private async buildPersistedSnapshotBatch(snapshotBatch: readonly Snapshot[]): Promise<PersistedSnapshotEntry[]> {
    const persistedBatch: PersistedSnapshotEntry[] = [];
    for (const snapshot of snapshotBatch) {
      const hasPersistableIdentity = this.hasPersistableMarketIdentity(snapshot);
      if (!hasPersistableIdentity) {
        this.handleSkippedSnapshot(snapshot);
      }
      if (hasPersistableIdentity) {
        const persistedSnapshot = await this.persistCompleteSnapshot(snapshot);
        if (persistedSnapshot) {
          persistedBatch.push(persistedSnapshot);
        }
      }
    }
    return persistedBatch;
  }

  private async insertPersistedSnapshotBatch(persistedBatch: readonly PersistedSnapshotEntry[], startedAtMs: number): Promise<void> {
    if (persistedBatch.length > 0) {
      const ensureDurationMs = Date.now() - startedAtMs;
      const insertStartedAtMs = Date.now();
      await this.snapshotRepositoryService.insertSnapshots(persistedBatch.map((entry) => entry.snapshot));
      const insertDurationMs = Date.now() - insertStartedAtMs;
      const dashboardStartedAtMs = Date.now();
      for (const persistedSnapshot of persistedBatch) {
        this.dashboardStateService.updateSnapshot(persistedSnapshot.marketRecord, persistedSnapshot.snapshot);
        this.totalPersistedSnapshotCount += 1;
      }
      const dashboardDurationMs = Date.now() - dashboardStartedAtMs;
      this.logBatchPersistencePerformance(persistedBatch.length, startedAtMs, ensureDurationMs, insertDurationMs, dashboardDurationMs);
    }
  }

  private async persistSnapshotBatch(snapshotBatch: readonly Snapshot[]): Promise<void> {
    const startedAtMs = Date.now();
    try {
      const persistedBatch = await this.buildPersistedSnapshotBatch(snapshotBatch);
      await this.insertPersistedSnapshotBatch(persistedBatch, startedAtMs);
    } catch (error) {
      this.totalFailedSnapshotCount += 1;
      LOGGER.error(`failed to persist snapshot batch size=${snapshotBatch.length}: ${error instanceof Error ? error.message : String(error)}`);
      throw error;
    }
  }

  private logBatchPersistencePerformance(batchSize: number, startedAtMs: number, ensureDurationMs: number, insertDurationMs: number, dashboardDurationMs: number): void {
    if (config.ENABLE_PERF_LOGS) {
      const totalDurationMs = Date.now() - startedAtMs;
      LOGGER.info(
        `snapshot batch persist performance batch_size=${batchSize} ensure_market_ms=${ensureDurationMs} insert_snapshot_ms=${insertDurationMs} update_dashboard_ms=${dashboardDurationMs} total_ms=${totalDurationMs}`,
      );
    }
  }

  /**
   * @section public:methods
   */

  public start(): void {
    if (!this.isStarted) {
      this.snapshotRuntime.addSnapshotListener({ listener: this.snapshotListener, assets: [...config.SUPPORTED_ASSETS], windows: [...config.SUPPORTED_WINDOWS] });
      this.isStarted = true;
      this.startDebugLogging();
      LOGGER.info("snapshot collector subscribed");
    }
  }

  public async stop(): Promise<void> {
    this.stopDebugLogging();
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
