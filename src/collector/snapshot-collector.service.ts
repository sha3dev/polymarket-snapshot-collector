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
import type { SnapshotCollectorDiagnosticService } from "./snapshot-collector-diagnostic.service.ts";
import type { SnapshotCollectorRuntime } from "./collector.types.ts";

/**
 * @section types
 */

type SnapshotCollectorServiceOptions = {
  marketRepositoryService: MarketRepositoryService;
  snapshotRepositoryService: SnapshotRepositoryService;
  snapshotDeduplicationService: SnapshotDeduplicationService;
  dashboardStateService: DashboardStateService;
  snapshotCollectorDiagnosticService: SnapshotCollectorDiagnosticService;
  snapshotRuntime: SnapshotCollectorRuntime;
};
type PersistedSnapshotEntry = { marketRecord: Awaited<ReturnType<MarketRepositoryService["ensureMarketStored"]>>; snapshot: Snapshot };
type QueuedSnapshotEntry = { snapshot: Snapshot; enqueuedAtMs: number };

/**
 * @section private:properties
 */

export class SnapshotCollectorService {
  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly snapshotRepositoryService: SnapshotRepositoryService;
  private readonly snapshotDeduplicationService: SnapshotDeduplicationService;
  private readonly dashboardStateService: DashboardStateService;
  private readonly snapshotCollectorDiagnosticService: SnapshotCollectorDiagnosticService;
  private readonly snapshotRuntime: SnapshotCollectorRuntime;
  private readonly maxPersistBatchSize = config.SNAPSHOT_INSERT_BATCH_MAX_SIZE;
  private isStarted = false;
  private readonly pendingSnapshots: QueuedSnapshotEntry[] = [];
  private activeDrainPromise: Promise<void> | null = null;
  private readonly snapshotListener = (snapshot: Snapshot): void => {
    const listenerReceivedAtMs = Date.now();
    this.enqueueSnapshot(snapshot, listenerReceivedAtMs);
  };

  /**
   * @section constructor
   */

  public constructor(options: SnapshotCollectorServiceOptions) {
    this.marketRepositoryService = options.marketRepositoryService;
    this.snapshotRepositoryService = options.snapshotRepositoryService;
    this.snapshotDeduplicationService = options.snapshotDeduplicationService;
    this.dashboardStateService = options.dashboardStateService;
    this.snapshotCollectorDiagnosticService = options.snapshotCollectorDiagnosticService;
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

  private enqueueSnapshot(snapshot: Snapshot, listenerReceivedAtMs: number): void {
    this.pendingSnapshots.push({ snapshot, enqueuedAtMs: listenerReceivedAtMs });
    this.snapshotCollectorDiagnosticService.recordListenerIngress(snapshot.generatedAt, listenerReceivedAtMs, this.pendingSnapshots.length);
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

  private handleSkippedSnapshot(snapshot: Snapshot): void {
    LOGGER.warn(`skipping snapshot without market identity for ${snapshot.asset}/${snapshot.window} at ${snapshot.generatedAt}`);
  }

  private async persistCompleteSnapshot(snapshot: Snapshot): Promise<PersistedSnapshotEntry | null> {
    let persistedSnapshot: PersistedSnapshotEntry | null = null;
    const shouldPersist = this.snapshotDeduplicationService.shouldPersist(snapshot);
    if (shouldPersist) {
      const marketRecord = await this.marketRepositoryService.ensureMarketStored(snapshot);
      persistedSnapshot = { marketRecord, snapshot };
    }
    return persistedSnapshot;
  }

  private async buildPersistedSnapshotBatch(snapshotBatch: readonly QueuedSnapshotEntry[]): Promise<PersistedSnapshotEntry[]> {
    const persistedBatch: PersistedSnapshotEntry[] = [];
    for (const queuedSnapshotEntry of snapshotBatch) {
      const queueWaitMs = Math.max(Date.now() - queuedSnapshotEntry.enqueuedAtMs, 0);
      const snapshot = queuedSnapshotEntry.snapshot;
      this.snapshotCollectorDiagnosticService.recordQueueWait(queueWaitMs, this.pendingSnapshots.length);
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

  private async insertPersistedSnapshotBatch(persistedBatch: readonly PersistedSnapshotEntry[]): Promise<void> {
    if (persistedBatch.length > 0) {
      this.updateDashboardSnapshotBatch(persistedBatch);
      const insertStartedAtMs = Date.now();
      await this.snapshotRepositoryService.insertSnapshots(persistedBatch.map((entry) => entry.snapshot));
      const insertDurationMs = Date.now() - insertStartedAtMs;
      const lastSnapshot = persistedBatch[persistedBatch.length - 1]?.snapshot || null;
      this.snapshotCollectorDiagnosticService.recordPersistence(insertDurationMs, lastSnapshot?.generatedAt || null, persistedBatch.length, this.pendingSnapshots.length);
    }
  }

  private updateDashboardSnapshotBatch(persistedBatch: readonly PersistedSnapshotEntry[]): void {
    for (const persistedSnapshot of persistedBatch) {
      this.snapshotCollectorDiagnosticService.recordDashboardUpdate(persistedSnapshot.snapshot.generatedAt);
      this.dashboardStateService.updateSnapshot(persistedSnapshot.marketRecord, persistedSnapshot.snapshot);
    }
  }

  private async persistSnapshotBatch(snapshotBatch: readonly QueuedSnapshotEntry[]): Promise<void> {
    try {
      const persistedBatch = await this.buildPersistedSnapshotBatch(snapshotBatch);
      await this.insertPersistedSnapshotBatch(persistedBatch);
    } catch (error) {
      LOGGER.error(`failed to persist snapshot batch size=${snapshotBatch.length}: ${error instanceof Error ? error.message : String(error)}`);
      throw error;
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
