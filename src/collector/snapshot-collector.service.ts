/**
 * @section imports:externals
 */

import { SnapshotService } from "@sha3/polymarket-snapshot";
import type { Snapshot, SnapshotService as SnapshotServiceType } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import LOGGER from "../logger.ts";
import type { MarketRepositoryService } from "../market/market-repository.service.ts";
import type { SnapshotRepositoryService } from "../snapshot/snapshot-repository.service.ts";
import type { StateStoreService } from "../snapshot/state-store.service.ts";

/**
 * @section types
 */

type SnapshotCollectorRuntime = Pick<SnapshotServiceType, "addSnapshotListener" | "removeSnapshotListener" | "disconnect">;
type SnapshotCollectorServiceOptions = {
  marketRepositoryService: MarketRepositoryService;
  snapshotRepositoryService: SnapshotRepositoryService;
  stateStoreService: StateStoreService;
  snapshotRuntime: SnapshotCollectorRuntime;
};
type PersistedSnapshotEntry = { marketRecord: Awaited<ReturnType<MarketRepositoryService["ensureMarketStored"]>>; snapshot: Snapshot };
type QueuedSnapshotEntry = { snapshot: Snapshot };

/**
 * @section private:properties
 */

export class SnapshotCollectorService {
  private readonly marketRepositoryService: MarketRepositoryService;
  private readonly snapshotRepositoryService: SnapshotRepositoryService;
  private readonly stateStoreService: StateStoreService;
  private readonly snapshotRuntime: SnapshotCollectorRuntime;
  private readonly maxPersistBatchSize = config.SNAPSHOT_INSERT_BATCH_MAX_SIZE;
  private isStarted = false;
  private readonly pendingSnapshots: QueuedSnapshotEntry[] = [];
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
    this.stateStoreService = options.stateStoreService;
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
    this.pendingSnapshots.push({ snapshot });
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

  private parseUtcTimestamp(value: string): number {
    const parsedTimestamp = new Date(value).getTime();
    return parsedTimestamp;
  }

  private isSnapshotWithinMarketWindow(snapshot: Snapshot, marketRecord: Awaited<ReturnType<MarketRepositoryService["ensureMarketStored"]>>): boolean {
    const marketStartTimestamp = this.parseUtcTimestamp(marketRecord.marketStart);
    const marketEndTimestamp = this.parseUtcTimestamp(marketRecord.marketEnd);
    const isWithinMarketWindow = snapshot.generatedAt >= marketStartTimestamp && snapshot.generatedAt < marketEndTimestamp;
    return isWithinMarketWindow;
  }

  private handleOutOfWindowSnapshot(snapshot: Snapshot, marketRecord: Awaited<ReturnType<MarketRepositoryService["ensureMarketStored"]>>): void {
    LOGGER.warn(
      `skipping out-of-window snapshot for ${snapshot.asset}/${snapshot.window} slug=${marketRecord.slug} generatedAt=${snapshot.generatedAt} marketStart=${marketRecord.marketStart} marketEnd=${marketRecord.marketEnd}`,
    );
  }

  private async persistCompleteSnapshot(snapshot: Snapshot): Promise<PersistedSnapshotEntry | null> {
    let persistedSnapshot: PersistedSnapshotEntry | null = null;
    const marketRecord = await this.marketRepositoryService.ensureMarketStored(snapshot);
    const isWithinMarketWindow = this.isSnapshotWithinMarketWindow(snapshot, marketRecord);
    if (isWithinMarketWindow) {
      persistedSnapshot = { marketRecord, snapshot };
    } else {
      this.handleOutOfWindowSnapshot(snapshot, marketRecord);
    }
    return persistedSnapshot;
  }

  private async buildPersistedSnapshotBatch(snapshotBatch: readonly QueuedSnapshotEntry[]): Promise<PersistedSnapshotEntry[]> {
    const persistedBatch: PersistedSnapshotEntry[] = [];
    for (const queuedSnapshotEntry of snapshotBatch) {
      const snapshot = queuedSnapshotEntry.snapshot;
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
      this.updateStateSnapshotBatch(persistedBatch);
      await this.snapshotRepositoryService.insertSnapshots(persistedBatch.map((entry) => entry.snapshot));
    }
  }

  private updateStateSnapshotBatch(persistedBatch: readonly PersistedSnapshotEntry[]): void {
    for (const persistedSnapshot of persistedBatch) {
      this.stateStoreService.updateSnapshot(persistedSnapshot.marketRecord, persistedSnapshot.snapshot);
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
