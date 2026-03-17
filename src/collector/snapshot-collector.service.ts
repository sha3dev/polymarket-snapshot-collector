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
import type { MarketSyncService } from "../market/market-sync.service.ts";
import type { FlatSnapshotRepositoryService } from "../snapshot/flat-snapshot-repository.service.ts";

/**
 * @section types
 */

type SnapshotCollectorRuntime = Pick<SnapshotService, "addSnapshotListener" | "removeSnapshotListener" | "disconnect">;

type SnapshotCollectorServiceOptions = {
  flatSnapshotRepositoryService: FlatSnapshotRepositoryService;
  marketSyncService: MarketSyncService;
  snapshotRuntime: SnapshotCollectorRuntime;
};

/**
 * @section class
 */

export class SnapshotCollectorService {
  /**
   * @section private:attributes
   */

  private readonly flatSnapshotRepositoryService: FlatSnapshotRepositoryService;
  private readonly marketSyncService: MarketSyncService;
  private readonly snapshotRuntime: SnapshotCollectorRuntime;
  private readonly snapshotBatchSize = config.SNAPSHOT_BATCH_SIZE;
  private readonly flushIntervalMs = config.SNAPSHOT_FLUSH_INTERVAL_MS;
  private readonly pendingSnapshots: Snapshot[] = [];
  private isStarted = false;
  private activeFlushPromise: Promise<void> | null = null;
  private flushTimer: NodeJS.Timeout | null = null;
  private readonly snapshotListener = (snapshot: Snapshot): void => {
    this.enqueueSnapshot(snapshot);
  };

  /**
   * @section constructor
   */

  public constructor(options: SnapshotCollectorServiceOptions) {
    this.flatSnapshotRepositoryService = options.flatSnapshotRepositoryService;
    this.marketSyncService = options.marketSyncService;
    this.snapshotRuntime = options.snapshotRuntime;
  }

  /**
   * @section factory
   */

  public static createDefault(options: Omit<SnapshotCollectorServiceOptions, "snapshotRuntime">): SnapshotCollectorService {
    const snapshotCollectorService = new SnapshotCollectorService({
      ...options,
      snapshotRuntime: new SnapshotService(config.SNAPSHOT_INTERVAL_MS),
    });
    return snapshotCollectorService;
  }

  /**
   * @section private:methods
   */

  private enqueueSnapshot(snapshot: Snapshot): void {
    this.pendingSnapshots.push(snapshot);
    if (this.pendingSnapshots.length >= this.snapshotBatchSize) {
      this.clearFlushTimer();
      this.ensureFlushStarted();
    } else {
      this.scheduleFlush();
    }
  }

  private scheduleFlush(): void {
    if (this.flushTimer === null) {
      this.flushTimer = setTimeout(() => {
        this.flushTimer = null;
        this.ensureFlushStarted();
      }, this.flushIntervalMs);
    }
  }

  private clearFlushTimer(): void {
    if (this.flushTimer !== null) {
      clearTimeout(this.flushTimer);
      this.flushTimer = null;
    }
  }

  private ensureFlushStarted(): void {
    if (this.activeFlushPromise === null) {
      this.activeFlushPromise = this.runFlushLoop();
    }
  }

  private async runFlushLoop(): Promise<void> {
    try {
      while (this.pendingSnapshots.length > 0) {
        const snapshots = this.pendingSnapshots.splice(0, this.snapshotBatchSize);
        await this.persistSnapshotBatch(snapshots);
      }
    } catch (error) {
      LOGGER.error(`snapshot flush failed: ${error instanceof Error ? error.message : String(error)}`);
      queueMicrotask(() => {
        throw error instanceof Error ? error : new Error(String(error));
      });
    } finally {
      this.activeFlushPromise = null;
      if (this.pendingSnapshots.length > 0) {
        this.ensureFlushStarted();
      }
    }
  }

  private async syncMarketSnapshots(snapshots: readonly Snapshot[]): Promise<void> {
    for (const snapshot of snapshots) {
      try {
        await this.marketSyncService.syncSnapshot(snapshot);
      } catch (error) {
        LOGGER.error(`market sync failed for generated_at=${snapshot.generated_at}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }

  private async persistSnapshotBatch(snapshots: readonly Snapshot[]): Promise<void> {
    await this.flatSnapshotRepositoryService.insertSnapshots(snapshots);
    await this.syncMarketSnapshots(snapshots);
  }

  /**
   * @section public:methods
   */

  public start(): void {
    if (!this.isStarted) {
      this.snapshotRuntime.addSnapshotListener({ listener: this.snapshotListener });
      this.isStarted = true;
      LOGGER.info("snapshot collector subscribed");
    }
  }

  public async stop(): Promise<void> {
    this.clearFlushTimer();
    if (this.isStarted) {
      this.snapshotRuntime.removeSnapshotListener(this.snapshotListener);
      await this.snapshotRuntime.disconnect();
      this.isStarted = false;
    }
    if (this.pendingSnapshots.length > 0) {
      this.ensureFlushStarted();
    }
    if (this.activeFlushPromise !== null) {
      await this.activeFlushPromise;
    }
  }
}
