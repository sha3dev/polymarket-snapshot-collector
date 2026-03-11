/**
 * @section imports:externals
 */

import type { Snapshot } from "@sha3/polymarket-snapshot";

/**
 * @section imports:internals
 */

import config from "../config.ts";
import { SnapshotConsistencyService } from "./snapshot-consistency.service.ts";
import type { SnapshotFingerprintEntry } from "./snapshot.types.ts";

/**
 * @section types
 */

type SnapshotDeduplicationServiceOptions = { ttlMs: number; maxKeys: number };

/**
 * @section private:attributes
 */

export class SnapshotDeduplicationService {
  private readonly ttlMs: number;
  private readonly maxKeys: number;
  private readonly cleanupIntervalMs: number;
  private lastCleanupAtMs = 0;

  /**
   * @section private:properties
   */

  private readonly fingerprintByKey = new Map<string, SnapshotFingerprintEntry>();

  /**
   * @section constructor
   */

  public constructor(options: SnapshotDeduplicationServiceOptions) {
    this.ttlMs = options.ttlMs;
    this.maxKeys = options.maxKeys;
    this.cleanupIntervalMs = Math.min(this.ttlMs, 5000);
  }

  /**
   * @section factory
   */

  public static createDefault(): SnapshotDeduplicationService {
    return new SnapshotDeduplicationService({ ttlMs: config.SNAPSHOT_DEDUPLICATION_TTL_MS, maxKeys: config.SNAPSHOT_DEDUPLICATION_MAX_KEYS });
  }

  /**
   * @section private:methods
   */

  private buildKey(snapshot: Snapshot): string {
    const key = `${snapshot.marketSlug || ""}|${snapshot.asset}|${snapshot.window}|${snapshot.generatedAt}`;
    return key;
  }

  private buildFingerprint(snapshot: Snapshot): string {
    const fingerprint = JSON.stringify(snapshot);
    return fingerprint;
  }

  private evictExpiredEntries(nowMs: number): void {
    for (const [key, entry] of this.fingerprintByKey.entries()) {
      const isExpired = nowMs - entry.storedAt > this.ttlMs;
      if (isExpired) {
        this.fingerprintByKey.delete(key);
      }
    }
  }

  private trimToMaxKeys(): void {
    while (this.fingerprintByKey.size > this.maxKeys) {
      const oldestKey = this.fingerprintByKey.keys().next().value;
      if (oldestKey) {
        this.fingerprintByKey.delete(oldestKey);
      }
    }
  }

  private evictExpiredEntriesIfNeeded(nowMs: number): void {
    const shouldRunCleanup = nowMs - this.lastCleanupAtMs >= this.cleanupIntervalMs;
    if (shouldRunCleanup) {
      this.evictExpiredEntries(nowMs);
      this.lastCleanupAtMs = nowMs;
    }
  }

  /**
   * @section public:methods
   */

  public shouldPersist(snapshot: Snapshot): boolean {
    const nowMs = Date.now();
    const key = this.buildKey(snapshot);
    const fingerprint = this.buildFingerprint(snapshot);
    const existingEntry = this.fingerprintByKey.get(key) || null;
    let shouldPersist = true;

    this.evictExpiredEntriesIfNeeded(nowMs);
    if (existingEntry) {
      const isSameFingerprint = existingEntry.fingerprint === fingerprint;
      if (isSameFingerprint) {
        shouldPersist = false;
      } else {
        throw new SnapshotConsistencyService(`duplicate snapshot identity with different payload for key ${key}`);
      }
    }
    if (shouldPersist) {
      this.fingerprintByKey.set(key, { fingerprint, storedAt: nowMs });
      this.trimToMaxKeys();
    }
    return shouldPersist;
  }
}
