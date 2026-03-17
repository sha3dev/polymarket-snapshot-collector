/**
 * @section imports:internals
 */

import LOGGER from "../logger.ts";
import type { MigrationRepositoryService } from "./migration-repository.service.ts";

/**
 * @section types
 */

type MigrationOrchestratorServiceOptions = {
  migrationRepositoryService: MigrationRepositoryService;
  isMigrationMode: boolean;
};

const PROGRESS_LOG_INTERVAL_MS = 60_000;

/**
 * @section class
 */

export class MigrationOrchestratorService {
  /**
   * @section private:attributes
   */

  private readonly migrationRepositoryService: MigrationRepositoryService;
  private readonly isMigrationMode: boolean;
  private activeMigrationPromise: Promise<void> | null = null;
  private progressLogTimer: NodeJS.Timeout | null = null;

  /**
   * @section constructor
   */

  public constructor(options: MigrationOrchestratorServiceOptions) {
    this.migrationRepositoryService = options.migrationRepositoryService;
    this.isMigrationMode = options.isMigrationMode;
  }

  /**
   * @section private:methods
   */

  private buildUtcDay(value: Date): string {
    const utcDay = value.toISOString().slice(0, 10);
    return utcDay;
  }

  private parseUtcDay(value: string): Date {
    const utcDay = new Date(`${value}T00:00:00.000Z`);
    return utcDay;
  }

  private addDays(value: Date, days: number): Date {
    const nextValue = new Date(value.getTime());
    nextValue.setUTCDate(nextValue.getUTCDate() + days);
    return nextValue;
  }

  private buildDayRange(startDay: string, endDay: string): string[] {
    const dayRange: string[] = [];
    let currentDay = this.parseUtcDay(startDay);
    const parsedEndDay = this.parseUtcDay(endDay);
    while (currentDay.getTime() <= parsedEndDay.getTime()) {
      dayRange.push(this.buildUtcDay(currentDay));
      currentDay = this.addDays(currentDay, 1);
    }
    return dayRange;
  }

  private buildResumeDay(minDay: string, lastCompletedDay: string | null): string {
    const resumeDay = lastCompletedDay ? this.buildUtcDay(this.addDays(this.parseUtcDay(lastCompletedDay), 1)) : minDay;
    return resumeDay;
  }

  private async migratePendingDays(): Promise<void> {
    const latestMigrationState = await this.migrationRepositoryService.readLatestMigrationState();
    const isAlreadyCompleted = latestMigrationState?.isCompleted === true;
    if (!isAlreadyCompleted) {
      const cutoffDay = this.buildUtcDay(new Date());
      const range = await this.migrationRepositoryService.readLegacySnapshotRange(cutoffDay);
      const hasHistoricalRange = range.minDay !== null && range.maxDay !== null;
      if (hasHistoricalRange) {
        const resumeDay = this.buildResumeDay(range.minDay || "", latestMigrationState?.lastCompletedDay || null);
        const dayRange = this.buildDayRange(resumeDay, range.maxDay || "");
        for (const day of dayRange) {
          await this.migrationRepositoryService.validateLegacySnapshotUniqueness(day);
          await this.migrationRepositoryService.migrateDay(day);
          await this.migrationRepositoryService.validateMigratedDay(day);
          await this.migrationRepositoryService.insertMigrationState("running", day, false, null);
        }
      }
      await this.migrationRepositoryService.insertMigrationState("completed", range.maxDay, true, null);
      LOGGER.info("snapshot legacy migration completed");
    }
  }

  private startProgressLogTimer(): void {
    if (this.progressLogTimer === null) {
      this.progressLogTimer = setInterval(() => {
        void this.runProgressLogTick();
      }, PROGRESS_LOG_INTERVAL_MS);
    }
  }

  private stopProgressLogTimer(): void {
    if (this.progressLogTimer !== null) {
      clearInterval(this.progressLogTimer);
      this.progressLogTimer = null;
    }
  }

  private async runProgressLogTick(): Promise<void> {
    try {
      const latestMigrationState = await this.migrationRepositoryService.readLatestMigrationState();
      const progressMessage =
        latestMigrationState === null
          ? "snapshot legacy migration progress: phase=starting lastCompletedDay=null isCompleted=0 error=null"
          : `snapshot legacy migration progress: phase=${latestMigrationState.phase} lastCompletedDay=${latestMigrationState.lastCompletedDay || "null"} isCompleted=${latestMigrationState.isCompleted ? "1" : "0"} error=${latestMigrationState.errorMessage || "null"}`;
      LOGGER.info(progressMessage);
    } catch (error) {
      LOGGER.error(`snapshot legacy migration progress log failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private async runMigrationLoop(): Promise<void> {
    this.startProgressLogTimer();
    try {
      await this.migrationRepositoryService.insertMigrationState("idle", null, false, null);
      await this.migratePendingDays();
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      await this.migrationRepositoryService.insertMigrationState("failed", null, false, errorMessage);
      LOGGER.error(`snapshot legacy migration failed: ${errorMessage}`);
      throw error;
    } finally {
      this.stopProgressLogTimer();
    }
  }

  /**
   * @section public:methods
   */

  public start(): void {
    if (this.isMigrationMode && this.activeMigrationPromise === null) {
      this.activeMigrationPromise = this.runMigrationLoop();
    }
  }

  public async stop(): Promise<void> {
    if (this.activeMigrationPromise !== null) {
      await this.activeMigrationPromise;
      this.activeMigrationPromise = null;
    }
  }
}
