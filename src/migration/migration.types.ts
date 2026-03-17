/**
 * @section types
 */

export type MigrationPhase = "idle" | "running" | "failed" | "completed";

export type MigrationState = {
  migrationName: string;
  phase: MigrationPhase;
  lastCompletedDay: string | null;
  isCompleted: boolean;
  errorMessage: string | null;
  updatedAt: string;
};

export type MigrationStateRecord = {
  migration_name: string;
  phase: MigrationPhase;
  last_completed_day: string | null;
  is_completed: number;
  error_message: string | null;
  updated_at: string;
};

export type LegacySnapshotRangeRow = {
  min_day: string | null;
  max_day: string | null;
};

export type CountRow = { count: number };
