import * as assert from "node:assert/strict";
import { test } from "node:test";

import type { CommandParams, InsertParams } from "@clickhouse/client";
import { ClickhouseClientService } from "../src/clickhouse/clickhouse-client.service.ts";
import { MigrationOrchestratorService } from "../src/migration/migration-orchestrator.service.ts";
import { MigrationRepositoryService } from "../src/migration/migration-repository.service.ts";
import { SnapshotFieldCatalogService } from "../src/snapshot/snapshot-field-catalog.service.ts";

type ClickhouseJsonResultSet = { json<T>(): Promise<T[]>; close(): void };
type ClickhouseDriver = {
  close(): Promise<void>;
  command(params: CommandParams): Promise<unknown>;
  insert<T>(params: InsertParams<unknown, T>): Promise<unknown>;
  query(params: { query: string; format: "JSONEachRow" }): Promise<ClickhouseJsonResultSet>;
};

function normalizeQuery(query: string): string {
  const normalizedQuery = query.replace(/\s+/g, " ").trim();
  return normalizedQuery;
}

function buildDriverDouble(): {
  commands: string[];
  inserts: Array<{ table: string; rows: unknown[] }>;
  queryResults: Map<string, unknown[]>;
  clickhouseDriver: ClickhouseDriver;
} {
  const commands: string[] = [];
  const inserts: Array<{ table: string; rows: unknown[] }> = [];
  const queryResults = new Map<string, unknown[]>();
  const clickhouseDriver: ClickhouseDriver = {
    async close() {},
    async command(params: CommandParams) {
      commands.push(params.query);
      return {};
    },
    async insert<T>(params: InsertParams<unknown, T>) {
      inserts.push({ table: params.table, rows: Array.isArray(params.values) ? [...params.values] : [] });
      return {};
    },
    async query(params: { query: string; format: "JSONEachRow" }) {
      return {
        async json<T>() {
          const matchedEntry = [...queryResults.entries()].find(([query]) => normalizeQuery(query) === normalizeQuery(params.query));
          return (matchedEntry?.[1] || []) as T[];
        },
        close() {},
      };
    },
  };
  return { commands, inserts, queryResults, clickhouseDriver };
}

test("MigrationRepositoryService generates migration SQL with market joins and market dates", async () => {
  const driverDouble = buildDriverDouble();
  const clickhouseClientService = new ClickhouseClientService({
    clickhouseDriver: driverDouble.clickhouseDriver,
    databaseName: "default",
  });
  const migrationRepositoryService = new MigrationRepositoryService({
    clickhouseClientService,
    snapshotFieldCatalogService: SnapshotFieldCatalogService.createDefault(),
  });

  await migrationRepositoryService.migrateDay("2026-03-11");

  assert.match(driverDouble.commands[0] || "", /DROP PARTITION/);
  assert.match(driverDouble.commands[1] || "", /LEFT JOIN default\.market/);
  assert.match(driverDouble.commands[1] || "", /btc_5m_market_start/);
  assert.match(driverDouble.commands[1] || "", /btc_5m_price_to_beat/);
});

test("MigrationOrchestratorService completes immediately when there is no legacy range", async () => {
  const insertedStates: string[] = [];
  const migrationOrchestratorService = new MigrationOrchestratorService({
    isMigrationMode: true,
    migrationRepositoryService: {
      async insertMigrationState(phase: string) {
        insertedStates.push(phase);
      },
      async readLatestMigrationState() {
        return null;
      },
      async readLegacySnapshotRange() {
        return { minDay: null, maxDay: null };
      },
    } as never,
  });

  migrationOrchestratorService.start();
  await migrationOrchestratorService.stop();

  assert.deepEqual(insertedStates, ["idle", "completed"]);
});

test("MigrationOrchestratorService migrates day by day and resumes from last completed day", async () => {
  const operations: string[] = [];
  const realDate = Date;
  global.Date = class extends realDate {
    public constructor(value?: string | number | Date) {
      super(value || "2026-03-12T12:00:00.000Z");
    }

    public static now(): number {
      return new realDate("2026-03-12T12:00:00.000Z").getTime();
    }
  } as DateConstructor;
  const migrationOrchestratorService = new MigrationOrchestratorService({
    isMigrationMode: true,
    migrationRepositoryService: {
      async insertMigrationState(phase: string, lastCompletedDay: string | null, isCompleted: boolean) {
        operations.push(`state:${phase}:${lastCompletedDay || "null"}:${isCompleted ? "1" : "0"}`);
      },
      async readLatestMigrationState() {
        return {
          migrationName: "snapshot_v2_backfill",
          phase: "running",
          lastCompletedDay: "2026-03-09",
          isCompleted: false,
          errorMessage: null,
          updatedAt: "2026-03-11T00:00:00.000Z",
        };
      },
      async readLegacySnapshotRange() {
        return { minDay: "2026-03-08", maxDay: "2026-03-11" };
      },
      async validateLegacySnapshotUniqueness(day: string) {
        operations.push(`validate-legacy:${day}`);
      },
      async migrateDay(day: string) {
        operations.push(`migrate:${day}`);
      },
      async validateMigratedDay(day: string) {
        operations.push(`validate-target:${day}`);
      },
    } as never,
  });

  try {
    migrationOrchestratorService.start();
    await migrationOrchestratorService.stop();
  } finally {
    global.Date = realDate;
  }

  assert.match(operations.join("|"), /migrate:2026-03-10/);
  assert.match(operations.join("|"), /migrate:2026-03-11/);
  assert.match(operations.join("|"), /state:completed:2026-03-11:1/);
});
