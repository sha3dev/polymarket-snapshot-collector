import * as assert from "node:assert/strict";
import { test } from "node:test";

import { SnapshotCollectorDiagnosticService } from "../src/collector/snapshot-collector-diagnostic.service.ts";

type IntervalHandle = { intervalId: number };
type IntervalEntry = { delayMs: number; listener: () => void; handle: IntervalHandle };

function buildDiagnosticHarness(startedAtMs = 1000): {
  advanceTo(nowMs: number): void;
  infoMessages: string[];
  runIntervals(delayMs: number): void;
  scheduler: {
    now(): number;
    scheduleRecurring(listener: () => void, delayMs: number): IntervalHandle;
    cancelRecurring(handle: IntervalHandle): void;
  };
  warnMessages: string[];
} {
  const intervalEntries: IntervalEntry[] = [];
  const messageCollector = buildMessageCollector();
  const scheduler = buildSchedulerDouble(intervalEntries, startedAtMs);
  return {
    advanceTo(nextNowMs: number): void {
      scheduler.advanceTo(nextNowMs);
    },
    infoMessages: messageCollector.infoMessages,
    runIntervals(delayMs: number): void {
      for (const intervalEntry of intervalEntries.filter((entry) => entry.delayMs === delayMs)) {
        intervalEntry.listener();
      }
    },
    scheduler: scheduler.scheduler,
    warnMessages: messageCollector.warnMessages,
  };
}

function buildMessageCollector(): { infoMessages: string[]; warnMessages: string[] } {
  return { infoMessages: [], warnMessages: [] };
}

function buildSchedulerDouble(
  intervalEntries: IntervalEntry[],
  startedAtMs: number,
): {
  advanceTo(nowMs: number): void;
  scheduler: {
    now(): number;
    scheduleRecurring(listener: () => void, delayMs: number): IntervalHandle;
    cancelRecurring(handle: IntervalHandle): void;
  };
} {
  let nowMs = startedAtMs;
  let nextHandleId = 1;
  return {
    advanceTo(nextNowMs: number): void {
      nowMs = nextNowMs;
    },
    scheduler: {
      now() {
        return nowMs;
      },
      scheduleRecurring(listener: () => void, delayMs: number): IntervalHandle {
        const handle = { intervalId: nextHandleId };
        nextHandleId += 1;
        intervalEntries.push({ delayMs, listener, handle });
        return handle;
      },
      cancelRecurring(handle: IntervalHandle): void {
        const intervalEntryIndex = intervalEntries.findIndex((intervalEntry) => intervalEntry.handle === handle);
        if (intervalEntryIndex >= 0) {
          intervalEntries.splice(intervalEntryIndex, 1);
        }
      },
    },
  };
}

function buildDiagnosticLogger(diagnosticHarness: ReturnType<typeof buildDiagnosticHarness>): {
  info(message: string): void;
  warn(message: string): void;
} {
  return {
    info(message: string): void {
      diagnosticHarness.infoMessages.push(message);
    },
    warn(message: string): void {
      diagnosticHarness.warnMessages.push(message);
    },
  };
}

function createDiagnosticService(diagnosticHarness: ReturnType<typeof buildDiagnosticHarness>): SnapshotCollectorDiagnosticService {
  return new SnapshotCollectorDiagnosticService({
    logger: buildDiagnosticLogger(diagnosticHarness),
    scheduler: diagnosticHarness.scheduler,
    snapshotBatchSize: 16,
    logIntervalMs: 5000,
    eventLoopSampleIntervalMs: 500,
    warningCooldownMs: 5000,
  });
}

test("SnapshotCollectorDiagnosticService aggregates lag metrics and samples event loop delay", () => {
  const diagnosticHarness = buildDiagnosticHarness();
  const snapshotCollectorDiagnosticService = createDiagnosticService(diagnosticHarness);

  snapshotCollectorDiagnosticService.start();
  snapshotCollectorDiagnosticService.recordListenerIngress(1000, 1400, 3);
  diagnosticHarness.advanceTo(1900);
  diagnosticHarness.runIntervals(500);
  snapshotCollectorDiagnosticService.recordQueueWait(700, 2);
  snapshotCollectorDiagnosticService.recordDashboardUpdate(1200);
  diagnosticHarness.advanceTo(4300);
  snapshotCollectorDiagnosticService.recordPersistence(2300, 1500, 2, 1);
  diagnosticHarness.advanceTo(6000);
  diagnosticHarness.runIntervals(5000);
  snapshotCollectorDiagnosticService.stop();

  assert.equal(diagnosticHarness.warnMessages.length, 2);
  assert.match(diagnosticHarness.warnMessages[0] || "", /stage=event_loop/);
  assert.match(diagnosticHarness.warnMessages[1] || "", /stage=clickhouse/);
  assert.equal(diagnosticHarness.infoMessages.length, 2);
  assert.match(diagnosticHarness.infoMessages[0] || "", /received_count=1/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /dashboard_updated_count=1/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /persisted_count=2/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /ingress_lag_avg_ms=400/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /queue_wait_avg_ms=700/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /dashboard_lag_avg_ms=700/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /insert_avg_ms=2300/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /persist_lag_avg_ms=2800/);
  assert.match(diagnosticHarness.infoMessages[0] || "", /event_loop_lag_avg_ms=400/);
});
