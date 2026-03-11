/**
 * @section imports:internals
 */

import config from "../config.ts";
import LOGGER from "../logger.ts";

/**
 * @section consts
 */

const LOG_INTERVAL_MS = 5000;
const EVENT_LOOP_SAMPLE_INTERVAL_MS = 500;
const WARNING_COOLDOWN_MS = 5000;
const LAG_WARNING_THRESHOLD_MS = 1000;
const EVENT_LOOP_WARNING_THRESHOLD_MS = 250;
const CLICKHOUSE_WARNING_THRESHOLD_MS = 2000;

/**
 * @section types
 */

type SnapshotCollectorDiagnosticLogger = { info(message: string): void; warn(message: string): void };
type SnapshotCollectorDiagnosticIntervalHandle = unknown;
type SnapshotCollectorDiagnosticScheduler = {
  now(): number;
  scheduleRecurring(listener: () => void, delayMs: number): SnapshotCollectorDiagnosticIntervalHandle;
  cancelRecurring(handle: SnapshotCollectorDiagnosticIntervalHandle): void;
};
type SnapshotCollectorDiagnosticMetric = { sampleCount: number; totalMs: number; maxMs: number };
type SnapshotCollectorDiagnosticServiceOptions = {
  logger: SnapshotCollectorDiagnosticLogger;
  scheduler: SnapshotCollectorDiagnosticScheduler;
  snapshotBatchSize: number;
  logIntervalMs: number;
  eventLoopSampleIntervalMs: number;
  warningCooldownMs: number;
};

/**
 * @section private:properties
 */

export class SnapshotCollectorDiagnosticService {
  private readonly logger: SnapshotCollectorDiagnosticLogger;
  private readonly scheduler: SnapshotCollectorDiagnosticScheduler;
  private readonly snapshotBatchSize: number;
  private readonly logIntervalMs: number;
  private readonly eventLoopSampleIntervalMs: number;
  private readonly warningCooldownMs: number;
  private isStarted = false;
  private logIntervalHandle: SnapshotCollectorDiagnosticIntervalHandle | null = null;
  private eventLoopIntervalHandle: SnapshotCollectorDiagnosticIntervalHandle | null = null;
  private nextExpectedEventLoopSampleAtMs = 0;
  private windowStartedAtMs = 0;
  private currentQueueDepth = 0;
  private maxQueueDepth = 0;
  private receivedCount = 0;
  private dashboardUpdatedCount = 0;
  private persistedCount = 0;
  private readonly listenerIngressLagMetric = this.createMetric();
  private readonly queueWaitLagMetric = this.createMetric();
  private readonly dashboardLagMetric = this.createMetric();
  private readonly persistenceDurationMetric = this.createMetric();
  private readonly persistenceLagMetric = this.createMetric();
  private readonly eventLoopLagMetric = this.createMetric();
  private readonly lastWarningAtByStage = new Map<string, number>();

  /**
   * @section constructor
   */

  public constructor(options: SnapshotCollectorDiagnosticServiceOptions) {
    this.logger = options.logger;
    this.scheduler = options.scheduler;
    this.snapshotBatchSize = options.snapshotBatchSize;
    this.logIntervalMs = options.logIntervalMs;
    this.eventLoopSampleIntervalMs = options.eventLoopSampleIntervalMs;
    this.warningCooldownMs = options.warningCooldownMs;
  }

  /**
   * @section factory
   */

  public static createDefault(): SnapshotCollectorDiagnosticService {
    return new SnapshotCollectorDiagnosticService({
      logger: LOGGER,
      scheduler: {
        now() {
          return Date.now();
        },
        scheduleRecurring(listener, delayMs) {
          return setInterval(listener, delayMs);
        },
        cancelRecurring(handle) {
          clearInterval(handle as ReturnType<typeof setInterval>);
        },
      },
      snapshotBatchSize: config.SNAPSHOT_INSERT_BATCH_MAX_SIZE,
      logIntervalMs: LOG_INTERVAL_MS,
      eventLoopSampleIntervalMs: EVENT_LOOP_SAMPLE_INTERVAL_MS,
      warningCooldownMs: WARNING_COOLDOWN_MS,
    });
  }

  /**
   * @section private:methods
   */

  private createMetric(): SnapshotCollectorDiagnosticMetric {
    const metric: SnapshotCollectorDiagnosticMetric = { sampleCount: 0, totalMs: 0, maxMs: 0 };
    return metric;
  }

  private resetMetric(metric: SnapshotCollectorDiagnosticMetric): void {
    metric.sampleCount = 0;
    metric.totalMs = 0;
    metric.maxMs = 0;
  }

  private trackMetric(metric: SnapshotCollectorDiagnosticMetric, valueMs: number): void {
    metric.sampleCount += 1;
    metric.totalMs += valueMs;
    metric.maxMs = Math.max(metric.maxMs, valueMs);
  }

  private updateQueueDepth(queueDepth: number): void {
    this.currentQueueDepth = queueDepth;
    this.maxQueueDepth = Math.max(this.maxQueueDepth, queueDepth);
  }

  private warnIfNeeded(stage: string, isThresholdExceeded: boolean, message: string): void {
    if (isThresholdExceeded) {
      const nowMs = this.scheduler.now();
      const hasPreviousWarning = this.lastWarningAtByStage.has(stage);
      const lastWarningAtMs = this.lastWarningAtByStage.get(stage) || 0;
      const canWarn = !hasPreviousWarning || nowMs - lastWarningAtMs >= this.warningCooldownMs;
      if (canWarn) {
        this.lastWarningAtByStage.set(stage, nowMs);
        this.logger.warn(message);
      }
    }
  }

  private readAverageMs(metric: SnapshotCollectorDiagnosticMetric): number {
    const averageMs = metric.sampleCount > 0 ? Math.round(metric.totalMs / metric.sampleCount) : 0;
    return averageMs;
  }

  private buildMetricSummary(label: string, metric: SnapshotCollectorDiagnosticMetric): string {
    const summary = `${label}_avg_ms=${this.readAverageMs(metric)} ${label}_max_ms=${metric.maxMs}`;
    return summary;
  }

  private resetWindowMetrics(): void {
    this.windowStartedAtMs = this.scheduler.now();
    this.maxQueueDepth = this.currentQueueDepth;
    this.receivedCount = 0;
    this.dashboardUpdatedCount = 0;
    this.persistedCount = 0;
    this.resetMetric(this.listenerIngressLagMetric);
    this.resetMetric(this.queueWaitLagMetric);
    this.resetMetric(this.dashboardLagMetric);
    this.resetMetric(this.persistenceDurationMetric);
    this.resetMetric(this.persistenceLagMetric);
    this.resetMetric(this.eventLoopLagMetric);
  }

  private logWindowSummary(): void {
    const windowDurationMs = this.scheduler.now() - this.windowStartedAtMs;
    this.logger.info(
      `snapshot diagnostics window_ms=${windowDurationMs} received_count=${this.receivedCount} dashboard_updated_count=${this.dashboardUpdatedCount} persisted_count=${this.persistedCount} queue_depth_current=${this.currentQueueDepth} queue_depth_max=${this.maxQueueDepth} ${this.buildMetricSummary("ingress_lag", this.listenerIngressLagMetric)} ${this.buildMetricSummary("queue_wait", this.queueWaitLagMetric)} ${this.buildMetricSummary("dashboard_lag", this.dashboardLagMetric)} ${this.buildMetricSummary("insert", this.persistenceDurationMetric)} ${this.buildMetricSummary("persist_lag", this.persistenceLagMetric)} ${this.buildMetricSummary("event_loop_lag", this.eventLoopLagMetric)}`,
    );
    this.resetWindowMetrics();
  }

  private sampleEventLoopLag(): void {
    const actualNowMs = this.scheduler.now();
    const eventLoopLagMs = Math.max(actualNowMs - this.nextExpectedEventLoopSampleAtMs, 0);
    this.nextExpectedEventLoopSampleAtMs = actualNowMs + this.eventLoopSampleIntervalMs;
    this.trackMetric(this.eventLoopLagMetric, eventLoopLagMs);
    this.warnIfNeeded(
      "event_loop",
      eventLoopLagMs > EVENT_LOOP_WARNING_THRESHOLD_MS,
      `snapshot diagnostics warning stage=event_loop event_loop_lag_ms=${eventLoopLagMs}`,
    );
  }

  /**
   * @section public:methods
   */

  public start(): void {
    if (!this.isStarted) {
      this.isStarted = true;
      this.resetWindowMetrics();
      this.nextExpectedEventLoopSampleAtMs = this.scheduler.now() + this.eventLoopSampleIntervalMs;
      this.logIntervalHandle = this.scheduler.scheduleRecurring(() => {
        this.logWindowSummary();
      }, this.logIntervalMs);
      this.eventLoopIntervalHandle = this.scheduler.scheduleRecurring(() => {
        this.sampleEventLoopLag();
      }, this.eventLoopSampleIntervalMs);
    }
  }

  public stop(): void {
    if (this.isStarted) {
      if (this.logIntervalHandle) {
        this.scheduler.cancelRecurring(this.logIntervalHandle);
        this.logIntervalHandle = null;
      }
      if (this.eventLoopIntervalHandle) {
        this.scheduler.cancelRecurring(this.eventLoopIntervalHandle);
        this.eventLoopIntervalHandle = null;
      }
      this.logWindowSummary();
      this.isStarted = false;
    }
  }

  public recordListenerIngress(snapshotGeneratedAtMs: number, listenerReceivedAtMs: number, queueDepth: number): void {
    if (this.isStarted) {
      const listenerIngressLagMs = Math.max(listenerReceivedAtMs - snapshotGeneratedAtMs, 0);
      this.receivedCount += 1;
      this.updateQueueDepth(queueDepth);
      this.trackMetric(this.listenerIngressLagMetric, listenerIngressLagMs);
      this.warnIfNeeded(
        "listener",
        listenerIngressLagMs > LAG_WARNING_THRESHOLD_MS,
        `snapshot diagnostics warning stage=listener ingress_lag_ms=${listenerIngressLagMs} queue_depth=${queueDepth}`,
      );
      this.warnIfNeeded(
        "queue",
        queueDepth > this.snapshotBatchSize * 2,
        `snapshot diagnostics warning stage=queue queue_depth=${queueDepth} queue_depth_limit=${this.snapshotBatchSize * 2}`,
      );
    }
  }

  public recordQueueWait(queueWaitMs: number, queueDepth: number): void {
    if (this.isStarted) {
      this.updateQueueDepth(queueDepth);
      this.trackMetric(this.queueWaitLagMetric, queueWaitMs);
      this.warnIfNeeded(
        "queue_wait",
        queueWaitMs > LAG_WARNING_THRESHOLD_MS,
        `snapshot diagnostics warning stage=queue queue_wait_ms=${queueWaitMs} queue_depth=${queueDepth}`,
      );
    }
  }

  public recordDashboardUpdate(snapshotGeneratedAtMs: number): void {
    if (this.isStarted) {
      const dashboardLagMs = Math.max(this.scheduler.now() - snapshotGeneratedAtMs, 0);
      this.dashboardUpdatedCount += 1;
      this.trackMetric(this.dashboardLagMetric, dashboardLagMs);
      this.warnIfNeeded(
        "dashboard",
        dashboardLagMs > LAG_WARNING_THRESHOLD_MS,
        `snapshot diagnostics warning stage=dashboard dashboard_lag_ms=${dashboardLagMs}`,
      );
    }
  }

  public recordPersistence(insertDurationMs: number, lastSnapshotGeneratedAtMs: number | null, persistedCount: number, queueDepth: number): void {
    if (this.isStarted) {
      this.persistedCount += persistedCount;
      this.updateQueueDepth(queueDepth);
      this.trackMetric(this.persistenceDurationMetric, insertDurationMs);
      this.warnIfNeeded(
        "clickhouse",
        insertDurationMs > CLICKHOUSE_WARNING_THRESHOLD_MS,
        `snapshot diagnostics warning stage=clickhouse insert_duration_ms=${insertDurationMs} batch_size=${persistedCount}`,
      );
      if (lastSnapshotGeneratedAtMs !== null) {
        const persistenceLagMs = Math.max(this.scheduler.now() - lastSnapshotGeneratedAtMs, 0);
        this.trackMetric(this.persistenceLagMetric, persistenceLagMs);
      }
    }
  }
}
