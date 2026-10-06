import { createLogger, type LoggerOptions, type LoggerStatus } from "./logging";
import type { TraceContext } from "./shared";

export type ErrorService = "app" | "collector";
export interface ErrorReport { service: ErrorService; operation: string; category: string; error: unknown; stack?: string; trace?: Partial<TraceContext>; context?: Record<string, unknown>; }
export interface ErrorReporter { report(report: ErrorReport): Promise<string>; flush(): Promise<void>; getStatus(): LoggerStatus; }
export interface ErrorReporterOptions extends Omit<LoggerOptions, "service" | "directory" | "filename"> {}

const contextKeys = new Set(["activityId", "attempt", "backend", "captureMode", "clipKey", "operationId", "originalTextBytes", "retry", "status"]);
function context(value: Record<string, unknown> | undefined): Record<string, string | number | boolean> | undefined {
  if (!value) return undefined;
  const kept: Record<string, string | number | boolean> = {};
  for (const [key, item] of Object.entries(value)) if (contextKeys.has(key) && (typeof item === "string" || typeof item === "number" || typeof item === "boolean")) kept[key] = typeof item === "string" ? item.slice(0, 128) : item;
  return Object.keys(kept).length ? kept : undefined;
}

export function createErrorReporter(options: ErrorReporterOptions = {}): ErrorReporter {
  const logger = createLogger({ ...options, service: "app", directory: "errors", filename: "reports.jsonl", serializeAcrossProcesses: true });
  return {
    async report(report) {
      const reportId = crypto.randomUUID().replace(/-/g, "");
      const error = report.error instanceof Error ? report.error : new Error(String(report.error));
      await logger.log("error", { operation: report.operation, message: error.message, stack: report.stack ?? error.stack, category: report.category, reportId, reportService: report.service, sourceId: report.trace?.sourceId, participantId: report.trace?.participantId, jobId: report.trace?.jobId, traceId: report.trace?.traceId, spanId: report.trace?.spanId, metadata: context(report.context) });
      return reportId;
    },
    flush: () => logger.flush(),
    getStatus: () => logger.getStatus(),
  };
}
