/**
 * @section imports:externals
 */

import type { CommandParams, InsertParams } from "@clickhouse/client";

/**
 * @section types
 */

export type ClickhouseJsonResultSet = {
  json<T>(): Promise<T[]>;
  close(): void;
};

export type ClickhouseDriver = {
  close(): Promise<void>;
  command(params: CommandParams): Promise<unknown>;
  insert<T>(params: InsertParams<unknown, T>): Promise<unknown>;
  query(params: { query: string; format: "JSONEachRow" }): Promise<ClickhouseJsonResultSet>;
};
