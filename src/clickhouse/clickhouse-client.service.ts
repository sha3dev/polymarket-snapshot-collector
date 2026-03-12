/**
 * @section imports:externals
 */

import { createClient } from "@clickhouse/client";
import type { CommandParams, InsertParams } from "@clickhouse/client";

/**
 * @section imports:internals
 */

import config from "../config.ts";

/**
 * @section types
 */

type ClickhouseJsonResultSet = {
  json<T>(): Promise<T[]>;
  close(): void;
};
type ClickhouseDriver = {
  close(): Promise<void>;
  command(params: CommandParams): Promise<unknown>;
  insert<T>(params: InsertParams<unknown, T>): Promise<unknown>;
  query(params: { query: string; format: "JSONEachRow" }): Promise<ClickhouseJsonResultSet>;
};
type ClickhouseClientServiceOptions = { clickhouseDriver: ClickhouseDriver; databaseName: string };

/**
 * @section private:properties
 */

export class ClickhouseClientService {
  private readonly clickhouseDriver: ClickhouseDriver;
  private readonly databaseName: string;

  /**
   * @section constructor
   */

  public constructor(options: ClickhouseClientServiceOptions) {
    this.clickhouseDriver = options.clickhouseDriver;
    this.databaseName = options.databaseName;
  }

  /**
   * @section factory
   */

  public static createDefault(): ClickhouseClientService {
    const clickhouseDriver = ClickhouseClientService.buildDefaultDriver();
    return new ClickhouseClientService({ clickhouseDriver, databaseName: config.CLICKHOUSE_DATABASE });
  }

  /**
   * @section private:methods
   */

  private static buildJsonResultSet(resultSet: Awaited<ReturnType<ReturnType<typeof createClient>["query"]>>): ClickhouseJsonResultSet {
    return { async json<T>(): Promise<T[]> { return (await resultSet.json<T>()) as T[]; }, close(): void { resultSet.close(); } };
  }

  private static buildDefaultDriver(): ClickhouseDriver {
    const clickhouseClient = createClient({ url: config.CLICKHOUSE_URL, database: config.CLICKHOUSE_DATABASE, username: config.CLICKHOUSE_USERNAME, password: config.CLICKHOUSE_PASSWORD });
    const clickhouseDriver: ClickhouseDriver = {
      async close(): Promise<void> {
        await clickhouseClient.close();
      },
      async command(params): Promise<unknown> {
        return await clickhouseClient.command(params);
      },
      async insert(params): Promise<unknown> {
        return await clickhouseClient.insert(params);
      },
      async query(params): Promise<ClickhouseJsonResultSet> {
        const resultSet = await clickhouseClient.query({ ...params, format: "JSONEachRow" });
        return ClickhouseClientService.buildJsonResultSet(resultSet);
      },
    };
    return clickhouseDriver;
  }

  private buildQualifiedTableName(tableName: string): string {
    const qualifiedTableName = `${this.databaseName}.${tableName}`;
    return qualifiedTableName;
  }

  /**
   * @section public:methods
   */

  public async close(): Promise<void> {
    await this.clickhouseDriver.close();
  }

  public async command(query: string): Promise<void> {
    await this.clickhouseDriver.command({ query });
  }

  public async insertJsonRows<RowShape extends Record<string, unknown>>(tableName: string, rows: readonly RowShape[]): Promise<void> {
    const qualifiedTableName = this.buildQualifiedTableName(tableName);
    await this.clickhouseDriver.insert({ table: qualifiedTableName, values: [...rows], format: "JSONEachRow" });
  }

  public async queryJsonRows<RowShape extends Record<string, unknown>>(query: string): Promise<RowShape[]> {
    const resultSet = await this.clickhouseDriver.query({ query, format: "JSONEachRow" });
    const rows = await resultSet.json<RowShape>();
    resultSet.close();
    return rows;
  }
}
