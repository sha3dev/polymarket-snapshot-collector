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

type ClickhouseClientServiceOptions = {
  clickhouseDriver: ClickhouseDriver;
  databaseName: string;
};

/**
 * @section class
 */

export class ClickhouseClientService {
  /**
   * @section private:attributes
   */

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
    const clickhouseClientService = new ClickhouseClientService({
      clickhouseDriver: ClickhouseClientService.buildDefaultDriver(),
      databaseName: config.CLICKHOUSE_DATABASE,
    });
    return clickhouseClientService;
  }

  /**
   * @section private:methods
   */

  private static buildJsonResultSet(resultSet: Awaited<ReturnType<ReturnType<typeof createClient>["query"]>>): ClickhouseJsonResultSet {
    const clickhouseJsonResultSet: ClickhouseJsonResultSet = {
      async json<T>(): Promise<T[]> {
        const rows = (await resultSet.json<T>()) as T[];
        return rows;
      },
      close(): void {
        resultSet.close();
      },
    };
    return clickhouseJsonResultSet;
  }

  private static buildDefaultDriver(): ClickhouseDriver {
    const clickhouseClient = createClient({
      url: config.CLICKHOUSE_URL,
      database: config.CLICKHOUSE_DATABASE,
      username: config.CLICKHOUSE_USER,
      password: config.CLICKHOUSE_PASSWORD,
    });
    const clickhouseDriver: ClickhouseDriver = {
      async close(): Promise<void> {
        await clickhouseClient.close();
      },
      async command(params: CommandParams): Promise<unknown> {
        const commandResult = await clickhouseClient.command(params);
        return commandResult;
      },
      async insert<T>(params: InsertParams<unknown, T>): Promise<unknown> {
        const insertResult = await clickhouseClient.insert(params);
        return insertResult;
      },
      async query(params: { query: string; format: "JSONEachRow" }): Promise<ClickhouseJsonResultSet> {
        const resultSet = await clickhouseClient.query({ ...params, format: "JSONEachRow" });
        const clickhouseJsonResultSet = ClickhouseClientService.buildJsonResultSet(resultSet);
        return clickhouseJsonResultSet;
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
    await this.clickhouseDriver.insert({
      table: qualifiedTableName,
      values: [...rows],
      format: "JSONEachRow",
    });
  }

  public async queryJsonRows<RowShape extends Record<string, unknown>>(query: string): Promise<RowShape[]> {
    const resultSet = await this.clickhouseDriver.query({ query, format: "JSONEachRow" });
    const rows = await resultSet.json<RowShape>();
    resultSet.close();
    return rows;
  }
}
