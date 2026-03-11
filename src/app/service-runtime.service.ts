/**
 * @section imports:externals
 */

import type { Server } from "node:http";

/**
 * @section imports:internals
 */

import { AppInfoService } from "../app-info/app-info.service.ts";
import { ClickhouseClientService } from "../clickhouse/clickhouse-client.service.ts";
import { ClickhouseSchemaService } from "../clickhouse/clickhouse-schema.service.ts";
import { SnapshotCollectorService } from "../collector/snapshot-collector.service.ts";
import config from "../config.ts";
import { HttpServerService } from "../http/http-server.service.ts";
import LOGGER from "../logger.ts";
import { MarketRepositoryService } from "../market/market-repository.service.ts";
import { SnapshotDeduplicationService } from "../snapshot/snapshot-deduplication.service.ts";
import { SnapshotQueryService } from "../snapshot/snapshot-query.service.ts";
import { SnapshotRepositoryService } from "../snapshot/snapshot-repository.service.ts";

/**
 * @section types
 */

type ServiceRuntimeOptions = {
  clickhouseClientService: ClickhouseClientService;
  clickhouseSchemaService: ClickhouseSchemaService;
  snapshotCollectorService: SnapshotCollectorService;
  httpServerService: HttpServerService;
};

/**
 * @section private:properties
 */

export class ServiceRuntime {
  private readonly clickhouseClientService: ClickhouseClientService;
  private readonly clickhouseSchemaService: ClickhouseSchemaService;
  private readonly snapshotCollectorService: SnapshotCollectorService;
  private readonly httpServerService: HttpServerService;
  private activeServer: Server | null = null;

  /**
   * @section constructor
   */

  public constructor(options: ServiceRuntimeOptions) {
    this.clickhouseClientService = options.clickhouseClientService;
    this.clickhouseSchemaService = options.clickhouseSchemaService;
    this.snapshotCollectorService = options.snapshotCollectorService;
    this.httpServerService = options.httpServerService;
  }

  /**
   * @section factory
   */

  public static createDefault(): ServiceRuntime {
    const clickhouseClientService = ClickhouseClientService.createDefault();
    const clickhouseSchemaService = new ClickhouseSchemaService({ clickhouseClientService });
    const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });
    const snapshotRepositoryService = new SnapshotRepositoryService({ clickhouseClientService });
    const snapshotDeduplicationService = SnapshotDeduplicationService.createDefault();
    const snapshotQueryService = new SnapshotQueryService({ marketRepositoryService, snapshotRepositoryService });
    const snapshotCollectorService = SnapshotCollectorService.createDefault({ marketRepositoryService, snapshotRepositoryService, snapshotDeduplicationService });
    const httpServerService = new HttpServerService({ appInfoService: AppInfoService.createDefault(), snapshotQueryService });
    return new ServiceRuntime({ clickhouseClientService, clickhouseSchemaService, snapshotCollectorService, httpServerService });
  }

  /**
   * @section private:methods
   */

  private async listen(server: Server): Promise<Server> {
    const listenedServer = await new Promise<Server>((resolve, reject) => {
      server.listen(config.DEFAULT_PORT, config.HTTP_HOST, () => {
        resolve(server);
      });
      server.once("error", (error) => {
        reject(error);
      });
    });
    return listenedServer;
  }

  private async closeServer(server: Server): Promise<void> {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * @section public:methods
   */

  public buildServer(): Server {
    const server = this.httpServerService.buildServer();
    return server;
  }

  public async startServer(): Promise<Server> {
    await this.clickhouseSchemaService.ensureSchema();
    this.snapshotCollectorService.start();
    const server = this.buildServer();
    const listenedServer = await this.listen(server);
    this.activeServer = listenedServer;
    LOGGER.info(`service listening on http://${config.HTTP_HOST}:${config.DEFAULT_PORT}`);
    return listenedServer;
  }

  public async stop(): Promise<void> {
    const activeServer = this.activeServer;
    if (activeServer) {
      await this.closeServer(activeServer);
      this.activeServer = null;
    }
    await this.snapshotCollectorService.stop();
    await this.clickhouseClientService.close();
  }
}
