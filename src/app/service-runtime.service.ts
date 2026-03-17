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
import { MarketSyncService } from "../market/market-sync.service.ts";
import { FlatSnapshotRepositoryService } from "../snapshot/flat-snapshot-repository.service.ts";
import { SnapshotFieldCatalogService } from "../snapshot/snapshot-field-catalog.service.ts";
import { SnapshotQueryService } from "../snapshot/snapshot-query.service.ts";

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
 * @section class
 */

export class ServiceRuntime {
  /**
   * @section private:attributes
   */

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
    const serviceRuntime = ServiceRuntime.createRuntime();
    return serviceRuntime;
  }

  private static createRuntime(): ServiceRuntime {
    const clickhouseClientService = ClickhouseClientService.createDefault();
    const snapshotFieldCatalogService = SnapshotFieldCatalogService.createDefault();
    const marketRepositoryService = new MarketRepositoryService({ clickhouseClientService });
    const flatSnapshotRepositoryService = new FlatSnapshotRepositoryService({ clickhouseClientService, snapshotFieldCatalogService });
    const snapshotQueryService = new SnapshotQueryService({ marketRepositoryService, flatSnapshotRepositoryService });
    const serviceRuntime = new ServiceRuntime({
      clickhouseClientService,
      clickhouseSchemaService: new ClickhouseSchemaService({ clickhouseClientService, snapshotFieldCatalogService }),
      snapshotCollectorService: SnapshotCollectorService.createDefault({
        flatSnapshotRepositoryService,
        marketSyncService: MarketSyncService.createDefault({ marketRepositoryService, snapshotFieldCatalogService }),
      }),
      httpServerService: new HttpServerService({
        appInfoService: AppInfoService.createDefault(),
        snapshotQueryService,
      }),
    });
    return serviceRuntime;
  }

  /**
   * @section private:methods
   */

  private async listen(server: Server): Promise<Server> {
    const listenedServer = await new Promise<Server>((resolve, reject) => {
      server.listen(config.HTTP_PORT, config.HTTP_HOST, () => {
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
    const server = this.httpServerService.buildServer() as unknown as Server;
    return server;
  }

  public async startServer(): Promise<Server> {
    await this.clickhouseSchemaService.ensureSchema();
    this.snapshotCollectorService.start();
    const server = this.buildServer();
    const listenedServer = await this.listen(server);
    this.activeServer = listenedServer;
    LOGGER.info(`service listening on http://${config.HTTP_HOST}:${config.HTTP_PORT}`);
    return listenedServer;
  }

  public async stop(): Promise<void> {
    const activeServer = this.activeServer;
    if (activeServer !== null) {
      await this.closeServer(activeServer);
      this.activeServer = null;
    }
    await this.snapshotCollectorService.stop();
    await this.clickhouseClientService.close();
  }
}
