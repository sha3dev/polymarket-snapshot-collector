/**
 * @section imports:internals
 */

import config from "../config.ts";

/**
 * @section types
 */

export type AppInfoPayload = { ok: true; serviceName: string };

type AppInfoServiceOptions = { serviceName: string };

/**
 * @section class
 */

export class AppInfoService {
  /**
   * @section private:attributes
   */

  private readonly serviceName: string;

  /**
   * @section constructor
   */

  public constructor(options: AppInfoServiceOptions) {
    this.serviceName = options.serviceName;
  }

  /**
   * @section factory
   */

  public static createDefault(): AppInfoService {
    return new AppInfoService({ serviceName: config.SERVICE_NAME });
  }

  /**
   * @section public:methods
   */

  public buildPayload(): AppInfoPayload {
    return { ok: true, serviceName: this.serviceName };
  }
}
