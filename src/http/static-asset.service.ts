/**
 * @section imports:externals
 */

import { readFile } from "node:fs/promises";

/**
 * @section types
 */

type StaticAssetPayload = { body: string; contentType: string };

/**
 * @section private:properties
 */

export class StaticAssetService {
  private readonly cachedAssetByName = new Map<string, StaticAssetPayload>();

  /**
   * @section private:methods
   */

  private buildAssetUrl(assetName: string): URL {
    const assetUrl = new URL(`../../public/${assetName}`, import.meta.url);
    return assetUrl;
  }

  private readContentType(assetName: string): string {
    let contentType = "text/plain; charset=utf-8";
    if (assetName.endsWith(".html")) {
      contentType = "text/html; charset=utf-8";
    }
    if (assetName.endsWith(".css")) {
      contentType = "text/css; charset=utf-8";
    }
    if (assetName.endsWith(".js")) {
      contentType = "text/javascript; charset=utf-8";
    }
    return contentType;
  }

  /**
   * @section public:methods
   */

  public async readAsset(assetName: string): Promise<StaticAssetPayload> {
    const cachedAsset = this.cachedAssetByName.get(assetName) || null;
    let assetPayload = cachedAsset;
    if (!assetPayload) {
      const body = await readFile(this.buildAssetUrl(assetName), "utf8");
      assetPayload = { body, contentType: this.readContentType(assetName) };
      this.cachedAssetByName.set(assetName, assetPayload);
    }
    return assetPayload;
  }
}
