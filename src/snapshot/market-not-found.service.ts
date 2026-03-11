/**
 * @section public:properties
 */

export class MarketNotFoundService extends Error {
  /**
   * @section constructor
   */

  public constructor(slug: string) {
    super(`market not found for slug ${slug}`);
    this.name = "MarketNotFoundService";
  }
}
