/**
 * @section public:properties
 */

export class SnapshotConsistencyService extends Error {
  /**
   * @section constructor
   */

  public constructor(message: string) {
    super(message);
    this.name = "SnapshotConsistencyService";
  }
}
