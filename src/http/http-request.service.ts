/**
 * @section public:properties
 */

export class HttpRequestService extends Error {
  public readonly statusCode: number;
  public readonly errorCode: string;

  /**
   * @section constructor
   */

  public constructor(statusCode: number, errorCode: string, message: string) {
    super(message);
    this.name = "HttpRequestService";
    this.statusCode = statusCode;
    this.errorCode = errorCode;
  }
}
