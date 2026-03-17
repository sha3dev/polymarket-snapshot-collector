/**
 * @section class
 */

export class HttpRequestService extends Error {
  /**
   * @section public:properties
   */

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
