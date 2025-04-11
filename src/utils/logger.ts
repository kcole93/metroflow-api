export class LoggerService {
  private static instance: LoggerService;

  private constructor() {}

  public static getInstance(): LoggerService {
    if (!LoggerService.instance) {
      LoggerService.instance = new LoggerService();
    }
    return LoggerService.instance;
  }

  public createServiceLogger(serviceName: string) {
    return {
      log: (message: string) => this.log(serviceName, message),
      warn: (message: string) => this.warn(serviceName, message),
      error: (message: string, error?: unknown) =>
        this.error(serviceName, message, error),
    };
  }

  private log(serviceName: string, message: string): void {
    console.log(`[${serviceName}] ${message}`);
  }

  private warn(serviceName: string, message: string): void {
    console.warn(`[${serviceName}] ${message}`);
  }

  private error(serviceName: string, message: string, error?: unknown): void {
    if (error instanceof Error) {
      console.error(`[${serviceName}] ${message}`, error);
    } else if (error !== undefined) {
      console.error(`[${serviceName}] ${message}`, "Unknown error:", error);
    } else {
      console.error(`[${serviceName}] ${message}`);
    }
  }
}

// Usage example:
// const staticDataLogger = LoggerService.getInstance().createServiceLogger('Static Data');
// staticDataLogger.log('Data loaded successfully');
// staticDataLogger.error('Failed to load data');
