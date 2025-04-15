// src/services/analyticsService.ts
import Database, { Database as DB, Statement } from "better-sqlite3";
import path from "node:path";
import fs from "node:fs";
import { logger } from "../utils/logger";

// Station data row from DB
interface StationAnalyticsRow {
  system: string;
  station_id: string;
  station_name: string;
  count: number;
}
// Station analytics structure for API
interface StationAnalytics {
  count: number;
  name: string;
}
type StationAnalyticsData = Record<string, Record<string, StationAnalytics>>;

// Define the structure for Usage Analytics
interface ApiUsageRow {
  day_timestamp: number;
  endpoint: string;
  count: number;
}
type ApiUsageData = ApiUsageRow[];

// --- Database Setup ---
const dataDir = path.join(__dirname, "..", "..", "data");
const dbPath = path.join(dataDir, "analytics.sqlite");

// Ensure the data directory exists
if (!fs.existsSync(dataDir)) {
  try {
    fs.mkdirSync(dataDir, { recursive: true });
    logger.info(`[Analytics Service] Created data directory: ${dataDir}`);
  } catch (error) {
    const msg = `[Analytics Service] Failed to create data directory: ${dataDir}`;
    logger.error(msg, { error });
    throw new Error(msg);
  }
}

class AnalyticsService {
  private db: DB;
  private trackStationStmt: Statement<[string, string, string]>;
  private getAllStationsStmt: Statement;
  private trackApiHitStmt: Statement<[number, string]>;
  private getApiUsageStmt: Statement;

  constructor(databasePath: string) {
    logger.info(
      `[Analytics Service] Initializing database at: ${databasePath}`,
    );
    try {
      // Connect to the database (creates the file if it doesn't exist)
      this.db = new Database(databasePath);

      // Ensure the table exists (using IF NOT EXISTS is idempotent)
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS analytics_counts (
          system       TEXT    NOT NULL,
          station_id   TEXT    NOT NULL,
          station_name TEXT    NOT NULL,
          count        INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (system, station_id)
        );
      `);
      // API Usage Table (Daily)
      this.db.exec(`
        CREATE TABLE IF NOT EXISTS api_usage_daily (
          day_timestamp INTEGER NOT NULL,
          endpoint      TEXT    NOT NULL,
          count         INTEGER NOT NULL DEFAULT 0,
          PRIMARY KEY (day_timestamp, endpoint)
          );
      `);

      logger.info("[Analytics Service] Database tables ensured.");

      // --- Prepare Statements for Performance ---
      // Insert a new record or increment the count if the combination already exists
      this.trackStationStmt = this.db.prepare(`
              INSERT INTO analytics_counts (system, station_id, station_name, count)
              VALUES (?, ?, ?, 1) -- Include name placeholder
              ON CONFLICT(system, station_id) DO UPDATE SET
                count = count + 1,
                station_name = excluded.station_name; -- Update name on conflict too
            `);
      this.getAllStationsStmt = this.db.prepare(`
              SELECT system, station_id, station_name, count FROM analytics_counts;
            `); // Select the name

      // API Usage statements (modified for daily)
      this.trackApiHitStmt = this.db.prepare(`
              INSERT INTO api_usage_daily (day_timestamp, endpoint, count) -- Use daily table/column
              VALUES (?, ?, 1)
              ON CONFLICT(day_timestamp, endpoint) DO UPDATE SET
                count = count + 1;
            `);
      this.getApiUsageStmt = this.db.prepare(`
              SELECT day_timestamp, endpoint, count
              FROM api_usage_daily -- Use daily table
              ORDER BY day_timestamp DESC, endpoint ASC;
            `);

      logger.info(
        "[Analytics Service] Database connection and statements ready.",
      );

      // Graceful shutdown handling
      process.on("exit", this.close.bind(this)); // Normal exit
      process.on("SIGINT", this.handleShutdownSignal.bind(this, "SIGINT")); // Ctrl+C
      process.on("SIGTERM", this.handleShutdownSignal.bind(this, "SIGTERM")); // Termination signal
      process.on("uncaughtException", (err, origin) => {
        // Attempt cleanup on crash
        logger.error(
          "[Analytics Service] Uncaught exception, attempting DB close.",
          { error: err, origin },
        );
        this.close();
        process.exit(1); // Exit after logging/cleanup attempt
      });
    } catch (error) {
      logger.error(
        "[Analytics Service] FATAL: Database initialization failed!",
        { error },
      );
      // If the DB is critical, exiting might be appropriate
      process.exit(1);
    }
  }

  public trackStationLookup(
    system: string,
    stationId: string,
    stationName: string,
  ): void {
    if (
      !system ||
      !stationId ||
      !stationName ||
      typeof system !== "string" ||
      typeof stationId !== "string" ||
      typeof stationName !== "string"
    ) {
      logger.warn(
        "[Analytics Service] Invalid input for trackStationLookup, skipping.",
        { system, stationId, stationName },
      );
      return;
    }

    try {
      // Execute with the station name included
      this.trackStationStmt.run(system, stationId, stationName);
      logger.debug(
        `[Analytics Service] Tracked lookup: ${system}-${stationId} (${stationName})`,
      );
    } catch (error) {
      logger.error(
        `[Analytics Service] Failed to track lookup for ${system}-${stationId}`,
        { error },
      );
    }
  }

  public getAnalyticsData(): StationAnalyticsData {
    try {
      const rows = this.getAllStationsStmt.all() as StationAnalyticsRow[];
      const analyticsData: StationAnalyticsData = {};

      for (const row of rows) {
        if (!analyticsData[row.system]) {
          analyticsData[row.system] = {};
        }
        analyticsData[row.system][row.station_id] = {
          count: row.count,
          name: row.station_name,
        };
      }
      return analyticsData;
    } catch (error) {
      logger.error(
        "[Analytics Service] Failed to retrieve station analytics data from DB",
        { error },
      );
      return {};
    }
  }

  public trackApiHit(endpoint: string): void {
    if (!endpoint || typeof endpoint !== "string") {
      logger.warn(
        "[Analytics Service] Invalid endpoint received for API hit tracking.",
        { endpoint },
      );
      return;
    }
    try {
      // Calculate the timestamp for the start of the *current day* (UTC)
      const now = new Date();
      const startOfDayUTC = new Date(
        Date.UTC(
          now.getUTCFullYear(),
          now.getUTCMonth(),
          now.getUTCDate(),
          0,
          0,
          0,
          0,
        ),
      );
      const dayTimestamp = Math.floor(startOfDayUTC.getTime() / 1000); // Unix Epoch seconds

      this.trackApiHitStmt.run(dayTimestamp, endpoint); // Use daily timestamp
      logger.debug(
        `[Analytics Service] Tracked API hit for endpoint: ${endpoint} on day ${dayTimestamp}`,
      );
    } catch (error) {
      logger.error(
        `[Analytics Service] Failed to track API hit for endpoint: ${endpoint}`,
        { error },
      );
    }
  }

  public getApiUsageData(): ApiUsageData {
    try {
      const rows = this.getApiUsageStmt.all() as ApiUsageRow[];
      return rows; // Return daily data
    } catch (error) {
      logger.error(
        "[Analytics Service] Failed to retrieve API usage data from DB",
        { error },
      );
      return [];
    }
  }

  // --- Shutdown Handling ---
  private isShuttingDown = false;

  public close(): void {
    if (this.isShuttingDown) return; // Prevent multiple close attempts
    this.isShuttingDown = true;

    if (this.db && this.db.open) {
      logger.info("[Analytics Service] Closing database connection...");
      try {
        this.db.close();
        logger.info("[Analytics Service] Database connection closed.");
      } catch (error) {
        logger.error("[Analytics Service] Error closing database connection:", {
          error,
        });
      }
    } else {
      logger.info(
        "[Analytics Service] Database connection already closed or not initialized.",
      );
    }
  }

  private handleShutdownSignal(signal: string): void {
    logger.info(
      `[Analytics Service] Received ${signal}. Initiating graceful shutdown...`,
    );
    this.close();
    process.exit(0); // Exit after cleanup
  }
}

// Export a singleton instance, initializing it with the path
const analyticsService = new AnalyticsService(dbPath);
export { analyticsService };
