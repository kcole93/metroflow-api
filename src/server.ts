// src/server.ts
import express, { Express, Request, Response, NextFunction } from "express";
import { trackApiUsage } from "./middleware/apiTracker";
import http from "http";
import { logger } from "./utils/logger";
import * as dotenv from "dotenv";
import routes from "./routes";
import { loadProtobufDefinitions } from "./utils/protobufLoader";
import { loadStaticData } from "./services/staticDataService";
import { loadBoroughData } from "./services/geoService";
import { refreshActiveServices } from "./services/calendarService";
import { analyticsService } from "./services/analyticsService";

dotenv.config();

const app: Express = express();
const port = process.env.PORT || 3000;
let server: http.Server | null = null; // Variable to hold the server instance

// --- Middleware ---
app.use(express.json());
app.use(trackApiUsage);

// --- Use Routes ---
app.use("/", routes);

// --- Basic Root Response ---
app.get("/", (req: Request, res: Response) => {
  res.send("MTA API Wrapper is running!");
});

// --- Global Error Handler ---
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  // Ensure analyticsService DB is closed even on unhandled errors, if possible
  // Note: This might run *after* the uncaughtException handler in AnalyticsService
  analyticsService.close();
  logger.error("Unhandled error:", { error: err }); // Pass error object
  // Avoid sending stack trace in production
  res.status(500).json({ error: "Something went wrong on the server." });
});

// --- Graceful Shutdown Logic ---
const gracefulShutdown = (signal: string) => {
  logger.warn(`Received ${signal}. Starting graceful shutdown...`);

  // 1. Stop accepting new connections
  if (server) {
    server.close((err) => {
      if (err) {
        logger.error("Error closing HTTP server:", { error: err });
      } else {
        logger.info("HTTP server closed.");
      }

      // 2. Close the Analytics database connection
      analyticsService.close(); // This is now safe as no new requests should arrive

      // 3. Exit process (allow time for logs to flush, etc.)
      logger.info("Shutdown complete. Exiting.");
      process.exit(0);
    });

    // Optional: Force shutdown after a timeout if server.close() hangs
    setTimeout(() => {
      logger.error("Graceful shutdown timed out. Forcing exit.");
      analyticsService.close(); // Attempt close again just in case
      process.exit(1);
    }, 10000); // 10 seconds timeout
  } else {
    // If server wasn't started, just close DB and exit
    logger.warn("Server not started, only closing analytics DB.");
    analyticsService.close();
    process.exit(0);
  }
};

// --- Server Startup ---
async function startServer() {
  try {
    logger.info("Initializing server...");
    await Promise.all([
      loadProtobufDefinitions(),
      loadBoroughData(),
      refreshActiveServices(),
    ]);
    await loadStaticData();

    server = app.listen(port, () => {
      // Assign the server instance
      logger.info(
        // Use logger.info instead of console.info
        `⚡️ Server is running at http://localhost:${port}`,
      );

      // --- Attach Shutdown Handlers AFTER Server Starts ---
      // This replaces the handlers inside AnalyticsService for better coordination
      // Note: The handlers inside AnalyticsService act as a fallback if these aren't registered
      process.off("SIGINT", analyticsService["handleShutdownSignal"]); // Remove old handler if exists
      process.off("SIGTERM", analyticsService["handleShutdownSignal"]); // Remove old handler if exists

      process.on("SIGINT", () => gracefulShutdown("SIGINT")); // Ctrl+C
      process.on("SIGTERM", () => gracefulShutdown("SIGTERM")); // Termination signal

      logger.info("Graceful shutdown handlers attached.");
    });

    // Optional: Handle server startup errors specifically
    server.on("error", (error) => {
      logger.error("HTTP server startup error:", { error });
      process.exit(1);
    });
  } catch (error) {
    logger.error("🚨 Failed to initialize server components:", { error }); // Use logger and pass error obj
    process.exit(1);
  }
}

// --- Uncaught Exception / Unhandled Rejection ---
// These act as a last resort safety net
process.on("uncaughtException", (err, origin) => {
  logger.error("UNCAUGHT EXCEPTION:", { error: err, origin });
  // Attempt graceful shutdown, might fail if state is corrupted
  gracefulShutdown("uncaughtException");
});

process.on("unhandledRejection", (reason, promise) => {
  logger.error("UNHANDLED REJECTION:", { reason });
  // Attempt graceful shutdown
  gracefulShutdown("unhandledRejection");
});

// --- Start the Server ---
startServer();
