// src/server.ts
import express, { Express, Request, Response, NextFunction } from "express";
import { trackApiUsage } from "./middleware/apiTracker";
import http from "http";
import { logger } from "./utils/logger";
import * as dotenv from "dotenv";
import routes from "./routes";
import cron from "node-cron";
import { rateLimit } from "express-rate-limit";
import { loadProtobufDefinitions } from "./utils/protobufLoader";
import { loadStaticData } from "./services/staticDataService";
import { loadBoroughData } from "./services/geoService";
import { refreshActiveServices } from "./services/calendarService";
import { analyticsService } from "./services/analyticsService";
import { runStaticDataRefreshTask } from "./tasks/refreshStaticData";

dotenv.config();

// --- Environment Variables ---
const port = process.env.PORT || 3000;
const REFRESH_SCHEDULE = process.env.STATIC_REFRESH_SCHEDULE || "0 0 * * *"; // Default: Every day at midnight
const REFRESH_TIMEZONE = process.env.STATIC_REFRESH_TIMEZONE || "America/New_York"; // Default: America/New_York

// Define type for environment variable validation
interface EnvVar {
  name: string;
  value: string | undefined;
  required: boolean;
}

// Validate environment variables
function validateEnvVars() {
  const requiredVars: EnvVar[] = [
    // Core API keys and endpoints
    { name: 'MTA_API_BASE', value: process.env.MTA_API_BASE, required: true },
    
    // GTFS static files
    { name: 'GTFS_STATIC_URL_NYCT', value: process.env.GTFS_STATIC_URL_NYCT, required: true },
    { name: 'GTFS_STATIC_URL_LIRR', value: process.env.GTFS_STATIC_URL_LIRR, required: true },
    { name: 'GTFS_STATIC_URL_MNR', value: process.env.GTFS_STATIC_URL_MNR, required: true },
    
    // Protobuf paths - these should exist in the codebase
    { name: 'PROTO_BASE_PATH', value: process.env.PROTO_BASE_PATH, required: true },
    { name: 'PROTO_NYCT_PATH', value: process.env.PROTO_NYCT_PATH, required: true },
    { name: 'PROTO_MTARR_PATH', value: process.env.PROTO_MTARR_PATH, required: true },
    
    // Security-related variables (optional)
    { name: 'HEALTH_CHECK_API_KEY', value: process.env.HEALTH_CHECK_API_KEY, required: false }
  ];

  const missingVars = requiredVars.filter(v => v.required && !v.value);
  
  if (missingVars.length > 0) {
    logger.error(`Missing required environment variables: ${missingVars.map(v => v.name).join(', ')}`);
    process.exit(1);
  }
  
  // Validate schedule format
  try {
    const isValid = cron.validate(REFRESH_SCHEDULE);
    if (!isValid) {
      logger.error(`Invalid cron schedule format: ${REFRESH_SCHEDULE}`);
      process.exit(1);
    }
  } catch (e) {
    logger.error(`Error validating cron schedule: ${REFRESH_SCHEDULE}`, { error: e });
    process.exit(1);
  }
  
  logger.info("Environment variables validated successfully");
}

const app: Express = express();
let server: http.Server | null = null; // Variable to hold the server instance

// --- Trust Proxy ---
// IMPORTANT: Enable if running behind a reverse proxy (Nginx, etc.) for accurate rate limiting
// Adjust '1' based on the number of proxy layers
app.set("trust proxy", 1);

// --- Rate Limiter ---
const limiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  limit: 1000, // Limit each IP to 1000 requests per windowMs
  standardHeaders: "draft-7",
  legacyHeaders: false,
  message: "Too many requests from this IP, please try again after 15 minutes",
  handler: (req, res, next, options) => {
    logger.warn(
      `Rate limit exceeded for IP ${req.ip}. Endpoint: ${req.method} ${req.originalUrl}`,
    );
    res.status(options.statusCode).send(options.message);
  },
});

// --- Middleware ---
app.use(express.json());
app.use("/", limiter);
app.use(trackApiUsage);

// --- Security Headers ---
app.use((req, res, next) => {
  // Remove X-Powered-By header
  res.removeHeader('X-Powered-By');
  
  // Prevent MIME type sniffing
  res.setHeader('X-Content-Type-Options', 'nosniff');
  
  // Enable XSS protection in browsers
  res.setHeader('X-XSS-Protection', '1; mode=block');
  
  // Prevent clickjacking
  res.setHeader('X-Frame-Options', 'DENY');
  
  // Enable strict HTTPS
  if (process.env.NODE_ENV === 'production') {
    res.setHeader('Strict-Transport-Security', 'max-age=15552000; includeSubDomains');
  }
  
  next();
});

// --- Use Routes ---
app.use("/", routes);

// --- Basic Root Response ---
app.get("/", (req: Request, res: Response) => {
  res.send("MTA API Wrapper is running!");
});

// --- Global Error Handler ---
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  // Log detailed error information but don't close analytics DB in the middleware
  // (That should be handled by the graceful shutdown process)
  logger.error("Unhandled API error:", { 
    error: err,
    message: err.message,
    stack: err.stack,
    path: req.path,
    method: req.method,
    ip: req.ip,
    headers: req.headers['user-agent']
  });
  
  // Avoid sending stack trace in production
  const isProd = process.env.NODE_ENV === 'production';
  if (isProd) {
    res.status(500).json({ error: "Something went wrong on the server." });
  } else {
    // In development, include more details
    res.status(500).json({ 
      error: "Something went wrong on the server.", 
      message: err.message,
      stack: err.stack
    });
  }
});

// --- Graceful Shutdown Logic ---
const gracefulShutdown = (signal: string): void => {
  // Avoid multiple shutdown calls
  if (isShuttingDown) {
    logger.info(`Shutdown already in progress, ignoring ${signal}`);
    return;
  }
  
  isShuttingDown = true;
  logger.warn(`Received ${signal}. Starting graceful shutdown...`);

  // 1. Stop accepting new connections
  if (server) {
    // Set a hard shutdown timeout
    const forcedExitTimer = setTimeout(() => {
      logger.error("Graceful shutdown timed out after 15 seconds. Forcing exit.");
      analyticsService.close(); // Attempt close again just in case
      process.exit(1);
    }, 15000); // 15 seconds timeout
    
    server.close((err) => {
      if (err) {
        logger.error("Error closing HTTP server:", { error: err });
      } else {
        logger.info("HTTP server closed successfully.");
      }

      // 2. Close the Analytics database connection
      analyticsService.close(); // This is now safe as no new requests should arrive
      
      // 3. Clear the forced exit timer
      clearTimeout(forcedExitTimer);

      // 4. Exit process (allow time for logs to flush, etc.)
      logger.info("Shutdown complete. Exiting with code 0.");
      process.exit(0);
    });
  } else {
    // If server wasn't started, just close DB and exit
    logger.warn("Server not started, only closing analytics DB.");
    analyticsService.close();
    process.exit(0);
  }
};

// --- Server Startup ---
async function startServer(): Promise<void> {
  try {
    logger.info("Initializing server...");
    
    // Validate environment variables first
    validateEnvVars();
    
    await Promise.all([
      loadProtobufDefinitions(),
      loadBoroughData(),
      refreshActiveServices(),
      loadStaticData(),
    ]);

    server = app.listen(port, () => {
      // Assign the server instance
      logger.info(
        // Use logger.info instead of console.info
        `⚡️ Server is running at http://localhost:${port} in ${process.env.NODE_ENV || 'development'} mode`,
      );

      // --- Attach Shutdown Handlers AFTER Server Starts ---
      // This replaces the handlers inside AnalyticsService for better coordination
      // Note: The handlers inside AnalyticsService act as a fallback if these aren't registered
      process.off("SIGINT", analyticsService["handleShutdownSignal"]); // Remove old handler if exists
      process.off("SIGTERM", analyticsService["handleShutdownSignal"]); // Remove old handler if exists

      process.on("SIGINT", () => gracefulShutdown("SIGINT")); // Ctrl+C
      process.on("SIGTERM", () => gracefulShutdown("SIGTERM")); // Termination signal

      logger.info("Graceful shutdown handlers attached.");

      logger.info(
        `Scheduling static data refresh task with schedule "${REFRESH_SCHEDULE}" in timezone "${REFRESH_TIMEZONE}" (Source: Env Var or Default)`,
      );
      cron.schedule(
        REFRESH_SCHEDULE,
        async () => {
          logger.info("Cron job triggered: Running static data refresh task.");
          try {
            // No need to await here unless subsequent scheduled tasks depend on it
            runStaticDataRefreshTask().catch((taskError) => {
              // Catch errors within the async task execution itself
              logger.error(
                "Error occurred during scheduled static data refresh task execution:",
                { error: taskError },
              );
            });
          } catch (scheduleError) {
            // Catch errors related to scheduling itself (less likely)
            logger.error("Error scheduling static data refresh task:", {
              error: scheduleError,
            });
          }
        },
        {
          scheduled: true,
          timezone: REFRESH_TIMEZONE,
        },
      );
      logger.info("Static data refresh task scheduled.");
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

// Flag to track if shutdown is in progress
let isShuttingDown: boolean = false;

// --- Uncaught Exception / Unhandled Rejection ---
// These act as a last resort safety net
process.on("uncaughtException", (err: Error, origin: string) => {
  logger.error("UNCAUGHT EXCEPTION:", { 
    error: err, 
    message: err.message,
    stack: err.stack,
    origin 
  });
  // Attempt graceful shutdown, might fail if state is corrupted
  gracefulShutdown("uncaughtException");
});

process.on("unhandledRejection", (reason: unknown, promise: Promise<any>) => {
  // Handle both Error objects and other types of reasons
  const reasonObj = reason instanceof Error ? 
    { message: reason.message, stack: reason.stack } : 
    { reason };
    
  logger.error("UNHANDLED REJECTION:", reasonObj);
  // Attempt graceful shutdown
  gracefulShutdown("unhandledRejection");
});

// --- Start the Server ---
startServer();
