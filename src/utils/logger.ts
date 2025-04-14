// src/utils/logger.ts
import winston from "winston";
import path from "path";
import fs from "fs";

// Ensure logs directory exists
const logDir = path.join(__dirname, "..", "..", "logs"); // Place logs directory at project root
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir);
}

// Determine log level from environment variable, default to 'info'
// Common levels: error, warn, info
const logLevel = process.env.LOG_LEVEL || "info";

// Define custom format for console logs (with colors)
const consoleFormat = winston.format.combine(
  winston.format.colorize(),
  winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    let metaString = "";
    if (meta && Object.keys(meta).length) {
      // Handle potential circular structures in meta objects
      try {
        metaString = JSON.stringify(
          meta,
          (key, value) => {
            if (typeof value === "object" && value !== null) {
              // Simple check for circularity, might need more robust solution
              // Or consider using a library like 'fast-safe-stringify'
              if (value instanceof Error) {
                return { message: value.message, stack: value.stack };
              }
              // Basic circular check placeholder, might need improvement
              // return '[Object]'; // Simplistic fallback
            }
            return value;
          },
          2,
        ); // Pretty print meta object
      } catch (e) {
        metaString = "[Meta serialization error]";
      }
    }
    // Ensure message is a string
    const messageStr =
      typeof message === "object" ? JSON.stringify(message) : message;

    return `[${timestamp}] ${level}: ${messageStr}${metaString ? ` ${metaString}` : ""}`;
  }),
);

// Define custom format for file logs (without colors)
const fileFormat = winston.format.combine(
  winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    let metaString = "";
    if (meta && Object.keys(meta).length) {
      try {
        metaString = JSON.stringify(meta, (key, value) => {
          if (value instanceof Error) {
            return { message: value.message, stack: value.stack };
          }
          return value;
        }); // Compact JSON for file
      } catch (e) {
        metaString = "[Meta serialization error]";
      }
    }
    // Ensure message is a string
    const messageStr =
      typeof message === "object" ? JSON.stringify(message) : message;
    return `[${timestamp}] ${level}: ${messageStr}${metaString ? ` ${metaString}` : ""}`;
  }),
);

// Create the logger instance
const logger = winston.createLogger({
  level: logLevel, // Minimum level to log
  format: winston.format.json(), // Default format if others aren't specified per transport
  transports: [
    // Console Transport
    new winston.transports.Console({
      format: consoleFormat,
      level: logLevel, // Respect the environment variable level for console
    }),
    // File Transport for all logs (based on level)
    new winston.transports.File({
      filename: path.join(logDir, "combined.log"),
      level: logLevel, // Log based on the configured level
      format: fileFormat,
      maxsize: 5242880, // 5MB
      maxFiles: 5, // Keep up to 5 rotated files
      tailable: true,
    }),
    // File Transport specifically for errors
    new winston.transports.File({
      filename: path.join(logDir, "error.log"),
      level: "error", // Only log 'error' level messages
      format: fileFormat,
      maxsize: 5242880, // 5MB
      maxFiles: 3,
      tailable: true,
    }),
  ],
  exitOnError: false, // Do not exit on handled exceptions
});

// Stream interface for morgan (optional, if you use express logging middleware)
// logger.stream = {
//   write: (message) => {
//     logger.http(message.trim());
//   },
// };

export type Logger = winston.Logger;
export { logger };
