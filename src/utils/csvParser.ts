import fs from "node:fs";
import path from "node:path";
import Papa from "papaparse";
import { Logger } from "./logger";

/**
 * Parses a CSV file using streaming to minimize memory usage.
 * Processes rows in chunks instead of loading the entire file into memory.
 */
export async function parseCsvFile<T extends object>(
  filePath: string,
  logger: Logger,
): Promise<T[]> {
  return new Promise((resolve, reject) => {
    logger.info(`Reading CSV file: ${path.basename(filePath)}`);

    const results: T[] = [];
    let errorCount = 0;

    const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });

    fileStream.on("error", (error) => {
      logger.error(
        `Error reading CSV ${path.basename(filePath)}: ${error.message}`,
        { error },
      );
      reject(error);
    });

    Papa.parse<T>(fileStream, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      step: (row) => {
        if (row.errors.length > 0) {
          errorCount++;
          if (errorCount <= 5) {
            logger.warn(`Parse error in ${path.basename(filePath)}:`, { errors: row.errors });
          }
        }
        if (row.data) {
          results.push(row.data);
        }
      },
      complete: () => {
        if (errorCount > 5) {
          logger.warn(`${errorCount} total parsing errors in ${path.basename(filePath)}`);
        }
        logger.debug(`Successfully parsed ${results.length} rows from ${path.basename(filePath)}`);
        resolve(results);
      },
      error: (error) => {
        logger.error(`Error parsing CSV ${path.basename(filePath)}: ${error.message}`, { error });
        reject(error);
      },
    });
  });
}

/**
 * Processes a CSV file row by row using a callback, without storing all rows in memory.
 * Use this for very large files like stop_times.txt where you only need to extract specific data.
 */
export async function processCsvFileStreaming<T extends object>(
  filePath: string,
  logger: Logger,
  onRow: (row: T) => void,
): Promise<number> {
  return new Promise((resolve, reject) => {
    logger.info(`Streaming CSV file: ${path.basename(filePath)}`);

    let rowCount = 0;
    let errorCount = 0;

    const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });

    fileStream.on("error", (error) => {
      logger.error(
        `Error reading CSV ${path.basename(filePath)}: ${error.message}`,
        { error },
      );
      reject(error);
    });

    Papa.parse<T>(fileStream, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
      step: (row) => {
        if (row.errors.length > 0) {
          errorCount++;
        } else if (row.data) {
          rowCount++;
          onRow(row.data);
        }
      },
      complete: () => {
        if (errorCount > 0) {
          logger.warn(`${errorCount} parsing errors in ${path.basename(filePath)}`);
        }
        logger.debug(`Streamed ${rowCount} rows from ${path.basename(filePath)}`);
        resolve(rowCount);
      },
      error: (error) => {
        logger.error(`Error parsing CSV ${path.basename(filePath)}: ${error.message}`, { error });
        reject(error);
      },
    });
  });
}
