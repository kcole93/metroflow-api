import fs from "node:fs/promises";
import path from "node:path";
import Papa from "papaparse";
import { LoggerService } from "./logger";

export async function parseCsvFile<T extends object>(
  filePath: string,
  logger: ReturnType<LoggerService["createServiceLogger"]>,
): Promise<T[]> {
  try {
    logger.log(`Reading ${filePath}`);
    const fileContent = await fs.readFile(filePath, "utf8");
    const result = Papa.parse<T>(fileContent, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
    });
    if (result.errors.length > 0) {
      logger.warn(
        `Parsing errors in ${path.basename(filePath)}: ${JSON.stringify(result.errors.slice(0, 2))}`,
      );
    }
    return result.data;
  } catch (error: unknown) {
    if (error instanceof Error) {
      logger.error(
        `Error reading/parsing CSV ${path.basename(filePath)}:`,
        error,
      );
    } else {
      // Handle cases where the caught error is not an Error object
      logger.error(
        `Unknown error reading/parsing CSV ${path.basename(filePath)}:`,
        new Error(String(error)),
      );
    }
    throw error;
  }
}
