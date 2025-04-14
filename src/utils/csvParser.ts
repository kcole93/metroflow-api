import fs from "node:fs/promises";
import path from "node:path";
import Papa, { ParseResult, ParseError } from "papaparse"; // Import types from papaparse
// Import the Logger type you defined/exported
import { Logger } from "./logger";

export async function parseCsvFile<T extends object>(
  filePath: string,
  logger: Logger, // Use the imported Logger type
): Promise<T[]> {
  try {
    logger.info(`Reading CSV file: ${path.basename(filePath)}`); // Use info or debug
    const fileContent: string = await fs.readFile(filePath, "utf8");

    // Explicitly type the result from Papa.parse
    const result: ParseResult<T> = Papa.parse<T>(fileContent, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false,
    });

    // Check for parsing errors
    if (result.errors.length > 0) {
      // Log only a few errors to avoid flooding logs
      const errorsToLog = result.errors.slice(0, 5);
      logger.warn(
        `Encountered ${result.errors.length} parsing error(s) in ${path.basename(
          filePath,
        )}. First ${errorsToLog.length} errors:`,
        { errors: errorsToLog }, // Pass errors as structured metadata
      );
    }

    // Log successful parsing info
    logger.debug(
      `Successfully parsed ${result.data.length} rows from ${path.basename(
        filePath,
      )}. Meta:`,
      result.meta,
    );

    return result.data;
  } catch (error: unknown) {
    if (error instanceof Error) {
      logger.error(
        `Error reading/parsing CSV ${path.basename(filePath)}: ${error.message}`,
        { error }, // Pass the full error object as metadata
      );
    } else {
      logger.error(
        `Unknown error reading/parsing CSV ${path.basename(filePath)}`,
        { error: String(error) }, // Log the string representation
      );
    }
    // Rethrow the error to signal failure to the caller
    throw error;
  }
}
