// src/tasks/refreshStaticData.ts
import fs from "node:fs/promises";
import path from "node:path";
import AdmZip from "adm-zip";
import { rimraf } from "rimraf";
import { logger } from "../utils/logger";
import { loadStaticData } from "../services/staticDataService";

const GTFS_STATIC_SOURCES = {
  NYCT: {
    url:
      process.env.GTFS_STATIC_URL_NYCT ||
      "https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip",
    targetDirName: "NYCT",
  },
  LIRR: {
    url:
      process.env.GTFS_STATIC_URL_LIRR ||
      "https://rrgtfsfeeds.s3.amazonaws.com/gtfslirr.zip",
    targetDirName: "LIRR",
  },
  MNR: {
    url:
      process.env.GTFS_STATIC_URL_MNR ||
      "https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip",
    targetDirName: "MNR",
  },
};
// Check if URLs are provided
if (
  !GTFS_STATIC_SOURCES.NYCT.url ||
  !GTFS_STATIC_SOURCES.LIRR.url ||
  !GTFS_STATIC_SOURCES.MNR.url
) {
  logger.error(
    "[Static Refresh] Missing required GTFS Static URL environment variables (e.g., GTFS_STATIC_URL_NYCT). Task cannot run.",
  );
}

const STATIC_DATA_BASE_PATH = path.join(
  __dirname,
  "..",
  "assets",
  "gtfs-static",
);
const TEMP_DOWNLOAD_DIR = path.join(__dirname, "..", "..", "temp-downloads"); // Create a temporary dir outside src

// --- Helper: Ensure Directory Exists ---
async function ensureDirExists(dirPath: string) {
  try {
    await fs.mkdir(dirPath, { recursive: true });
  } catch (error: any) {
    if (error.code !== "EEXIST") {
      // Ignore if directory already exists
      throw error; // Re-throw other errors
    }
  }
}

// --- Helper: Download and Unzip ---
async function downloadAndUnzip(
  sourceName: string,
  url: string,
  targetSystemDir: string,
): Promise<void> {
  const tempFilePath = path.join(
    TEMP_DOWNLOAD_DIR,
    `${sourceName}_${Date.now()}.zip`,
  );
  logger.info(
    `[Static Refresh] Downloading ${sourceName} from ${url} to ${tempFilePath}...`,
  );

  // Ensure temp dir exists
  await ensureDirExists(TEMP_DOWNLOAD_DIR);

  // Download
  const response = await fetch(url);
  if (!response.ok || !response.body) {
    throw new Error(
      `Failed to download ${sourceName}: Status ${response.status}`,
    );
  }
  const fileStream = await fs.open(tempFilePath, "w");
  // @ts-ignore ReadableStream incompatibility needs checking with node-fetch v3 types if issue persists
  await fileStream.writeFile(response.body);
  await fileStream.close();

  logger.info(
    `[Static Refresh] Download complete for ${sourceName}. Unzipping...`,
  );

  // Clean target directory before unzipping
  logger.info(`[Static Refresh] Cleaning target directory: ${targetSystemDir}`);
  await rimraf(targetSystemDir); // Delete existing contents
  await ensureDirExists(targetSystemDir); // Recreate directory

  // Unzip
  try {
    const zip = new AdmZip(tempFilePath);
    zip.extractAllTo(targetSystemDir, /*overwrite*/ true);
    logger.info(
      `[Static Refresh] Unzipped ${sourceName} successfully to ${targetSystemDir}.`,
    );
  } catch (unzipError) {
    logger.error(`[Static Refresh] Failed to unzip ${sourceName}`, {
      error: unzipError,
    });
    // Attempt to clean up failed unzip attempt directory? Maybe leave it for inspection.
    throw new Error(`Unzipping failed for ${sourceName}`); // Propagate error
  } finally {
    // Clean up temporary zip file
    try {
      await fs.unlink(tempFilePath);
    } catch (cleanupError) {
      logger.warn(
        `[Static Refresh] Failed to delete temporary file ${tempFilePath}`,
        { error: cleanupError },
      );
    }
  }
}

// --- Main Refresh Task Function ---
let isTaskRunning = false; // Prevent concurrent runs

export async function runStaticDataRefreshTask(): Promise<void> {
  if (isTaskRunning) {
    logger.warn(
      "[Static Refresh] Task is already running. Skipping scheduled execution.",
    );
    return;
  }

  isTaskRunning = true;
  logger.info(
    "[Static Refresh] Starting scheduled static data refresh task...",
  );

  const results = await Promise.allSettled(
    Object.entries(GTFS_STATIC_SOURCES).map(([sourceName, config]) => {
      const targetDir = path.join(STATIC_DATA_BASE_PATH, config.targetDirName);
      return downloadAndUnzip(sourceName, config.url, targetDir);
    }),
  );

  let allSucceeded = true;
  results.forEach((result, index) => {
    const sourceName = Object.keys(GTFS_STATIC_SOURCES)[index];
    if (result.status === "rejected") {
      allSucceeded = false;
      logger.error(`[Static Refresh] Failed to process ${sourceName}:`, {
        reason: result.reason,
      });
    }
  });

  if (allSucceeded) {
    logger.info(
      "[Static Refresh] All sources downloaded and unzipped successfully. Reloading data into memory...",
    );
    try {
      // *** CRITICAL: Trigger the reload of static data ***
      await loadStaticData();
      logger.info(
        "[Static Refresh] In-memory static data reloaded successfully.",
      );
    } catch (reloadError) {
      logger.error(
        "[Static Refresh] CRITICAL: Failed to reload static data into memory after successful download!",
        { error: reloadError },
      );
      // Maybe alert someone here? The files are updated, but the app isn't using them.
    }
  } else {
    logger.error(
      "[Static Refresh] One or more sources failed to download/unzip. Static data in memory was NOT reloaded.",
    );
    // You might want to add alerting here too.
  }

  isTaskRunning = false;
  logger.info("[Static Refresh] Finished scheduled static data refresh task.");
}
