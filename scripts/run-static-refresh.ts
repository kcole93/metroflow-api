// scripts/run-static-refresh.ts
import { runStaticDataRefreshTask } from "../src/tasks/refreshStaticData";
import { logger } from "../src/utils/logger";

async function runTask() {
  logger.info("Starting manual static data refresh...");
  try {
    await runStaticDataRefreshTask();
    logger.info("Manual static data refresh finished successfully.");
    process.exit(0); // Exit cleanly on success
  } catch (error) {
    logger.error("Manual static data refresh failed:", error);
    process.exit(1); // Exit with error code on failure
  }
}

runTask();
