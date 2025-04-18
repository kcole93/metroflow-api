// src/routes/index.ts
import { Router } from "express";
import { logger } from "../utils/logger";
import { analyticsService } from "../services/analyticsService";
import mtaRoutes from "./mtaRoutes";

const router = Router();

// Prefix all MTA routes with /api/v1
router.use("/api/v1", mtaRoutes);

// Health check endpoint
router.get("/api/health", (req, res) => {
  res.status(200).json({ status: "OK", timestamp: new Date().toISOString() });
});

// Analytics Endpoints ---
// --- Station Analytics Endpoint ---
router.get("/api/analytics/stations", (req, res) => {
  // Renamed for clarity
  logger.debug("[API Route] GET /analytics/stations request received");
  try {
    const analyticsData = analyticsService.getAnalyticsData(); // Gets station data
    res.status(200).json(analyticsData);
  } catch (error) {
    logger.error(
      "[API Route] Failed to retrieve or send station analytics data:",
      { error },
    );
    res
      .status(500)
      .json({ message: "Error retrieving station analytics data" });
  }
});

// --- API Usage Analytics Endpoint ---
router.get("/api/analytics/usage", (req, res) => {
  logger.debug("[API Route] GET /analytics/usage request received");
  try {
    const usageData = analyticsService.getApiUsageData(); // Gets API usage data
    res.status(200).json(usageData);
  } catch (error) {
    logger.error("[API Route] Failed to retrieve or send API usage data:", {
      error,
    });
    res.status(500).json({ message: "Error retrieving API usage data" });
  }
});
export default router;
