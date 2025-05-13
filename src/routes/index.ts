// src/routes/index.ts
import { Router, Response, Request, RequestHandler } from "express";
import { logger } from "../utils/logger";
import { analyticsService } from "../services/analyticsService";
import mtaRoutes from "./mtaRoutes";
import { getCacheStats } from "../services/cacheService";

const router = Router();

// Prefix all MTA routes with /api/v1
router.use("/api/v1", mtaRoutes);

// Basic health check endpoint - publicly accessible
const basicHealthCheckHandler: RequestHandler = (_req, res) => {
  // Only provide minimal health status for public endpoint
  res.status(200).json({
    status: "OK",
    timestamp: new Date().toISOString()
  });
};

// Detailed health check endpoint - protected with API key
const detailedHealthCheckHandler: RequestHandler = (req, res) => {
  // Verify API key
  const apiKey = req.headers['x-api-key'] || req.query.api_key;
  const configuredApiKey = process.env.HEALTH_CHECK_API_KEY;
  
  // If no API key is configured, only allow on localhost
  if (!configuredApiKey) {
    const clientIp = req.ip || '';
    const isLocalhost = clientIp === '127.0.0.1' || clientIp === '::1' || clientIp === '::ffff:127.0.0.1';
    
    if (!isLocalhost) {
      logger.warn(`Unauthorized access attempt to detailed health check from IP: ${clientIp}`);
      res.status(403).json({ error: "Forbidden: detailed health check only available from localhost when no API key is configured" });
      return;
    }
  } else if (apiKey !== configuredApiKey) {
    logger.warn(`Invalid API key used for detailed health check: ${apiKey}`);
    res.status(403).json({ error: "Forbidden: invalid API key" });
    return;
  }
  
  // Get cache statistics
  let cacheStats = {};
  try {
    if (typeof getCacheStats === 'function') {
      cacheStats = getCacheStats();
    }
  } catch (e) {
    logger.debug("Error getting cache stats for health check", { error: e });
  }

  // Detailed health information
  const healthData = {
    status: "OK",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
    environment: process.env.NODE_ENV || 'development',
    memoryUsage: {
      rss: Math.round(process.memoryUsage().rss / 1024 / 1024) + 'MB',
      heapTotal: Math.round(process.memoryUsage().heapTotal / 1024 / 1024) + 'MB',
      heapUsed: Math.round(process.memoryUsage().heapUsed / 1024 / 1024) + 'MB'
    },
    cache: cacheStats
  };
  
  res.status(200).json(healthData);
  return;
};

// Register health check routes
router.get("/api/health", basicHealthCheckHandler);
router.get("/api/health/detailed", detailedHealthCheckHandler);

// Analytics Endpoints ---
// --- Station Analytics Endpoint ---
const stationAnalyticsHandler: RequestHandler = (_req, res) => {
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
};

// --- API Usage Analytics Endpoint ---
const apiUsageAnalyticsHandler: RequestHandler = (_req, res) => {
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
};

// Register analytics routes
router.get("/api/analytics/stations", stationAnalyticsHandler);
router.get("/api/analytics/usage", apiUsageAnalyticsHandler);

export default router;