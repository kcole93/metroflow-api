import { Router, Response, RequestHandler } from "express";
import * as mtaService from "../services/mtaService";
import { getServiceAlerts } from "../services/alertService";
import { logger } from "../utils/logger";

const router = Router();

const ALLOWED_QUERY_SYSTEMS = ["LIRR", "SUBWAY", "MNR"] as const;
type FilterableSystem = (typeof ALLOWED_QUERY_SYSTEMS)[number];

// Error handler
const handleServiceError = (
  err: unknown,
  res: Response,
  defaultMessage: string,
) => {
  logger.error("Error handled in route:", err);
  const statusCode =
    err instanceof Object &&
    "statusCode" in err &&
    typeof err.statusCode === "number"
      ? err.statusCode
      : 500;
  const message = err instanceof Error ? err.message : defaultMessage;
  res.status(statusCode).json({ error: message });
};

// GET /api/v1/stations?q=Penn&system=LIRR
const getStationsHandler: RequestHandler = async (req, res) => {
  const query = req.query.q as string | undefined;
  const systemQuery = req.query.system as string | undefined;

  // Variable to hold the validated filter
  let systemFilter: FilterableSystem | undefined = undefined;

  if (systemQuery) {
    const normalizedSystemQuery = systemQuery.toUpperCase();

    if (
      (ALLOWED_QUERY_SYSTEMS as ReadonlyArray<string>).includes(
        normalizedSystemQuery,
      )
    ) {
      systemFilter = normalizedSystemQuery as FilterableSystem;
    } else {
      res.status(400).json({
        error: `Invalid system query parameter. Must be one of: ${ALLOWED_QUERY_SYSTEMS.join(
          ", ",
        )}`,
      });
      return;
    }
  }

  try {
    const stations = await mtaService.getStations(query, systemFilter);
    res.json(stations);
  } catch (err) {
    handleServiceError(err, res, "Failed to retrieve stations.");
  }
};

router.get("/stations", getStationsHandler);

const getDeparturesHandler: RequestHandler = async (req, res) => {
  const stationId = req.params.stationId;
  if (!stationId) {
    res.status(400).json({ error: "Station ID parameter is required." });
    return;
  }

  // --- Time Limit Processing
  const limitMinutesQuery = req.query.limitMinutes as string | undefined;
  let limitMinutes: number | undefined = undefined; // Default: no limit

  if (limitMinutesQuery) {
    const parsedLimit = parseInt(limitMinutesQuery, 10);
    // Validate: must be a positive number
    if (isNaN(parsedLimit) || parsedLimit <= 0) {
      res.status(400).json({
        error:
          "Invalid limitMinutes query parameter. Must be a positive number.",
      });
      return;
    }
    limitMinutes = parsedLimit;
    logger.info(
      `[Departures Route] Applying time limit: ${limitMinutes} minutes`,
    );
  }

  try {
    // Pass limitMinutes to the service function
    const departures = await mtaService.getDeparturesForStation(
      stationId,
      limitMinutes,
    );
    res.json(departures);
  } catch (err) {
    handleServiceError(
      err,
      res,
      `Failed to retrieve departures for station ${stationId}.`,
    );
  }
};
router.get("/departures/:stationId", getDeparturesHandler);
// GET /api/v1/departures/:stationId (e.g., /departures/LIRR-3 for Penn LIRR)
router.get("/departures/:stationId", getDeparturesHandler);

// GET /api/v1/alerts
const getAlertsHandler: RequestHandler = async (req, res) => {
  // Filter by affected lines (comma-separated string)
  const linesQuery = req.query.lines as string | undefined;
  let targetLines: string[] | undefined = undefined;
  if (linesQuery) {
    targetLines = linesQuery
      .split(",")
      .map((line) => line.trim().toUpperCase())
      .filter((line) => line.length > 0);
    logger.info(
      `[Alerts Route] Filtering for lines: [${targetLines.join(", ")}]`,
    );
  }

  // Filter for alerts currently active (optional)
  const activeNowQuery = req.query.activeNow as string | undefined;
  const filterActiveNow = activeNowQuery === "true" || activeNowQuery === "1"; // Check for true/1
  if (filterActiveNow) {
    logger.info(`[Alerts Route] Filtering for alerts active now.`);
  }

  try {
    // Pass filters to the service function
    const alerts = await getServiceAlerts(targetLines, filterActiveNow);
    res.json(alerts);
  } catch (err) {
    handleServiceError(err, res, "Failed to retrieve service alerts.");
  }
};
router.get("/alerts", getAlertsHandler);

export default router;
