import {
  Router,
  Request,
  Response,
  NextFunction,
  RequestHandler,
} from "express";
import * as mtaService from "../services/mtaService";
// Import the specific types needed if possible, or define allowed systems here
import { StaticStopInfo } from "../types"; // Import if StaticStopInfo['system'] defines the types

const router = Router();

// *** Define ONLY the system values ALLOWED as query parameters ***
const ALLOWED_QUERY_SYSTEMS = ["LIRR", "SUBWAY", "MNR"] as const;
// Create a type from these allowed values if needed for the service function signature
type FilterableSystem = (typeof ALLOWED_QUERY_SYSTEMS)[number];

// Error handler
const handleServiceError = (
  err: unknown,
  res: Response,
  defaultMessage: string,
) => {
  console.error("Error handled in route:", err);
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

  // Variable to hold the validated filter, typed correctly
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
        error: `Invalid system query parameter. Must be one of: ${ALLOWED_QUERY_SYSTEMS.join(", ")}`, // Use the correct constant
      });
      return;
    }
  }

  try {
    // Ensure mtaService.getStations signature accepts FilterableSystem | undefined
    const stations = await mtaService.getStations(query, systemFilter);
    res.json(stations);
  } catch (err) {
    handleServiceError(err, res, "Failed to retrieve stations.");
  }
};

router.get("/stations", getStationsHandler);

// --- Explicitly type the handler ---
const getDeparturesHandler: RequestHandler = async (req, res) => {
  const stationId = req.params.stationId;
  if (!stationId) {
    // Send the response...
    res.status(400).json({ error: "Station ID parameter is required." });
    // ...then return void (implicitly) to satisfy RequestHandler type
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
    console.log(
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
// GET /api/v1/departures/:stationId (e.g., /departures/L03 for Penn LIRR)
router.get("/departures/:stationId", getDeparturesHandler);

// GET /api/v1/alerts
router.get(
  "/alerts",
  async (req: Request, res: Response, next: NextFunction) => {
    try {
      const alerts = await mtaService.getServiceAlerts();
      res.json(alerts);
    } catch (err) {
      handleServiceError(err, res, "Failed to retrieve service alerts.");
    }
  },
);

export default router;
