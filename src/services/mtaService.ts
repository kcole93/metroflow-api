import { logger } from "../utils/logger";
import { analyticsService } from "../services/analyticsService";
import { fetchAndParseFeed } from "../utils/gtfsFeedParser";
import { getStaticData } from "./staticDataService";
import { getActiveServicesForToday } from "./calendarService";
import { parse as dateParse, format } from "date-fns";
import {
  Station,
  Departure,
  StaticRouteInfo,
  StaticStopInfo,
  Direction,
  SystemType,
  StaticData,
  PeakStatus,
  DepartureSource,
  StaticStopTimeInfo,
  StaticTripInfo,
  AccessibilityStatus,
} from "../types";
import * as dotenv from "dotenv";

dotenv.config();

// --- Feed URL Constants ---
const MTA_API_BASE =
  process.env.MTA_API_BASE ||
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds";

const SUBWAY_FEEDS = {
  ACE: `${MTA_API_BASE}/nyct%2Fgtfs-ace`,
  BDFM: `${MTA_API_BASE}/nyct%2Fgtfs-bdfm`,
  G: `${MTA_API_BASE}/nyct%2Fgtfs-g`,
  JZ: `${MTA_API_BASE}/nyct%2Fgtfs-jz`,
  NQRW: `${MTA_API_BASE}/nyct%2Fgtfs-nqrw`,
  L: `${MTA_API_BASE}/nyct%2Fgtfs-l`,
  NUMERIC: `${MTA_API_BASE}/nyct%2Fgtfs`, // 1-6, S
  SI: `${MTA_API_BASE}/nyct%2Fgtfs-si`,
};
const LIRR_FEED = `${MTA_API_BASE}/lirr%2Fgtfs-lirr`;
const MNR_FEED = `${MTA_API_BASE}/mnr%2Fgtfs-mnr`;

// --- Route ID to Feed URL Map ---
export const ROUTE_ID_TO_FEED_MAP: { [key: string]: string } = {
  "SUBWAY-A": SUBWAY_FEEDS.ACE,
  "SUBWAY-C": SUBWAY_FEEDS.ACE,
  "SUBWAY-E": SUBWAY_FEEDS.ACE,
  "SUBWAY-B": SUBWAY_FEEDS.BDFM,
  "SUBWAY-D": SUBWAY_FEEDS.BDFM,
  "SUBWAY-F": SUBWAY_FEEDS.BDFM,
  "SUBWAY-FX": SUBWAY_FEEDS.BDFM,
  "SUBWAY-M": SUBWAY_FEEDS.BDFM,
  "SUBWAY-G": SUBWAY_FEEDS.G,
  "SUBWAY-J": SUBWAY_FEEDS.JZ,
  "SUBWAY-Z": SUBWAY_FEEDS.JZ,
  "SUBWAY-N": SUBWAY_FEEDS.NQRW,
  "SUBWAY-Q": SUBWAY_FEEDS.NQRW,
  "SUBWAY-R": SUBWAY_FEEDS.NQRW,
  "SUBWAY-W": SUBWAY_FEEDS.NQRW,
  "SUBWAY-L": SUBWAY_FEEDS.L,
  "SUBWAY-1": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-2": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-3": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-4": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-5": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-5X": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-6": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-6X": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-S": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-GS": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-SI": SUBWAY_FEEDS.SI,
  "SUBWAY-7": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-7X": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-FS": SUBWAY_FEEDS.NUMERIC,
  "LIRR-1": LIRR_FEED,
  "LIRR-2": LIRR_FEED,
  "LIRR-3": LIRR_FEED,
  "LIRR-4": LIRR_FEED,
  "LIRR-5": LIRR_FEED,
  "LIRR-6": LIRR_FEED,
  "LIRR-7": LIRR_FEED,
  "LIRR-8": LIRR_FEED,
  "LIRR-9": LIRR_FEED,
  "LIRR-10": LIRR_FEED,
  "LIRR-11": LIRR_FEED,
  "LIRR-12": LIRR_FEED,
  "MNR-1": MNR_FEED,
  "MNR-2": MNR_FEED,
  "MNR-3": MNR_FEED,
  "MNR-4": MNR_FEED,
  "MNR-5": MNR_FEED,
  "MNR-6": MNR_FEED,
};

/**
 * Fixes a platform direction issue for M trains in Williamsburg/Bushwick.
 * The MTA GTFS-RT feed has a known bug where M train platform directions
 * are inverted for certain stations.
 *
 * @param tripId The trip ID or route identifier for logging
 * @param stopId The stop ID to fix if needed
 * @returns The corrected stop ID
 */
/**
 * Fixes platform direction issues for M trains in Williamsburg/Bushwick.
 * 
 * The MTA GTFS-RT feed has a known bug where M train platform directions
 * are inverted for certain stations. This function corrects the issue by
 * swapping N/S suffixes for the affected stations.
 * 
 * @param tripId - The trip ID or route identifier for logging purposes
 * @param stopId - The stop ID to fix if needed (e.g., "M13N")
 * @returns The corrected stop ID or the original if no fix was needed
 */
function fixMTrainPlatformsInBushwick(tripId: string, stopId: string): string {
  // List of stations with inverted platform directions
  const buggyStations = new Set([
    "M11", // Myrtle Av
    "M12", // Central Av
    "M13", // Knickerbocker Av
    "M14", // Myrtle-Wyckoff Avs
    "M16", // Seneca Av
    "M18", // Forest Av
  ]);

  // Skip if the stop ID doesn't match our pattern or isn't in our list
  if (stopId.length !== 4 || !buggyStations.has(stopId.substring(0, 3))) {
    return stopId;
  }

  // Invert N/S direction
  const stationBase = stopId.substring(0, 3);
  const direction = stopId.charAt(3);
  let newDirection = "N";

  if (direction === "N") {
    newDirection = "S";
  }

  const newStopId = stationBase + newDirection;
  logger.debug(
    `[M Train Fix] Corrected platform for ${tripId}: ${stopId} → ${newStopId}`,
  );

  return newStopId;
}

// --- Helper function to determine per-station accessibility
/**
 * Determines accessibility status and notes for a transit station.
 * 
 * This function evaluates a station's accessibility based on ADA status
 * and wheelchair boarding information. It provides both a standardized
 * status classification and any additional notes regarding accessibility.
 * 
 * @param stopInfo - The static stop information for the station
 * @returns Object containing accessibilityStatus (enum) and accessibilityNotes (string or null)
 */
function getStationAccessibilityInfo(stopInfo: StaticStopInfo): {
  accessibilityStatus: AccessibilityStatus;
  accessibilityNotes: string | null;
} {
  logger.debug(
    // Use info level for clarity during debugging
    `[Accessibility Check] Station: ${stopInfo.name} (${stopInfo.id}), System: ${stopInfo.system}, ` +
      `Source adaStatus: ${stopInfo.adaStatus} (Type: ${typeof stopInfo.adaStatus}), ` +
      `Source wheelchairBoarding: ${stopInfo.wheelchairBoarding} (Type: ${typeof stopInfo.wheelchairBoarding})`,
  );

  let accessibilityStatus: AccessibilityStatus = "No Information"; // Default

  if (stopInfo.system === "SUBWAY") {
    switch (stopInfo.adaStatus) {
      case 1:
        accessibilityStatus = "Fully Accessible";
        break;
      case 2:
        accessibilityStatus = "Partially Accessible";
        break;
      case 0:
        accessibilityStatus = "Not Accessible";
        break;
      default:
        // Keep default "No Information" for null/undefined/other
        break;
    }
  } else if (stopInfo.system === "LIRR" || stopInfo.system === "MNR") {
    switch (stopInfo.wheelchairBoarding) {
      case 1:
        // LIRR/MNR 1 maps to 'Accessible', no standard 'Partial' state here
        accessibilityStatus = "Fully Accessible";
        break;
      case 2:
        accessibilityStatus = "Not Accessible";
        break;
      case 0:
      default:
        break;
    }
  }

  const accessibilityNotes = stopInfo.adaNotes || null;

  return { accessibilityStatus, accessibilityNotes };
}

// --- getStations Function ---
/**
 * Retrieves stations matching the specified search criteria.
 * 
 * This function searches through the static station data to find stations
 * that match the provided query string and/or system filter. It performs
 * case-insensitive partial matching on station names and provides detailed
 * information about matching stations including coordinates, lines served,
 * and accessibility information.
 * 
 * @param query - Optional search term to find stations by name
 * @param systemFilter - Optional system filter (SUBWAY, LIRR, MNR) to limit results
 * @returns Promise resolving to an array of matching Station objects
 * @throws Error if static data is not available
 */
export async function getStations(
  query?: string,
  systemFilter?: StaticStopInfo["system"],
): Promise<Station[]> {
  let staticData: StaticData;
  try {
    staticData = getStaticData(); // Must be loaded successfully first

    // Check if number of routes from StaticData matches number of routes defined in ROUTE_ID_TO_FEED_MAP
    const routeIdsFromStaticData = Array.from(staticData.routes.keys());
    const routeIdsFromMap = Object.keys(ROUTE_ID_TO_FEED_MAP);
    if (routeIdsFromStaticData.length !== routeIdsFromMap.length) {
      logger.warn(
        `# of routes from StaticData (${routeIdsFromStaticData.length}) do not match Routes from ROUTE_ID_TO_FEED_MAP (${routeIdsFromMap.length})`,
      );
    }
  } catch (err) {
    logger.error("[Departures] Static data not available:", { error: err });
    return [];
  }

  const stations: Station[] = [];
  const lowerCaseQuery = query?.toLowerCase();
  for (const [uniqueStopId, stopInfo] of staticData.stops.entries()) {
    if (systemFilter && stopInfo.system !== systemFilter) continue;
    const nameMatch =
      !lowerCaseQuery || stopInfo.name.toLowerCase().includes(lowerCaseQuery);
    if (!nameMatch) continue;
    let includeStop = false;
    if (stopInfo.system === "LIRR" || stopInfo.system === "MNR")
      includeStop = true;
    else if (stopInfo.system === "SUBWAY") {
      if (stopInfo.parentStationId == null || stopInfo.parentStationId === "")
        includeStop = true;
    } else includeStop = true;
    if (includeStop) {
      const lines: string[] = [];
      if (stopInfo.servedByRouteIds?.size > 0) {
        for (const routeId of stopInfo.servedByRouteIds) {
          const routeMapKey = `${stopInfo.system}-${routeId}`;
          const routeInfo = staticData.routes.get(routeMapKey);
          if (
            routeInfo?.route_short_name &&
            !lines.includes(routeInfo.route_short_name)
          )
            lines.push(routeInfo.route_short_name);
          else if (routeInfo && !lines.includes(routeId)) lines.push(routeId);
        }
      }

      const { accessibilityStatus, accessibilityNotes } =
        getStationAccessibilityInfo(stopInfo);

      stations.push({
        id: stopInfo.id,
        name: stopInfo.name,
        latitude: stopInfo.latitude,
        longitude: stopInfo.longitude,
        lines: lines.sort(),
        system: stopInfo.system,
        borough: stopInfo.borough ? stopInfo.borough : undefined,
        wheelchair_boarding: stopInfo.wheelchairBoarding || undefined,
        accessibilityStatus: accessibilityStatus,
        accessibilityNotes: accessibilityNotes || undefined,
      });
    }
  }
  stations.sort((a, b) => a.name.localeCompare(b.name));
  return stations;
}

// --- Helper function to map peak_offpeak value ---
function getPeakStatus(
  peakOffpeakValue: string | null | undefined,
): PeakStatus | null {
  if (peakOffpeakValue === "1") {
    return "Peak";
  } else if (peakOffpeakValue === "0") {
    return "Off-Peak";
  }
  return null;
}

// --- Helper function to determine system from feed URL ---
function getSystemFromFeedUrl(feedUrl: string): SystemType | null {
  if (feedUrl === LIRR_FEED) return "LIRR";
  if (feedUrl === MNR_FEED) return "MNR";

  for (const key in SUBWAY_FEEDS) {
    if (SUBWAY_FEEDS[key as keyof typeof SUBWAY_FEEDS] === feedUrl) {
      return "SUBWAY";
    }
  }

  return null;
}

// --- Helper to get normalized trip ID for MNR/LIRR ---
function normalizeTripId(tripId: string, systemName: SystemType): string {
  if (systemName === "MNR" || systemName === "LIRR") {
    // Remove leading zeros for MNR/LIRR trip IDs
    return tripId.replace(/^0+/, "");
  }
  return tripId;
}

// --- Helper to find static trip info for a given trip ID ---
function findStaticTripInfo(
  tripIdFromFeed: string,
  systemName: SystemType,
  entity: any,
  staticData: StaticData,
  processedRealtimeTripIds: Set<string>,
): { staticTripInfo: StaticTripInfo | null; effectiveTripId: string } {
  let staticTripInfo: StaticTripInfo | null = null;
  let effectiveTripId = tripIdFromFeed;

  if (systemName === "MNR") {
    // For MNR, need to use vehicleTripId (vehicle.vehicle.label) for lookup
    const vehicleLabel = entity.vehicle?.vehicle?.label?.trim();

    if (vehicleLabel) {
      // Use vehicleLabel to look up the actual trip_id via vehicleTripsMap or tripsByShortName
      const staticTripId =
        staticData.vehicleTripsMap?.get(vehicleLabel) ||
        staticData.tripsByShortName?.get(vehicleLabel);

      if (staticTripId) {
        const tripInfo = staticData.trips.get(staticTripId);
        staticTripInfo = tripInfo || null;
        logger.debug(
          `[MNR Trip] Found static trip using vehicle label ${vehicleLabel}: ${staticTripId}`,
        );

        // Track the vehicle label as a processed trip to avoid duplicates from static data
        processedRealtimeTripIds.add(vehicleLabel);
        effectiveTripId = vehicleLabel;
      }
    } else {
      // Fallback to the old method if vehicle label isn't available
      const staticTripId = staticData.tripsByShortName?.get(tripIdFromFeed);
      if (staticTripId) {
        const tripInfo = staticData.trips.get(staticTripId);
        staticTripInfo = tripInfo || null;
      }
    }

    if (!staticTripInfo) {
      // If no match by vehicle label or trip_short_name, try direct trip_id lookup as last resort
      const tripInfo = staticData.trips.get(tripIdFromFeed);
      staticTripInfo = tripInfo || null;
    }
  } else {
    // For LIRR and other systems, we can use the trip_id directly
    const tripInfo = staticData.trips.get(tripIdFromFeed);
    staticTripInfo = tripInfo || null;
  }

  return { staticTripInfo, effectiveTripId };
}

// --- Helper to determine trip direction ---
/**
 * Determines the human-readable direction of a trip based on multiple data sources.
 * 
 * This function uses a multi-step approach to determine the most accurate direction label:
 * - For MNR: Prioritizes static data (direction_id) with fallbacks to stop sequence analysis
 * - For Subway: Uses platform orientation (N/S) to lookup direction labels, with special handling for M trains
 * - For LIRR: Primarily uses static direction_id when available
 * 
 * The function accounts for complexities like parent/child station relationships and
 * corrects for known data issues in the GTFS feed.
 * 
 * @param systemName - The transit system (SUBWAY, LIRR, MNR)
 * @param staticTripInfo - Optional static trip information if available
 * @param rtTrip - The realtime trip object from the GTFS-RT feed
 * @param tripIdFromFeed - The trip ID string from the feed
 * @param stopTimeUpdates - Array of stop time updates for this trip
 * @param currentStopId - The stop ID where this trip is being observed
 * @param staticData - Reference to loaded static GTFS data
 * @returns The determined direction as a Direction type (N, S, Inbound, Outbound, or Unknown)
 */
function determineTripDirection(
  systemName: SystemType,
  staticTripInfo: StaticTripInfo | null,
  rtTrip: any,
  tripIdFromFeed: string,
  stopTimeUpdates: any[],
  currentStopId: string,
  staticData: StaticData,
): Direction {
  // Apply M train fix to correct inverted platform directions for M trains
  if (systemName === "SUBWAY" && rtTrip?.route_id === "M") {
    currentStopId = fixMTrainPlatformsInBushwick(tripIdFromFeed, currentStopId);
  }

  let tripDirection: Direction = "Unknown";

  // Special case for MNR - prioritize static data for accuracy, but have fallbacks
  if (systemName === "MNR") {
    // For MNR, first check the static data (most reliable source when available)
    logger.debug(
      `[MNR Direction Debug] Trip ${tripIdFromFeed} static data check: hasStaticTripInfo=${!!staticTripInfo}, direction_id=${staticTripInfo?.direction_id}`,
    );

    if (staticTripInfo?.direction_id != null) {
      const dirId = staticTripInfo.direction_id;
      if (dirId === 0) {
        tripDirection = "Outbound";
        logger.debug(
          `[MNR Direction] Trip ${tripIdFromFeed} direction from static data: Outbound (direction_id: 0)`,
        );
      } else if (dirId === 1) {
        tripDirection = "Inbound";
        logger.debug(
          `[MNR Direction] Trip ${tripIdFromFeed} direction from static data: Inbound (direction_id: 1)`,
        );
      } else {
        // Log unexpected direction_id values
        logger.warn(
          `[MNR Direction] Trip ${tripIdFromFeed} has unusual direction_id value: ${dirId}`,
        );
      }
    } else if (staticTripInfo) {
      logger.warn(
        `[MNR Direction] Trip ${tripIdFromFeed} has matched static data but direction_id is null or undefined`,
      );
    }

    // If static data didn't provide direction, infer from stop sequence
    if (tripDirection === "Unknown" && stopTimeUpdates.length > 0) {
      // Find the first and last stop in sequence
      let firstStop = stopTimeUpdates[0];
      let lastStop = stopTimeUpdates[0];
      let minSequence = Number(firstStop.stop_sequence) || 0;
      let maxSequence = Number(lastStop.stop_sequence) || 0;

      for (let i = 1; i < stopTimeUpdates.length; i++) {
        const currentSequence = Number(stopTimeUpdates[i].stop_sequence) || 0;
        if (currentSequence < minSequence) {
          minSequence = currentSequence;
          firstStop = stopTimeUpdates[i];
        }
        if (currentSequence > maxSequence) {
          maxSequence = currentSequence;
          lastStop = stopTimeUpdates[i];
        }
      }

      // Grand Central is stop_id 1
      const firstStopId = firstStop.stop_id?.trim();
      const lastStopId = lastStop.stop_id?.trim();

      if (lastStopId === "1") {
        // If train ends at Grand Central, it's inbound
        tripDirection = "Inbound";
        logger.debug(
          `[MNR Direction] Trip ${tripIdFromFeed} determined as Inbound (ends at Grand Central)`,
        );
      } else if (firstStopId === "1") {
        // If train starts at Grand Central, it's outbound
        tripDirection = "Outbound";
        logger.debug(
          `[MNR Direction] Trip ${tripIdFromFeed} determined as Outbound (starts at Grand Central)`,
        );
      }
    }

    // Log if we still don't have a direction
    if (tripDirection === "Unknown") {
      logger.warn(
        `[MNR Direction] Unable to determine direction for trip ${tripIdFromFeed}`,
      );
    }
  } else if (staticTripInfo) {
    // For LIRR, use static info if available
    if (systemName === "LIRR") {
      if (staticTripInfo.direction_id != null) {
        const dirId = staticTripInfo.direction_id;
        if (dirId === 0) tripDirection = "Outbound";
        else if (dirId === 1) tripDirection = "Inbound";
      }
    }
    // For the subway, we want real-time data
  } // --- SUBWAY LOGIC ---
  if (systemName === "SUBWAY") {
    logger.debug(
      `[Subway Direction Start] Trip: ${tripIdFromFeed}, Platform: ${currentStopId}`,
    );

    if (currentStopId && currentStopId.length > 1) {
      // ** Method 1: Use Stop ID Suffix to determine LOCAL direction **
      const platformSuffix = currentStopId
        .charAt(currentStopId.length - 1)
        .toUpperCase();
      let localLogicalDirection: "N" | "S" | null = null;

      if (platformSuffix === "N") {
        localLogicalDirection = "N";
      } else if (platformSuffix === "S") {
        localLogicalDirection = "S";
      } else {
        logger.warn(
          `[Subway Direction] Platform ID ${currentStopId} for trip ${tripIdFromFeed} does not end in N or S. Cannot determine local direction from suffix.`,
        );
      }

      logger.debug(
        `[Subway Direction] Determined local logical direction from suffix '${platformSuffix}': ${localLogicalDirection || "None"}`,
      );

      // ** Method 2: Look up Parent Station Label based on LOCAL direction **
      if (localLogicalDirection) {
        const currentPlatformKey = `${systemName}-${currentStopId}`;
        const currentPlatformInfo = staticData.stops.get(currentPlatformKey);
        // *** Find the PARENT station ID ***
        const parentStationKey = currentPlatformInfo?.parentStationId
          ? currentPlatformInfo.parentStationId
          : currentPlatformKey; // Fallback if no parent (should be rare for N/S stops)

        if (parentStationKey) {
          const parentStationInfo = staticData.stops.get(parentStationKey);

          if (parentStationInfo) {
            let label: string | null = null;
            if (localLogicalDirection === "N" && parentStationInfo.northLabel) {
              label = parentStationInfo.northLabel;
              logger.debug(
                `[Subway Direction] Using North Label from Parent ${parentStationKey}: "${label}"`,
              );
            } else if (
              localLogicalDirection === "S" &&
              parentStationInfo.southLabel
            ) {
              label = parentStationInfo.southLabel;
              logger.debug(
                `[Subway Direction] Using South Label from Parent ${parentStationKey}: "${label}"`,
              );
            } else {
              logger.warn(
                `[Subway Direction] Parent ${parentStationKey} found, but no matching label for local direction '${localLogicalDirection}' (NorthLabel: ${parentStationInfo.northLabel}, SouthLabel: ${parentStationInfo.southLabel})`,
              );
            }

            if (label) {
              tripDirection = label as Direction; // Use the user-friendly label!
            } else {
              // Fallback if label is missing, use the logical N/S
              tripDirection = localLogicalDirection; // Assign 'N' or 'S'
              logger.debug(
                `[Subway Direction] No specific label found, using logical direction: ${tripDirection}`,
              );
            }
          } else {
            logger.warn(
              `[Subway Direction] Could not find static info for parent/self station key ${parentStationKey} (derived from platform ${currentStopId}). Falling back to logical N/S.`,
            );
            // Fallback if parent lookup fails
            tripDirection = localLogicalDirection;
          }
        } else {
          logger.warn(
            `[Subway Direction] Could not determine parent station key for platform ${currentStopId}. Falling back to logical N/S.`,
          );
          // Fallback if parent key fails
          tripDirection = localLogicalDirection;
        }
      } // End if localLogicalDirection
    } else {
      logger.warn(
        `[Subway Direction] Invalid or missing currentStopId: ${currentStopId} for trip ${tripIdFromFeed}. Cannot determine direction.`,
      );
    }

    // ** Fallback (Optional): Use Trip-Level Direction if Primary Methods Failed **
    // This might be less reliable for display but could be a last resort
    if (tripDirection === "Unknown" || tripDirection === null) {
      // Check explicitly for null from localLogicalDirection assignment
      const nyctTripExt = rtTrip?.[".transit_realtime.nyct_trip_descriptor"];
      const feedDirectionId = nyctTripExt?.direction; // 1 or 3

      logger.warn(
        `[Subway Direction] Primary methods failed for ${currentStopId}. Trying fallback using trip direction ${feedDirectionId}.`,
      );

      if (feedDirectionId === 1)
        tripDirection = "N"; // Or "Northbound"
      else if (feedDirectionId === 3) tripDirection = "S"; // Or "Southbound"
      if (tripDirection !== "Unknown" && tripDirection !== null) {
        logger.debug(
          `[Subway Direction] Using Trip Feed ID Fallback: ${tripDirection}`,
        );
      }
    }

    if (tripDirection === "Unknown" || tripDirection === null) {
      logger.error(
        // Elevate to error if absolutely no direction found
        `[Subway Direction] FINAL UNABLE to determine direction for trip ${tripIdFromFeed} at platform ${currentStopId}`,
      );
      tripDirection = "Unknown"; // Ensure it's the string "Unknown"
    }
    logger.debug(
      `[Subway Direction End] Final direction for ${tripIdFromFeed} at ${currentStopId}: ${tripDirection}`,
    );
  } // --- END SUBWAY LOGIC ---

  // Final catch-all if still unknown (e.g., LIRR without static match)
  if (tripDirection === "Unknown") {
    logger.warn(
      `[Direction Determination] Final direction unknown for Trip ${tripIdFromFeed}, System: ${systemName}`,
    );
  }

  return tripDirection;
}

// --- Helper to find the destination for a trip ---
/**
 * Determines the final destination name and borough for a transit trip.
 * 
 * This function employs a multi-step process with system-specific logic:
 * - For MNR: Prioritizes static trip_headsign, falls back to destination stop ID
 * - For Subway: Prioritizes realtime last stop based on array order, with parent station lookup
 * - For LIRR: Uses stop sequence to find the last stop
 * 
 * The function includes fallback mechanisms to handle cases where primary data sources are unavailable,
 * and special handling for the M line which has known platform direction issues.
 * 
 * @param systemName - The transit system (SUBWAY, LIRR, MNR)
 * @param staticTripInfo - Optional static trip information if available
 * @param rtTrip - The realtime trip object from the GTFS-RT feed
 * @param stopTimeUpdates - Array of stop time updates for this trip
 * @param staticData - Reference to loaded static GTFS data
 * @returns Object containing the finalDestination string and destinationBorough (nullable)
 */
function determineDestination(
  systemName: SystemType,
  staticTripInfo: StaticTripInfo | null, 
  rtTrip: any,
  stopTimeUpdates: any[],
  staticData: StaticData,
): { finalDestination: string; destinationBorough: string | null } {
  // Apply M train fix to correct platform issues for stop IDs in stop time updates
  if (systemName === "SUBWAY" && rtTrip?.route_id === "M" && stopTimeUpdates) {
    // Get the trip ID for logging
    const tripId = rtTrip?.trip_id || "unknown";

    // Fix each stop time update's stop ID
    for (let i = 0; i < stopTimeUpdates.length; i++) {
      if (stopTimeUpdates[i]?.stop_id) {
        const originalStopId = stopTimeUpdates[i].stop_id;
        stopTimeUpdates[i].stop_id = fixMTrainPlatformsInBushwick(
          tripId,
          originalStopId,
        );

        // Only log if we actually made a change
        if (stopTimeUpdates[i].stop_id !== originalStopId) {
          logger.debug(
            `[M Train Fix][Destination] Corrected platform in stopTimeUpdates: ${originalStopId} → ${stopTimeUpdates[i].stop_id}`,
          );
        }
      }
    }
  }
  let finalDestination = "Unknown Destination";
  let destinationBorough: string | null = null;
  let destSource = "Unknown"; // For debugging purposes

  // For MNR trains, prioritize static trip data headsign first since we now have accurate mapping
  if (systemName === "MNR" && staticTripInfo) {
    // --- MNR Method 1: Try trip_headsign from static data first (most reliable for MNR) ---
    if (staticTripInfo.trip_headsign) {
      destSource = "Trip Headsign";
      finalDestination = staticTripInfo.trip_headsign;
      logger.debug(
        `[MNR Destination] Set from trip_headsign: ${finalDestination}`,
      );
    }
    // --- MNR Method 2: Try static destination stop ID if headsign not available ---
    else if (staticTripInfo.destinationStopId) {
      destSource = "Static DestinationStopId";
      const staticDestKey = `${staticTripInfo.system}-${staticTripInfo.destinationStopId}`;
      const staticDestStop = staticData.stops.get(staticDestKey);

      if (staticDestStop) {
        finalDestination = staticDestStop.name;
        destinationBorough = staticDestStop.borough || null;
        logger.debug(
          `[MNR Destination] Set from static destinationStopId: ${finalDestination} (${staticDestKey})`,
        );
      }
    }
    // --- MNR Method 3: Fall back to last stop in sequence ---
    else if (stopTimeUpdates.length > 0) {
      destSource = "Last Stop Calculation";
      // Find the stop time update with the maximum sequence number
      let lastStopUpdate = stopTimeUpdates[0];
      let maxSequence = Number(lastStopUpdate.stop_sequence) || 0;

      for (let i = 1; i < stopTimeUpdates.length; i++) {
        const currentSequence = Number(stopTimeUpdates[i].stop_sequence) || 0;
        if (currentSequence > maxSequence) {
          maxSequence = currentSequence;
          lastStopUpdate = stopTimeUpdates[i];
        }
      }

      const lastStopId = lastStopUpdate?.stop_id?.trim();
      if (lastStopId) {
        // Construct the unique key for the destination stop
        const destStopKey = `${systemName}-${lastStopId}`;
        const destStopInfo = staticData.stops.get(destStopKey);

        if (destStopInfo) {
          finalDestination = destStopInfo.name;
          destinationBorough = destStopInfo.borough || null;
          destSource = "Last Stop Name";
          logger.debug(
            `[MNR Destination] Set from last stop: ${finalDestination} (${destStopKey})`,
          );
        }
      }
    }
  } // --- SUBWAY: Prioritize Realtime Last Stop for Destination Accuracy ---
  // --- SUBWAY: Prioritize Realtime Last Stop based on ARRAY ORDER ---
  else if (systemName === "SUBWAY") {
    // --- Method 1: Last stop from THIS trip_update's stopTimeUpdates array ORDER ---
    if (stopTimeUpdates && stopTimeUpdates.length > 0) {
      // Check if array exists and is not empty
      destSource = "Realtime Last Stop (Order)";
      // Get the LAST element in the stopTimeUpdates array
      const lastStopUpdate = stopTimeUpdates[stopTimeUpdates.length - 1];

      if (lastStopUpdate && lastStopUpdate.stop_id) {
        const lastPlatformId = lastStopUpdate.stop_id.trim();
        const lastPlatformKey = `${systemName}-${lastPlatformId}`;
        // Get the stop_sequence value just for logging, DO NOT use it for logic
        const seqForLog =
          lastStopUpdate.stop_sequence !== undefined
            ? ` (Seq reported: ${lastStopUpdate.stop_sequence})`
            : "";
        logger.debug(
          `[Subway Destination] Last platform ID from RT update array order: ${lastPlatformId}${seqForLog}`,
        );

        // **Get the PARENT station name for the destination display**
        const lastPlatformInfo = staticData.stops.get(lastPlatformKey);
        const parentStationId = lastPlatformInfo?.parentStationId;
        // Construct parent key ONLY if parentStationId exists and is different from the platform's original ID
        const parentStationKey =
          parentStationId &&
          parentStationId !== lastPlatformInfo?.originalStopId
            ? parentStationId
            : lastPlatformKey; // Fallback: use the platform key itself if it's the parent or lookup failed

        logger.debug(
          `[Subway Destination] Looking up parent station key: ${parentStationKey} (derived from ${lastPlatformKey})`,
        );
        const destStopInfo = staticData.stops.get(parentStationKey);

        if (destStopInfo) {
          finalDestination = destStopInfo.name; // Use parent station name
          destinationBorough = destStopInfo.borough || null;
          logger.debug(
            `[Subway Destination] Set from Realtime Last Stop's Parent (Order): ${finalDestination} (Parent Key: ${parentStationKey})`,
          );
        } else {
          logger.warn(
            `[Subway Destination] Realtime last platform found (${lastPlatformKey}), but failed to look up static info for parent key ${parentStationKey}.`,
          );
          if (lastPlatformInfo) {
            finalDestination = lastPlatformInfo.name;
            destinationBorough = lastPlatformInfo.borough || null;
            destSource = "Realtime Last Stop (Order - Platform Name Fallback)";
            logger.debug(
              `[Subway Destination] Using platform name as fallback: ${finalDestination}`,
            );
          } else {
            logger.error(
              `[Subway Destination] Could not find static info for platform ${lastPlatformKey} either.`,
            );
          }
        }
      } else {
        logger.warn(
          `[Subway Destination] Last element in stopTimeUpdates array lacked a valid stop_id.`,
        );
      }
    } else {
      logger.warn(
        `[Subway Destination] No stopTimeUpdates provided in the realtime feed for trip ${rtTrip?.trip_id}. Cannot determine destination from RT.`,
      );
    }

    // --- Fallbacks using Static Data (ONLY if Realtime Last Stop failed) ---
    if (finalDestination === "Unknown Destination") {
      // (Keep your existing static fallback logic here - headsign, then static dest stop ID)
      logger.debug(
        `[Subway Destination] Realtime last stop method failed. Falling back to static data.`,
      );
      // Fallback 1: Static trip_headsign
      if (staticTripInfo?.trip_headsign) {
        destSource = "Static Trip Headsign (Fallback)";
        finalDestination = staticTripInfo.trip_headsign;
        logger.debug(
          `[Subway Destination] Using Fallback: Static trip_headsign: ${finalDestination}`,
        );
      }
      // Fallback 2: Static destinationStopId
      else if (staticTripInfo?.destinationStopId && staticTripInfo.system) {
        destSource = "Static DestinationStopId (Fallback)";
        const staticDestKey = `${staticTripInfo.system}-${staticTripInfo.destinationStopId}`;
        const staticDestPlatformInfo = staticData.stops.get(staticDestKey);
        const staticDestParentId = staticDestPlatformInfo?.parentStationId;
        const staticDestParentKey =
          staticDestParentId &&
          staticDestParentId !== staticDestPlatformInfo?.originalStopId
            ? `${staticTripInfo.system}-${staticDestParentId}`
            : staticDestKey;

        const staticDestStop = staticData.stops.get(staticDestParentKey);
        if (staticDestStop) {
          finalDestination = staticDestStop.name;
          destinationBorough = staticDestStop.borough || null;
          logger.debug(
            `[Subway Destination] Using Fallback: Static destinationStopId's Parent: ${finalDestination} (Parent Key: ${staticDestParentKey})`,
          );
        } else {
          logger.warn(
            `[Subway Destination] Static destination platform key ${staticDestKey} found, but failed lookup for parent ${staticDestParentKey}`,
          );
        }
      }
    } // --- END SUBWAY LOGIC ---
  } else {
    // --- For LIRR and other non-MNR/non-Subway systems ---

    // --- Method 1: Find the last stop in sequence ---
    if (stopTimeUpdates.length > 0) {
      destSource = "Last Stop Calculation";
      // Find the stop time update with the maximum sequence number
      let lastStopUpdate = stopTimeUpdates[0];
      let maxSequence = Number(lastStopUpdate.stop_sequence) || 0;

      for (let i = 1; i < stopTimeUpdates.length; i++) {
        const currentSequence = Number(stopTimeUpdates[i].stop_sequence) || 0;
        if (currentSequence > maxSequence) {
          maxSequence = currentSequence;
          lastStopUpdate = stopTimeUpdates[i];
        }
      }

      const lastStopId = lastStopUpdate?.stop_id?.trim();
      if (lastStopId && systemName) {
        // Construct the unique key for the destination stop
        const destStopKey = `${systemName}-${lastStopId}`;
        const destStopInfo = staticData.stops.get(destStopKey);

        if (destStopInfo) {
          finalDestination = destStopInfo.name;
          destinationBorough = destStopInfo.borough || null;
          destSource = "Last Stop Name";
          logger.debug(
            `[Destination] Set from last stop: ${finalDestination} (${destStopKey})`,
          );
        } else {
          logger.debug(
            `[Destination] Failed lookup for last stop key: ${destStopKey}`,
          );
        }
      }
    }

    // --- Method 2: Try static destination stop ID if method 1 failed ---
    if (
      finalDestination === "Unknown Destination" &&
      staticTripInfo?.destinationStopId &&
      staticTripInfo.system
    ) {
      destSource = "Static DestinationStopId";
      const staticDestKey = `${staticTripInfo.system}-${staticTripInfo.destinationStopId}`;
      const staticDestStop = staticData.stops.get(staticDestKey);

      if (staticDestStop) {
        finalDestination = staticDestStop.name;
        destinationBorough = staticDestStop.borough || null;
        logger.debug(
          `[Destination] Set from static destinationStopId: ${finalDestination} (${staticDestKey})`,
        );
      } else {
        logger.debug(
          `[Destination] Failed lookup for static destination stop key: ${staticDestKey}`,
        );
      }
    }

    // --- Method 3: Fallback to trip_headsign if methods 1 & 2 failed ---
    if (
      finalDestination === "Unknown Destination" &&
      staticTripInfo?.trip_headsign
    ) {
      destSource = "Trip Headsign";
      finalDestination = staticTripInfo.trip_headsign;
      logger.debug(`[Destination] Set from trip_headsign: ${finalDestination}`);
    }
  }

  // --- Method 4: Last resort for all systems - try route's long name ---
  if (
    finalDestination === "Unknown Destination" &&
    rtTrip?.route_id &&
    systemName
  ) {
    destSource = "Route Long Name";
    const routeKey = `${systemName}-${rtTrip.route_id}`;
    const routeInfo = staticData.routes.get(routeKey);

    if (routeInfo?.route_long_name) {
      finalDestination = routeInfo.route_long_name;
      logger.debug(
        `[Destination] Set from route_long_name: ${finalDestination}`,
      );
    }
  }

  if (finalDestination === "Unknown Destination") {
    logger.warn(`[Destination] Could not determine destination for trip`);
  }

  return { finalDestination, destinationBorough };
}

// --- Helper to determine if a stop is a terminal arrival ---
function checkIsTerminalArrival(
  stu: any,
  systemName: SystemType,
  stopTimeUpdates: any[],
  stuOriginalStopId: string,
  staticData: StaticData,
): boolean {
  // Check if this is the last stop of the trip
  let isLastStop = false;
  if (stopTimeUpdates.length > 0) {
    const thisSeq = Number(stu.stop_sequence) || 0;
    // Using explicit typing for the callback
    isLastStop = stopTimeUpdates.every(
      (otherStu: { stop_sequence?: string | number }) => {
        const otherSeq = Number(otherStu.stop_sequence) || 0;
        return otherSeq <= thisSeq;
      },
    );
  }

  // For MNR and LIRR, we need additional checks to identify terminal arrivals
  if (systemName === "MNR" || systemName === "LIRR") {
    // Get the unique stop key for this system and stop ID
    const stopKey = `${systemName}-${stuOriginalStopId}`;
    // Look up stop info to check if it's a terminal station
    const stopInfo = staticData.stops.get(stopKey);
    const isTerminalStation = stopInfo?.isTerminal || false;

    // If this is a known terminal station or the last stop of the trip
    if (isTerminalStation || isLastStop) {
      logger.debug(
        `[${systemName} Terminal] Identified terminal arrival at ${stuOriginalStopId} (${stopInfo?.name || "Unknown"}) for trip (isTerminalStation: ${isTerminalStation}, isLastStop: ${isLastStop})`,
      );
      return true;
    }
  } else if (isLastStop) {
    // For other systems, just use the last stop check
    return true;
  }

  return false;
}

// --- Helper to extract track information from stop time update ---
function extractTrackInfo(
  stu: any,
  systemName: SystemType,
  originalChildStopIds: Set<string>,
): string | undefined {
  // Get extensions appropriate to the system type
  const mtarrExtension =
    systemName === "LIRR" || systemName === "MNR"
      ? stu[".transit_realtime.mta_railroad_stop_time_update"]
      : null;

  const nyctExtension =
    systemName === "SUBWAY"
      ? stu[".transit_realtime.nyct_stop_time_update"]
      : null;

  // Determine track from available sources
  let track: string | undefined;

  // For LIRR, use ONLY the mtarrExtension track
  if (systemName === "LIRR") {
    // Check if this is the stop time update for the current station
    const isForCurrentStation = originalChildStopIds.has(stu.stop_id);

    // If it's for the current station and has the MTARR extension, extract track info
    if (mtarrExtension && isForCurrentStation) {
      // Always try to extract track information regardless of any other factors
      if (mtarrExtension.track && mtarrExtension.track.trim() !== "") {
        track = mtarrExtension.track;
        logger.debug(
          `[LIRR Track] Got track from MTARR extension: ${track} for stop ${stu.stop_id}`,
        );
      }
    }
  }
  // For MNR, use the mtarrExtension track similarly to LIRR
  else if (systemName === "MNR") {
    // Check if this is the stop time update for the current station
    const isForCurrentStation = originalChildStopIds.has(stu.stop_id);

    // If it's for the current station and has the MTARR extension, extract track info
    if (mtarrExtension && isForCurrentStation && mtarrExtension.track) {
      if (mtarrExtension.track.trim() !== "") {
        track = mtarrExtension.track;
        logger.debug(
          `[MNR Track] Got track from MTARR extension: ${track} for stop ${stu.stop_id}`,
        );
      }
    }
  }
  // For Subway, use the nyctExtension
  else if (systemName === "SUBWAY" && nyctExtension) {
    track = nyctExtension.actualTrack;
  }

  // Fallbacks for MNR and Subway only (not for LIRR)
  if (!track && systemName !== "LIRR") {
    track = stu.departure?.track || stu.arrival?.track || undefined;
  }

  // Log MNR track sources for debugging
  if (systemName === "MNR") {
    if (mtarrExtension || stu.departure?.track || stu.arrival?.track) {
      logger.debug(
        `[MNR Track] Trip sources: MTARR=${!!mtarrExtension}, departure=${!!stu.departure?.track}, arrival=${!!stu.arrival?.track}, final=${track}`,
      );
    }
  }

  return track;
}

// --- Helper to find static stop time info ---
function findStaticStopTimeInfo(
  stu: any,
  systemName: SystemType,
  tripIdFromFeed: string,
  staticTripInfo: StaticTripInfo | null,
  entity: any,
  staticData: StaticData,
): StaticStopTimeInfo | null {
  const stopTimesForStop = staticData.stopTimeLookup.get(stu.stop_id);
  let staticStopTimeInfo: StaticStopTimeInfo | null = null;

  if (!stopTimesForStop) {
    logger.debug(
      `[Stop Time Debug] No stop times found for stop ${stu.stop_id}`,
    );
    return null;
  }

  // Different lookup logic based on system type
  if (systemName === "MNR") {
    // For MNR, we need to try multiple possible keys:

    // 1. Try using the vehicle.label/trip_short_name (most reliable for MNR)
    const mnrVehicleLabel = entity.vehicle?.vehicle?.label?.trim();
    if (mnrVehicleLabel && stopTimesForStop.has(mnrVehicleLabel)) {
      staticStopTimeInfo = stopTimesForStop.get(mnrVehicleLabel) || null;
      logger.debug(
        `[MNR Stop Time] Found using vehicle label ${mnrVehicleLabel} for stop ${stu.stop_id}`,
      );
    }
    // 2. Try using the static trip ID if available
    else if (staticTripInfo && stopTimesForStop.has(staticTripInfo.trip_id)) {
      staticStopTimeInfo = stopTimesForStop.get(staticTripInfo.trip_id) || null;
      logger.debug(
        `[MNR Stop Time] Found using static trip ID ${staticTripInfo.trip_id} for stop ${stu.stop_id}`,
      );
    }
    // 3. Try using trip_short_name from static trip if available
    else if (
      staticTripInfo?.trip_short_name &&
      stopTimesForStop.has(staticTripInfo.trip_short_name)
    ) {
      staticStopTimeInfo =
        stopTimesForStop.get(staticTripInfo.trip_short_name) || null;
      logger.debug(
        `[MNR Stop Time] Found using trip_short_name ${staticTripInfo.trip_short_name} for stop ${stu.stop_id}`,
      );
    }
    // 4. Try with the normalized tripIdFromFeed as last resort
    else if (stopTimesForStop.has(tripIdFromFeed)) {
      staticStopTimeInfo = stopTimesForStop.get(tripIdFromFeed) || null;
      logger.debug(
        `[MNR Stop Time] Found using feed trip ID ${tripIdFromFeed} for stop ${stu.stop_id}`,
      );
    }
    // Log if we still couldn't find a match
    else {
      logger.debug(
        `[MNR Stop Time] No match found for stop ${stu.stop_id} using any ID. vehicleLabel=${entity.vehicle?.vehicle?.label?.trim() || "N/A"}, tripID=${tripIdFromFeed}, staticTripID=${staticTripInfo?.trip_id || "N/A"}, trip_short_name=${staticTripInfo?.trip_short_name || "N/A"}`,
      );

      // For debugging, show a sample of available keys in the stopTimesForStop map
      const availableKeys = Array.from(stopTimesForStop.keys())
        .slice(0, 5)
        .join(", ");
      logger.debug(
        `[MNR Stop Time Keys] Sample keys for stop ${stu.stop_id}: ${availableKeys}...`,
      );
    }
  } else {
    // For LIRR and Subway, use the tripIdFromFeed directly (simpler case)
    if (stopTimesForStop.has(tripIdFromFeed)) {
      staticStopTimeInfo = stopTimesForStop.get(tripIdFromFeed) || null;
      logger.debug(
        `[Stop Time] Found for ${systemName} using trip ID ${tripIdFromFeed} for stop ${stu.stop_id}`,
      );
    } else {
      logger.debug(
        `[Stop Time] No match found for ${systemName} stop ${stu.stop_id} with trip ID ${tripIdFromFeed}`,
      );
    }
  }

  // Log what we found
  if (staticStopTimeInfo) {
    logger.debug(
      `[Stop Time Found] For ${systemName} trip at stop ${stu.stop_id}: pickup=${staticStopTimeInfo.pickupType}, dropoff=${staticStopTimeInfo.dropOffType}`,
    );
  }

  return staticStopTimeInfo;
}

// --- Helper to get note text for a stop time ---
function getNoteText(
  staticStopTimeInfo: StaticStopTimeInfo | null,
  staticData: StaticData,
): string | null {
  if (staticStopTimeInfo?.noteId && staticData.notes) {
    const note = staticData.notes.get(staticStopTimeInfo.noteId);
    if (note) {
      logger.debug(
        `[Note] Found note for noteId ${staticStopTimeInfo.noteId}: "${note.noteDesc}"`,
      );
      return note.noteDesc || null;
    }
  }
  return null;
}

// --- Helper to determine departures status based on time and delay ---
function getDepartureStatus(
  relevantTime: number,
  now: number,
  delayMinutes: number | null,
): string {
  if (delayMinutes != null) {
    if (delayMinutes > 1) return `Delayed ${delayMinutes} min`;
    else if (delayMinutes < -1) return `Early ${Math.abs(delayMinutes)} min`;
    else return "On Time";
  } else {
    // Proximity if no delay but RT time exists
    const diffMillis = relevantTime - now;
    if (diffMillis < 120000 && diffMillis >= 30000) return "Approaching";
    else if (diffMillis < 30000 && diffMillis >= -30000) return "Due";
    // else remains "Scheduled" even with RT time if far out
    return "Scheduled";
  }
}

// --- Helper to create a realtime departure object ---
function createRealtimeDeparture(
  rtTrip: any,
  stu: any,
  entity: any,
  systemName: SystemType,
  staticData: StaticData,
  staticTripInfo: StaticTripInfo | null,
  effectiveTripId: string,
  tripDirection: Direction,
  finalDestination: string,
  destinationBorough: string | null,
  relevantTime: number,
  isTerminalArrival: boolean,
  track: string | undefined,
  now: number,
  originalChildStopIds: Set<string>,
): Departure | null {
  try {
    const hasRealtimePrediction = true;

    // --- Route Info Lookup ---
    const actualRouteId = rtTrip?.route_id?.trim();
    let routeInfo: StaticRouteInfo | undefined | null = null;

    if (actualRouteId && systemName) {
      const routeMapKey = `${systemName}-${actualRouteId}`;
      routeInfo = staticData.routes.get(routeMapKey);
      if (!routeInfo) {
        logger.debug(`[Route Lookup] Failed for key: "${routeMapKey}"`);
      }
    }

    // --- Delay ---
    const delaySecs = hasRealtimePrediction
      ? (stu.departure?.delay ?? stu.arrival?.delay)
      : null;
    const delayMinutes = delaySecs != null ? Math.round(delaySecs / 60) : null;

    // --- Status based on RT prediction ---
    const status = getDepartureStatus(relevantTime, now, delayMinutes);

    // --- Determine Peak Status ---
    const peakStatus = getPeakStatus(staticTripInfo?.peak_offpeak);

    // Log peak status for MNR trains (to help verify correct data flow)
    if (systemName === "MNR" && staticTripInfo?.peak_offpeak) {
      logger.debug(
        `[MNR Peak Status] Trip has peak_offpeak=${staticTripInfo.peak_offpeak}, parsed as ${peakStatus || "null"}`,
      );
    }

    // Create the scheduled departureTime
    const scheduledTime = relevantTime ? new Date(relevantTime) : null;

    // Calculate the estimatedDepartureTime based on delays
    let estimatedTime: Date | null = null;
    if (scheduledTime && delayMinutes !== null) {
      estimatedTime = new Date(
        scheduledTime.getTime() + delayMinutes * 60 * 1000,
      );
    } else {
      estimatedTime = scheduledTime; // If no delay, estimated = scheduled
    }

    // Extract trainStatus from MTA railroad extensions if available for MNR and LIRR
    let trainStatus: string | null = null;
    const mtarrExtension =
      stu[".transit_realtime.mta_railroad_stop_time_update"];

    if (
      (systemName === "MNR" || systemName === "LIRR") &&
      mtarrExtension &&
      mtarrExtension.trainStatus
    ) {
      // Only use non-empty trainStatus values
      if (
        mtarrExtension.trainStatus &&
        mtarrExtension.trainStatus.trim() !== ""
      ) {
        trainStatus = mtarrExtension.trainStatus;
      }
    }

    // Get stop time info for pickup/dropoff details
    const staticStopTimeInfo = findStaticStopTimeInfo(
      stu,
      systemName,
      effectiveTripId,
      staticTripInfo,
      entity,
      staticData,
    );

    // Get note text if available
    const noteText = getNoteText(staticStopTimeInfo, staticData);

    // Create the departure object
    const departure: Departure = {
      id: `${effectiveTripId}-${stu.stop_id}-${relevantTime}`,
      tripId: effectiveTripId,
      routeId: actualRouteId,
      routeShortName: routeInfo?.route_short_name || "",
      routeLongName: routeInfo?.route_long_name || "",
      peakStatus: peakStatus,
      routeColor: routeInfo?.route_color || null,
      destination: finalDestination,
      departureTime: scheduledTime,
      estimatedDepartureTime: estimatedTime,
      delayMinutes: delayMinutes,
      track: track,
      status: status,
      direction: tripDirection,
      system: systemName,
      destinationBorough,
      isTerminalArrival: isTerminalArrival || undefined,
      source: "realtime",
      // Add trainStatus from MTARR extensions for MNR/LIRR
      trainStatus,
      // Add pickup_type, drop_off_type, and note_id from static stop time info
      pickupType: staticStopTimeInfo?.pickupType || null,
      dropOffType: staticStopTimeInfo?.dropOffType || null,
      noteId: staticStopTimeInfo?.noteId || null,
      noteText: noteText,
    };

    return departure;
  } catch (error) {
    logger.error("Error creating realtime departure:", { error });
    return null;
  }
}

// --- Helper to create a scheduled departure from static data ---
function createScheduledDeparture(
  staticTripId: string,
  tripInfo: StaticTripInfo,
  stopTimeInfo: StaticStopTimeInfo,
  platformId: string,
  systemName: SystemType,
  staticData: StaticData,
  scheduledTime: Date,
): Departure | null {
  try {
    const actualRouteId = tripInfo.route_id;
    const routeMapKey = `${systemName}-${actualRouteId}`;
    const routeInfo = staticData.routes.get(routeMapKey);

    let finalDestination = tripInfo.destinationStopId
      ? staticData.stops.get(`${systemName}-${tripInfo.destinationStopId}`)
          ?.name
      : null;
    if (!finalDestination) finalDestination = tripInfo.trip_headsign;
    if (!finalDestination)
      finalDestination = routeInfo?.route_short_name || "Unknown Destination";

    let direction: Direction = "Unknown";

    // Determine direction based on system type
    if (systemName === "MNR") {
      if (
        tripInfo?.direction_id !== undefined &&
        tripInfo?.direction_id !== null
      ) {
        const parsedDirId = Number(String(tripInfo.direction_id));
        if (parsedDirId === 1) {
          direction = "Outbound";
        } else if (parsedDirId === 0) {
          direction = "Inbound";
        }
      }
    } else {
      // Standard behavior for non-MNR trips
      if (tripInfo?.direction_id === 0) direction = "Outbound";
      else if (tripInfo?.direction_id === 1) direction = "Inbound";
    }

    // Determine peak status
    const peakStatus = getPeakStatus(tripInfo?.peak_offpeak);

    // For MNR static trips, include trip_short_name (which corresponds to vehicle.label)
    const effectiveTripId =
      systemName === "MNR" && tripInfo.trip_short_name
        ? tripInfo.trip_short_name // Use trip_short_name as the displayed tripId for MNR
        : staticTripId; // Use static ID for other systems

    // Look up and include note text if available (mainly for MNR)
    let noteText: string | null = null;
    if (stopTimeInfo.noteId && staticData.notes) {
      const note = staticData.notes.get(stopTimeInfo.noteId);
      if (note) {
        noteText = note.noteDesc || null;
      }
    }

    // For static data, estimatedDepartureTime equals departureTime (no delay)
    const departure: Departure = {
      id: staticTripId,
      tripId: effectiveTripId, // Use trip_short_name for MNR, static ID for others
      routeId: actualRouteId,
      routeShortName: routeInfo?.route_short_name || "",
      routeLongName: routeInfo?.route_long_name || "",
      peakStatus: peakStatus,
      routeColor: routeInfo?.route_color || null,
      destination: finalDestination,
      departureTime: scheduledTime, // Use SCHEDULED Date object
      estimatedDepartureTime: scheduledTime, // For static data, estimated = scheduled
      delayMinutes: null, // No delay info
      // Include track information from stop_times.txt if available
      track: stopTimeInfo.track || undefined,
      status: "Scheduled", // Explicitly set status
      direction: direction,
      destinationBorough:
        staticData.stops.get(`${systemName}-${tripInfo.destinationStopId}`)
          ?.borough || null,
      system: systemName,
      // Determine if this is a terminal arrival based on station info
      isTerminalArrival: (() => {
        // First, check if this is a known terminal station
        const stopKey = `${systemName}-${platformId}`;
        const stopInfo = staticData.stops.get(stopKey);

        // Consider it a terminal if the station is marked as terminal or the direction suggests it
        // (direction_id = 1 typically means inbound to a major terminal for commuter rail)
        return stopInfo?.isTerminal ||
          ((systemName === "MNR" || systemName === "LIRR") &&
            tripInfo.direction_id === 1)
          ? true
          : false;
      })(),
      source: "scheduled",

      // Add pickup_type, drop_off_type, and note_id from stop time info
      pickupType: stopTimeInfo.pickupType || null,
      dropOffType: stopTimeInfo.dropOffType || null,
      noteId: stopTimeInfo.noteId || null,
      noteText: noteText,
    };

    return departure;
  } catch (error) {
    logger.error("Error creating scheduled departure:", { error });
    return null;
  }
}

// --- Process realtime feed entities ---
/**
 * Processes GTFS-RT feed entities to extract departure information.
 * 
 * This function transforms raw GTFS-RT data into structured departure objects by:
 * 1. Filtering relevant trip updates that affect the requested station
 * 2. Matching realtime data with static schedule information when available
 * 3. Determining trip directions and destinations
 * 4. Calculating delays based on scheduled vs. realtime timestamps
 * 5. Handling system-specific extensions (NYCT, MTARR)
 * 
 * @param decodedEntities - Array of decoded GTFS-RT feed entities
 * @param systemName - The transit system the feed belongs to
 * @param staticData - Reference to loaded static GTFS data
 * @param originalChildStopIds - Set of stop IDs relevant to the requested station
 * @param processedRealtimeTripIds - Set to track processed trip IDs (for deduplication)
 * @param now - Current timestamp in milliseconds
 * @param cutoffTime - Maximum future timestamp in milliseconds to include departures
 * @returns Array of Departure objects derived from realtime data
 */
async function processRealtimeFeedEntities(
  decodedEntities: any[],
  systemName: SystemType,
  staticData: StaticData,
  originalChildStopIds: Set<string>,
  processedRealtimeTripIds: Set<string>,
  now: number,
  cutoffTime: number,
): Promise<Departure[]> {
  const realtimeDepartures: Departure[] = [];
  let totalUpdatesProcessed = 0;

  // --- Inside the real-time processing loop ---
  for (const entity of decodedEntities) {
    const trip_update = entity.trip_update;
    const vehicle = entity.vehicle;

    // Process trip_update entities (the main source of departure data)
    if (trip_update?.stop_time_update?.length > 0) {
      totalUpdatesProcessed++;
      const stopTimeUpdates = trip_update.stop_time_update;
      const rtTrip = trip_update.trip;

      // 1. Get and Validate IDs
      let tripIdFromFeed = rtTrip?.trip_id?.trim();
      if (!tripIdFromFeed) {
        logger.debug("[RT Skip] Entity missing trip_id.");
        continue; // Cannot proceed without trip_id
      }

      // Normalize trip ID for better matching
      tripIdFromFeed = normalizeTripId(tripIdFromFeed, systemName);

      if (systemName === "MNR" || systemName === "LIRR") {
        logger.debug(
          `[Trip ID] Normalized realtime ${systemName} trip ID: ${tripIdFromFeed}`,
        );
      }

      // Look up the static trip data using appropriate methods
      const { staticTripInfo, effectiveTripId } = findStaticTripInfo(
        tripIdFromFeed,
        systemName,
        entity,
        staticData,
        processedRealtimeTripIds,
      );

      if (!staticTripInfo) {
        logger.warn(
          `[RT Process] Static trip info not found for trip ${tripIdFromFeed}. Processing STUs without static context.`,
        );

        // For MNR, we need further investigation before rejecting trips
        if (systemName === "MNR") {
          // Log detailed information about this MNR trip for debugging
          logger.info(
            `[MNR Analysis] Trip ${tripIdFromFeed} doesn't exist in static data. Extensions available: ${Object.keys(
              rtTrip || {},
            )
              .filter((k) => k.startsWith("."))
              .join(", ")}`,
          );
        }
      }

      // 2. Determine Destination
      const { finalDestination, destinationBorough } = determineDestination(
        systemName,
        staticTripInfo,
        rtTrip,
        stopTimeUpdates,
        staticData,
      );

      // Track processed trip to avoid duplicates in static data
      processedRealtimeTripIds.add(tripIdFromFeed);

      // 3. Loop through Stop Time Updates (STUs)
      for (const stu of stopTimeUpdates) {
        const stuOriginalStopId = stu.stop_id?.trim();

        // Check if STU matches station
        if (
          !stuOriginalStopId ||
          !originalChildStopIds.has(stuOriginalStopId)
        ) {
          continue;
        }

        // Check for valid future time - prioritize departures, but handle terminal stations
        const departureTimeLong = stu.departure?.time;
        const arrivalTimeLong = stu.arrival?.time;
        let relevantTime: number;
        let isTerminalArrival = false;

        // First, try to use departure time if available
        if (departureTimeLong && Number(departureTimeLong) > 0) {
          relevantTime = Number(departureTimeLong) * 1000;
          // This is a normal departure
        } else if (systemName === "MNR" || systemName === "LIRR") {
          // For commuter rail, handle terminal station arrivals
          if (arrivalTimeLong && Number(arrivalTimeLong) > 0) {
            relevantTime = Number(arrivalTimeLong) * 1000;

            // Check if this is a terminal arrival
            isTerminalArrival = checkIsTerminalArrival(
              stu,
              systemName,
              stopTimeUpdates,
              stuOriginalStopId,
              staticData,
            );

            // Always include terminal arrivals
            const includeTerminalArrivals = true;

            if (!isTerminalArrival || includeTerminalArrivals) {
              logger.debug(
                `[MNR/LIRR] Using arrival time for stop ${stuOriginalStopId} on trip ${tripIdFromFeed} (${isTerminalArrival ? "terminal arrival" : "non-terminal stop"})`,
              );
            } else {
              logger.debug(
                `[MNR/LIRR] Skipping terminal arrival at ${stuOriginalStopId} on trip ${tripIdFromFeed}`,
              );
              continue; // Skip terminal arrivals if we don't want to include them
            }
          } else {
            // No valid time at all
            logger.debug(
              `[RT Skip] No valid departure or arrival time for stop ${stuOriginalStopId} on trip ${tripIdFromFeed}`,
            );
            continue;
          }
        } else {
          // For subway, we strictly require departure times
          logger.debug(
            `[RT Skip] No valid departure time for stop ${stuOriginalStopId} on trip ${tripIdFromFeed}`,
          );
          continue;
        }

        // Check if time is within our window
        const isTimeValid =
          relevantTime >= now - 60000 && relevantTime <= cutoffTime;
        if (!isTimeValid) {
          logger.debug(
            `[RT Skip] Time ${new Date(relevantTime).toISOString()} for stop ${stuOriginalStopId} on trip ${tripIdFromFeed} out of window.`,
          );
          continue;
        }
        // Determine trip direction
        const tripDirection = determineTripDirection(
          systemName,
          staticTripInfo,
          rtTrip,
          tripIdFromFeed,
          stopTimeUpdates,
          stuOriginalStopId,
          staticData,
        );

        // Extract track information
        const track = extractTrackInfo(stu, systemName, originalChildStopIds);

        // 5. Create Departure Object
        const departure = createRealtimeDeparture(
          rtTrip,
          stu,
          entity,
          systemName,
          staticData,
          staticTripInfo,
          effectiveTripId,
          tripDirection,
          finalDestination,
          destinationBorough,
          relevantTime,
          isTerminalArrival,
          track,
          now,
          originalChildStopIds,
        );

        if (departure) {
          realtimeDepartures.push(departure);
        }
      } // End STU loop
    } // End if trip_update has STUs

    // Process vehicle entities to use vehicleTripId (vehicle.id/label) for MNR trip lookups
    // For MNR trips, the vehicle.id/label should be used to find the corresponding trip_id
    if (
      systemName === "MNR" &&
      vehicle?.trip?.trip_id &&
      vehicle.vehicle?.label
    ) {
      const vehicleLabel = vehicle.vehicle.label.trim();
      // Add vehicleLabel to processedRealtimeTripIds to ensure we don't duplicate this trip from static data
      processedRealtimeTripIds.add(vehicleLabel);
    }
  } // End entity loop

  return realtimeDepartures;
}

// --- Process static schedule data for a station ---
/**
 * Processes static schedule data to supplement realtime departures.
 * 
 * This function generates departure information from static GTFS schedule data
 * for cases where realtime updates are unavailable. It:
 * 1. Filters for trips scheduled for today based on service_id
 * 2. Excludes trips already covered by realtime data
 * 3. Calculates scheduled departure times for the requested station
 * 4. Creates departure objects with "scheduled" source designation
 * 
 * Static departures serve as a fallback when realtime data is missing or incomplete.
 * 
 * @param systemName - The transit system to process
 * @param originalChildStopIds - Set of stop IDs relevant to the requested station
 * @param staticData - Reference to loaded static GTFS data
 * @param processedRealtimeTripIds - Set of trip IDs already handled by realtime data
 * @param now - Current timestamp in milliseconds
 * @param cutoffTime - Maximum future timestamp in milliseconds to include departures
 * @param todayStr - Today's date in YYYYMMDD format for service calendar lookup
 * @returns Array of Departure objects derived from static schedule
 */
async function processStaticScheduleData(
  systemName: SystemType,
  originalChildStopIds: Set<string>,
  staticData: StaticData,
  processedRealtimeTripIds: Set<string>,
  now: number,
  cutoffTime: number,
  todayStr: string,
): Promise<Departure[]> {
  const scheduledDepartures: Departure[] = [];
  let addedScheduled = 0;

  try {
    const activeServices = await getActiveServicesForToday(); // Get today's active service IDs
    logger.debug(
      `[Static Fallback ${systemName}] Found ${activeServices.size} active services today.`,
    );

    // Iterate through the station's platforms (original IDs)
    for (const platformId of originalChildStopIds) {
      const tripsStoppingAtPlatform =
        staticData.stopTimeLookup?.get(platformId);

      if (!tripsStoppingAtPlatform) {
        logger.debug(
          `[Static Fallback] No trips found for platform ${platformId}`,
        );
        continue;
      }

      logger.debug(
        `[Static Fallback] Found ${tripsStoppingAtPlatform.size} trips for platform ${platformId}`,
      );

      // Iterate through static trips scheduled to stop at this platform
      for (const [
        staticTripId,
        stopTimeInfo,
      ] of tripsStoppingAtPlatform.entries()) {
        // Skip if this trip was ALREADY processed via real-time feed
        let normalizedStaticTripId = normalizeTripId(staticTripId, systemName);

        if (normalizedStaticTripId !== staticTripId) {
          logger.debug(
            `[Trip ID] Normalized static ${systemName} trip ID: ${staticTripId} -> ${normalizedStaticTripId}`,
          );
        }

        // Check if trip should be skipped (already in realtime data)
        let shouldSkip = processedRealtimeTripIds.has(normalizedStaticTripId);

        if (!shouldSkip && systemName === "MNR") {
          // Check MNR-specific case using trip_short_name
          const tripWithShortName = staticData.trips.get(staticTripId);
          if (
            tripWithShortName?.trip_short_name &&
            processedRealtimeTripIds.has(tripWithShortName.trip_short_name)
          ) {
            shouldSkip = true;
            logger.silly(
              `[Static Skip] MNR Trip ${staticTripId} with short_name ${tripWithShortName.trip_short_name} already processed in realtime feed`,
            );
          }
        }

        if (shouldSkip) {
          logger.debug(
            `[Static Skip] Trip ${staticTripId} already processed in realtime feed`,
          );
          continue;
        }

        // Get static trip details
        const tripInfo = staticData.trips.get(staticTripId);

        // Ensure trip exists, matches the system, and has an active service ID
        if (
          !tripInfo ||
          tripInfo.system !== systemName ||
          !tripInfo.service_id ||
          !activeServices.has(tripInfo.service_id)
        ) {
          if (!tripInfo) {
            logger.silly(
              ` -> SKIP ${staticTripId}: Not found in staticData.trips map.`,
            );
          } else if (tripInfo.system !== systemName) {
            logger.silly(
              `-> SKIP ${staticTripId}: System mismatch (${tripInfo.system} != ${systemName}).`,
            );
          } else if (!tripInfo.service_id) {
            logger.silly(
              `-> SKIP ${staticTripId}: Missing service_id in tripInfo.`,
            );
          } else if (!activeServices.has(tripInfo.service_id)) {
            logger.silly(
              `-> SKIP ${staticTripId}: Service ID [${tripInfo.service_id}] not found in active list.`,
            );
          }
          continue; // Skip this trip
        }

        // Get scheduled time (prioritize departure)
        const scheduledTimeStr =
          stopTimeInfo.scheduledDepartureTime ||
          stopTimeInfo.scheduledArrivalTime;
        if (!scheduledTimeStr) continue;

        // Check pickup type - if pickup is not allowed (type = 1), skip this stop time
        if (stopTimeInfo.pickupType === 1) {
          logger.debug(
            `[Static Skip] Trip ${staticTripId} at stop ${platformId} has pickup_type=1 (no pickup allowed), skipping`,
          );
          continue;
        }

        // Construct Date object for scheduled time today
        let scheduledTime: Date | null = null;
        try {
          // Handle times > 24:00:00 if necessary
          let hours = parseInt(scheduledTimeStr.substring(0, 2), 10);
          let parseDateStr = todayStr;
          const adjustedHour = hours % 24;
          let parseTimeStr = scheduledTimeStr;

          if (!isNaN(hours) && hours >= 24) {
            // Time is on the next day
            const nextDay = new Date(now + 24 * 60 * 60 * 1000);
            parseDateStr = format(nextDay, "yyyy-MM-dd");
            // Adjust time string for parsing
            parseTimeStr = `${String(adjustedHour).padStart(2, "0")}:${scheduledTimeStr.substring(3)}`;
          }

          // Parse the date
          scheduledTime = dateParse(
            `${parseDateStr} ${parseTimeStr}`,
            "yyyy-MM-dd HH:mm:ss",
            new Date(),
          );
        } catch (parseError) {
          logger.error(
            `[Static Fallback] Error parsing time ${scheduledTimeStr}:`,
            { error: parseError, time: scheduledTimeStr },
          );
          continue;
        }

        // Check if scheduled time is valid and in the future window
        const relevantTime = scheduledTime?.getTime();
        if (
          relevantTime === null ||
          isNaN(relevantTime) ||
          relevantTime < now - 60000 ||
          relevantTime > cutoffTime
        ) {
          if (relevantTime) {
            logger.debug(
              `-> Skipping scheduled ${staticTripId}, time ${new Date(relevantTime).toISOString()} out of window`,
            );
          }
          continue;
        }

        // Create the scheduled departure
        const departure = createScheduledDeparture(
          staticTripId,
          tripInfo,
          stopTimeInfo,
          platformId,
          systemName,
          staticData,
          scheduledTime,
        );

        if (departure) {
          scheduledDepartures.push(departure);
          addedScheduled++;
        }
      } // End loop tripsStoppingHere
    } // End loop platformId

    logger.debug(
      `[Departures] Added ${addedScheduled} scheduled departures via static fallback for ${systemName}.`,
    );

    return scheduledDepartures;
  } catch (fallbackError) {
    logger.error(
      `[Departures] Error during static fallback for ${systemName}:`,
      { error: fallbackError },
    );
    return [];
  }
}

/**
 * Retrieves upcoming departures for a specified station across all transit systems (subway, rail).
 * 
 * This is the main entry point for departure data retrieval. The function:
 * 1. Validates the station ID and retrieves station information
 * 2. Determines which realtime feeds to fetch based on routes serving the station
 * 3. Processes both realtime and scheduled data to create a comprehensive departure list
 * 4. Applies requested filters and sorts departures by time
 * 
 * @param requestedUniqueStationId - The unique station ID in format "SYSTEM-STOPID" (e.g., "SUBWAY-L11" or "LIRR-237")
 * @param limitMinutes - Optional time limit in minutes; only returns departures within this window
 * @param sourceFilter - Optional filter to show only realtime or only scheduled departures
 * @returns Promise resolving to an array of Departure objects sorted by departure time
 * @throws Error if static data is not available or station cannot be found
 */
export async function getDeparturesForStation(
  requestedUniqueStationId: string, 
  limitMinutes?: number,
  sourceFilter?: DepartureSource,
): Promise<Departure[]> {
  // --- 1. Initialize and load static data ---
  let staticData: StaticData;
  try {
    staticData = getStaticData();
  } catch (err) {
    logger.error("[Departures] Static data not available:", err);
    return [];
  }

  // --- 2. Get station info and validate ---
  // Apply M train fix if this is an M train stop in Williamsburg/Bushwick
  let finalStationId = requestedUniqueStationId;
  if (
    requestedUniqueStationId.startsWith("SUBWAY-M1") &&
    requestedUniqueStationId.length >= 11
  ) {
    // Extract the stop ID part (e.g., "M13N" from "SUBWAY-M13N")
    const stopIdPart = requestedUniqueStationId.substring(7);
    // Apply the fix to get the corrected stop ID
    const fixedStopId = fixMTrainPlatformsInBushwick("stationId", stopIdPart);
    // Only update if there was a change
    if (fixedStopId !== stopIdPart) {
      finalStationId = `SUBWAY-${fixedStopId}`;
      logger.info(
        `[M Train Fix][Station] Corrected station ID: ${requestedUniqueStationId} → ${finalStationId}`,
      );
    }
  }

  const requestedStationInfo = staticData.stops.get(finalStationId);
  const stationName = requestedStationInfo?.name || "(Unknown)";

  analyticsService.trackStationLookup(
    requestedStationInfo?.system || "UNKNOWN",
    requestedUniqueStationId,
    stationName,
  );

  if (!requestedStationInfo) {
    logger.warn(
      `[Departures] Station info not found for unique ID: ${requestedUniqueStationId}`,
    );
    return [];
  }

  // --- 3. Get Child Stop IDs ---
  const originalChildStopIds = new Set<string>(
    requestedStationInfo.childStopIds,
  );

  // If no children, use the station's own ORIGINAL stop ID for matching STUs
  if (originalChildStopIds.size === 0) {
    if (requestedStationInfo.originalStopId) {
      logger.debug(
        `[Departures] No child stops for ${requestedUniqueStationId}, using own original ID: ${requestedStationInfo.originalStopId}.`,
      );
      originalChildStopIds.add(requestedStationInfo.originalStopId);
    } else {
      logger.warn(
        `[Departures] No child stops AND no original_stop_id for ${requestedUniqueStationId}. Cannot process.`,
      );
      return [];
    }
  }

  // --- 4. Get feed URLs and prepare for fetching ---
  const feedUrlsToFetch = Array.from(requestedStationInfo.feedUrls);

  logger.debug(
    `[Departures] Fetching for ${
      requestedStationInfo.name
    } (${requestedUniqueStationId}), checking ORIGINAL stop IDs: [${Array.from(
      originalChildStopIds,
    ).join(", ")}] from feeds: ${feedUrlsToFetch.join(", ") || "None"}`,
  );

  // --- 5. Fetch feeds in parallel ---
  const feedPromises = feedUrlsToFetch.map(async (url) => {
    const feedName = `feed_${url.split("/").pop()}`;
    const result = await fetchAndParseFeed(url, feedName);
    return { url, result };
  });

  // --- 6. Initialize data structures ---
  const realtimeDepartures: Departure[] = [];
  const scheduledDepartures: Departure[] = [];
  const processedRealtimeTripIds = new Set<string>();
  const now = Date.now();
  const todayStr = format(new Date(now), "yyyy-MM-dd");
  const cutoffTime =
    limitMinutes && limitMinutes > 0
      ? now + limitMinutes * 60 * 1000
      : Infinity;
  let totalEntitiesFetched = 0;

  try {
    // --- 7. Process realtime feeds ---
    const feedFetchResults = await Promise.all(feedPromises);
    logger.info(
      `[Departures] Processing results for ${feedFetchResults.length} fetched feeds...`,
    );

    for (const { url: feedUrl, result: fetchedData } of feedFetchResults) {
      if (!fetchedData?.message?.entity) {
        logger.warn(`-> Skipping feed ${feedUrl}: No valid data returned.`);
        continue;
      }

      const decodedEntities = fetchedData.message.entity;
      totalEntitiesFetched += decodedEntities.length;

      // Determine system for this feed
      const systemName = getSystemFromFeedUrl(feedUrl);
      if (!systemName) {
        logger.warn(`-> Could not determine system for ${feedUrl}. Skipping.`);
        continue;
      }

      logger.debug(`-> Processing feed for System: ${systemName}`);

      // Process entities from this feed
      const departures = await processRealtimeFeedEntities(
        decodedEntities,
        systemName,
        staticData,
        originalChildStopIds,
        processedRealtimeTripIds,
        now,
        cutoffTime,
      );

      realtimeDepartures.push(...departures);
    }

    // --- 8. Process static schedule data if needed ---
    const system = requestedStationInfo.system;
    const needStaticFallback =
      system === "LIRR" ||
      system === "MNR" ||
      (system === "SUBWAY" && realtimeDepartures.length === 0);

    if (needStaticFallback) {
      logger.debug(`[Departures] Checking static fallback for ${system}...`);

      const staticDepartures = await processStaticScheduleData(
        system,
        originalChildStopIds,
        staticData,
        processedRealtimeTripIds,
        now,
        cutoffTime,
        todayStr,
      );

      scheduledDepartures.push(...staticDepartures);
    }

    // --- 9. Apply source filter ---
    let filteredRealtime = realtimeDepartures;
    let filteredScheduled = scheduledDepartures;

    if (sourceFilter === "realtime") {
      filteredScheduled = []; // Exclude scheduled departures
      logger.debug(
        `[Departures] Filtering to show only realtime departures (${filteredRealtime.length})`,
      );
    } else if (sourceFilter === "scheduled") {
      filteredRealtime = []; // Exclude realtime departures
      logger.debug(
        `[Departures] Filtering to show only scheduled departures (${filteredScheduled.length})`,
      );
    }

    // --- 10. Combine and sort departures ---
    const combinedDepartures = [...filteredRealtime, ...filteredScheduled];

    logger.debug(
      `[Departures] Combined ${filteredRealtime.length} realtime and ${filteredScheduled.length} scheduled departures (source filter: ${sourceFilter || "none"})`,
    );

    // Sort by direction then time
    combinedDepartures.sort((a, b) => {
      const dirOrder: Record<string, number> = {
        Uptown: 1,
        Northbound: 1,
        Downtown: 2,
        Southbound: 2,
        Inbound: 3,
        Outbound: 4,
        Unknown: 5,
        zzz: 6,
      };
      const dirA = dirOrder[a.direction || "Unknown"] ?? dirOrder["zzz"];
      const dirB = dirOrder[b.direction || "Unknown"] ?? dirOrder["zzz"];
      if (dirA !== dirB) {
        return dirA - dirB;
      }
      const timeA = a.departureTime?.getTime() ?? Infinity;
      const timeB = b.departureTime?.getTime() ?? Infinity;
      return timeA - timeB;
    });

    return combinedDepartures;
  } catch (err) {
    logger.error("[Departures] Error processing departures:", { error: err });
    return [];
  }
}
