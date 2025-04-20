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
} from "../types";
import * as dotenv from "dotenv";

dotenv.config();

// --- Feed URL Constants ---
const MTA_API_BASE =
  process.env.MTA_API_BASE ||
  "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds";

export const SUBWAY_FEEDS = {
  ACE: `${MTA_API_BASE}/nyct%2Fgtfs-ace`,
  BDFM: `${MTA_API_BASE}/nyct%2Fgtfs-bdfm`,
  G: `${MTA_API_BASE}/nyct%2Fgtfs-g`,
  JZ: `${MTA_API_BASE}/nyct%2Fgtfs-jz`,
  NQRW: `${MTA_API_BASE}/nyct%2Fgtfs-nqrw`,
  L: `${MTA_API_BASE}/nyct%2Fgtfs-l`,
  NUMERIC: `${MTA_API_BASE}/nyct%2Fgtfs`, // 1-6, S
  SI: `${MTA_API_BASE}/nyct%2Fgtfs-si`,
};
export const LIRR_FEED = `${MTA_API_BASE}/lirr%2Fgtfs-lirr`;
export const MNR_FEED = `${MTA_API_BASE}/mnr%2Fgtfs-mnr`;

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

// --- getStations Function ---
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
    logger.error("[Departures] Static data not available:", err);
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
      stations.push({
        id: stopInfo.id,
        name: stopInfo.name,
        latitude: stopInfo.latitude,
        longitude: stopInfo.longitude,
        lines: lines.sort(),
        system: stopInfo.system,
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
// ---

// --- Get Realtime Departures ForStation (with Static Fallback for LIRR/MNR) ---
export async function getDeparturesForStation(
  requestedUniqueStationId: string, // e.g., "SUBWAY-L11" or "LIRR-237"
  limitMinutes?: number,
  sourceFilter?: DepartureSource, // Optional filter for realtime or scheduled departures
): Promise<Departure[]> {
  let staticData: StaticData;
  try {
    staticData = getStaticData(); // Must be loaded successfully first
  } catch (err) {
    logger.error("[Departures] Static data not available:", err);
    return [];
  }

  const requestedStationInfo = staticData.stops.get(requestedUniqueStationId); // Lookup by unique key
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

  // Get ORIGINAL Child Stop IDs (like "L11N", "128S", etc.)
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

  const feedUrlsToFetch = Array.from(requestedStationInfo.feedUrls);
  // Note: For LIRR/MNR, feedUrlsToFetch might be empty but we'll still run static fallback
  // if there are no feeds to fetch

  logger.debug(
    `[Departures] Fetching for ${
      requestedStationInfo.name
    } (${requestedUniqueStationId}), checking ORIGINAL stop IDs: [${Array.from(
      originalChildStopIds,
    ).join(", ")}] from feeds: ${feedUrlsToFetch.join(", ") || "None"}`,
  );

  // Fetch feeds in parallel, keeping track of source URL
  const feedPromises = feedUrlsToFetch.map(async (url) => {
    const feedName = `feed_${url.split("/").pop()}`;
    const result = await fetchAndParseFeed(url, feedName);
    return { url, result };
  });

  const realtimeDepartures: Departure[] = []; // Store RT departures separately
  const scheduledDepartures: Departure[] = []; // Store scheduled departures separately
  const processedRealtimeTripIds = new Set<string>(); // Track trips found in RT feed
  const now = Date.now();
  const todayStr = format(new Date(now), "yyyy-MM-dd"); // Use date-fns format
  const cutoffTime =
    limitMinutes && limitMinutes > 0
      ? now + limitMinutes * 60 * 1000
      : Infinity;
  let totalEntitiesFetched = 0;
  let totalUpdatesProcessed = 0;
  let totalDeparturesCreated = 0;

  try {
    const feedFetchResults = await Promise.all(feedPromises);

    logger.info(
      `[Departures] Processing results for ${feedFetchResults.length} fetched feeds...`,
    );
    for (const { url: feedUrl, result: fetchedData } of feedFetchResults) {
      if (!fetchedData?.message?.entity) {
        logger.warn(`-> Skipping feed ${feedUrl}: No valid data returned.`);
        continue; // Skip empty/failed feeds
      }

      const decodedEntities = fetchedData.message.entity;
      totalEntitiesFetched += decodedEntities.length;

      // Determine System for this specific feed's data
      let systemName: SystemType | null = null;
      if (feedUrl === LIRR_FEED) systemName = "LIRR";
      else if (feedUrl === MNR_FEED) systemName = "MNR";
      else {
        for (const key in SUBWAY_FEEDS) {
          if (SUBWAY_FEEDS[key as keyof typeof SUBWAY_FEEDS] === feedUrl) {
            systemName = "SUBWAY";
            break;
          }
        }
      }

      if (!systemName) {
        logger.warn(`-> Could not determine system for ${feedUrl}. Skipping.`);
        continue;
      }
      logger.debug(`-> Processing feed for System: ${systemName}`);

      // --- Inside the real-time processing loop ---
      for (const entity of decodedEntities) {
        const trip_update = entity.trip_update;
        const vehicle = entity.vehicle;

        // Process trip_update entities (the main source of departure data)
        if (trip_update?.stop_time_update?.length > 0) {
          totalUpdatesProcessed++;
          const stopTimeUpdates = trip_update.stop_time_update;
          const rtTrip = trip_update.trip;

          // --- 1. Get and Validate IDs ---
          let tripIdFromFeed = rtTrip?.trip_id?.trim();
          if (!tripIdFromFeed) {
            logger.debug("[RT Skip] Entity missing trip_id.");
            continue; // Cannot proceed without trip_id
          }

          // Normalize MNR/LIRR trip IDs for proper matching with static data
          // This helps avoid duplicates caused by different ID formats
          if (systemName === "MNR" || systemName === "LIRR") {
            // Log original ID for debugging
            logger.debug(
              `[Trip ID] Original realtime MNR/LIRR trip ID: ${tripIdFromFeed}`,
            );

            // Try to normalize the trip ID to match static data format
            // Remove any leading zeros that might be in static data but not realtime
            tripIdFromFeed = tripIdFromFeed.replace(/^0+/, "");

            logger.debug(
              `[Trip ID] Normalized realtime MNR/LIRR trip ID: ${tripIdFromFeed}`,
            );
          }

          // For MNR, need to use vehicleTripId (vehicle.vehicle.label) for lookup, not trip_id in the feed
          let staticTripInfo = null;
          if (systemName === "MNR") {
            // Check if we have vehicle info to get the vehicle label (the actual trip_short_name)
            const vehicleLabel = entity.vehicle?.vehicle?.label?.trim();
            if (vehicleLabel) {
              logger.debug(`[MNR Trip] Using vehicle label for lookup: ${vehicleLabel}`);
              
              // Use vehicleLabel to look up the actual trip_id via vehicleTripsMap or tripsByShortName
              const staticTripId = staticData.vehicleTripsMap?.get(vehicleLabel) || 
                                  staticData.tripsByShortName?.get(vehicleLabel);
              if (staticTripId) {
                staticTripInfo = staticData.trips.get(staticTripId);
                logger.debug(
                  `[MNR Trip] Found static trip using vehicle label ${vehicleLabel}: ${staticTripId}`,
                );

                // Track the vehicle label as a processed trip to avoid duplicates from static data
                processedRealtimeTripIds.add(vehicleLabel);
                
                // Verify direction_id is present (diagnostics for "Unknown" direction issue)
                if (staticTripInfo) {
                  logger.debug(
                    `[MNR Trip Detail] Trip matched via vehicle label ${vehicleLabel} to ${staticTripId} with direction_id=${staticTripInfo.direction_id !== undefined && staticTripInfo.direction_id !== null ? staticTripInfo.direction_id : "MISSING"}`,
                  );
                }
              }
            } else {
              // Fallback to the old method if vehicle label isn't available
              logger.debug(`[Trip ID] No vehicle label, trying trip_id: ${tripIdFromFeed}`);
              // Use the tripsByShortName lookup map to find the static trip_id
              const staticTripId = staticData.tripsByShortName?.get(tripIdFromFeed);
              if (staticTripId) {
                staticTripInfo = staticData.trips.get(staticTripId);
                logger.debug(
                  `[MNR Trip] Found static trip with trip_short_name ${tripIdFromFeed}: ${staticTripId}`,
                );

                // Verify direction_id is present (diagnostics for "Unknown" direction issue)
                if (staticTripInfo) {
                  logger.debug(
                    `[MNR Trip Detail] Trip ${tripIdFromFeed} matched to ${staticTripId} with direction_id=${staticTripInfo.direction_id !== undefined && staticTripInfo.direction_id !== null ? staticTripInfo.direction_id : "MISSING"}`,
                  );
                }
              }
            }

            if (!staticTripInfo) {
              // If no match by trip_short_name, log details and fall back to direct trip_id lookup
              logger.debug(
                `[MNR Analysis] No static trip found for short_name=${tripIdFromFeed}, trying direct trip_id lookup`,
              );

              // Try direct trip_id (rare but possible for testing/debugging)
              staticTripInfo = staticData.trips.get(tripIdFromFeed);

              if (!staticTripInfo) {
                // Log additional details for unmatched MNR trips to help with debugging
                if (rtTrip?.route_id) {
                  logger.debug(
                    `[MNR Analysis] Unmatched trip ${tripIdFromFeed} is on route_id=${rtTrip.route_id}`,
                  );

                  // Check for similar trips on this route to provide context
                  let similarTripsFound = 0;
                  for (const [_, trip] of staticData.trips.entries()) {
                    if (
                      trip.system === "MNR" &&
                      trip.route_id === rtTrip.route_id
                    ) {
                      similarTripsFound++;
                      if (similarTripsFound <= 3) {
                        // Limit logging to avoid excessive output
                        logger.debug(
                          `[MNR Analysis] Similar trip on route ${rtTrip.route_id}: trip_id=${trip.trip_id}, trip_short_name=${trip.trip_short_name || "N/A"}`,
                        );
                      }
                    }
                  }
                  if (similarTripsFound > 0) {
                    logger.debug(
                      `[MNR Analysis] Found ${similarTripsFound} similar trips on route ${rtTrip.route_id}`,
                    );
                  }
                }
              }
            }
          } else {
            // For all other systems, use the direct trip_id lookup
            staticTripInfo = staticData.trips.get(tripIdFromFeed);
          }
          if (!staticTripInfo) {
            logger.warn(
              `[RT Process] Static trip info not found for trip ${tripIdFromFeed}. Processing STUs without static context.`,
            );

            // For MNR, we need further investigation before rejecting trips
            // For now, just log this for analysis but don't filter
            if (systemName === "MNR") {
              // Log detailed information about this MNR trip for debugging
              logger.info(
                `[MNR Analysis] Trip ${tripIdFromFeed} doesn't exist in static data. Extensions available: ${Object.keys(
                  rtTrip || {},
                )
                  .filter((k) => k.startsWith("."))
                  .join(", ")}`,
              );

              // Do NOT skip for now - we need to analyze the data first
              // continue;
            }
            // For other systems, we'll proceed with limited info (for backward compatibility)
            // continue; // Option: Skip entirely if static data is essential
          }

          // --- 2. Determine Trip Direction (Once per Trip Update) ---
          let tripDirection: Direction = "Unknown";

          // Special case for MNR - prioritize static data for accuracy, but have fallbacks
          if (systemName === "MNR") {
            // For MNR, first check the static data (most reliable source when available)
            // Log ALL direction info attempts for MNR to diagnose "Unknown" direction issue
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
                const currentSequence =
                  Number(stopTimeUpdates[i].stop_sequence) || 0;
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
            // For other systems, use static info if available
            if (systemName === "LIRR") {
              if (staticTripInfo.direction_id != null) {
                const dirId = staticTripInfo.direction_id;
                if (dirId === 0) tripDirection = "Outbound";
                else if (dirId === 1) tripDirection = "Inbound";
              }
            } else if (systemName === "SUBWAY") {
              const nyctTripExt =
                rtTrip?.[".transit_realtime.nyct_trip_descriptor"];
              if (nyctTripExt?.direction === 1) tripDirection = "N";
              else if (nyctTripExt?.direction === 3) tripDirection = "S";
              // Add static fallback for Subway if needed
            }
          }
          // Note: Direction remains 'Unknown' if direction couldn't be determined

          // --- 3. Determine Destination (using different prioritization for MNR vs other systems) ---
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
                const currentSequence =
                  Number(stopTimeUpdates[i].stop_sequence) || 0;
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
          } else {
            // --- For non-MNR systems, keep existing priority order ---
            
            // --- Method 1: If we have stop time updates, find the last stop in sequence ---
            if (stopTimeUpdates.length > 0) {
              destSource = "Last Stop Calculation";
              // Find the stop time update with the maximum sequence number
              let lastStopUpdate = stopTimeUpdates[0];
              let maxSequence = Number(lastStopUpdate.stop_sequence) || 0;

              for (let i = 1; i < stopTimeUpdates.length; i++) {
                const currentSequence =
                  Number(stopTimeUpdates[i].stop_sequence) || 0;
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
              logger.debug(
                `[Destination] Set from trip_headsign: ${finalDestination}`,
              );
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
            if (systemName === "MNR") {
              const vehicleLabel = entity.vehicle?.vehicle?.label?.trim();
              logger.warn(
                `[MNR Destination] Could not determine destination for trip ${tripIdFromFeed}, vehicle label: ${vehicleLabel || "N/A"}`,
              );
            } else {
              logger.warn(
                `[Destination] Could not determine destination for trip ${tripIdFromFeed}`,
              );
            }
          } else if (systemName === "MNR") {
            const vehicleLabel = entity.vehicle?.vehicle?.label?.trim();
            logger.debug(
              `[MNR Destination] Final destination for vehicle ${vehicleLabel || "N/A"}: ${finalDestination} (source: ${destSource})`,
            );
          }

          processedRealtimeTripIds.add(tripIdFromFeed); // Track processed trip

          // --- 4. Loop through Stop Time Updates (STUs) ---
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

                // For MNR, we need additional checks to identify terminal arrivals
                if (systemName === "MNR") {
                  // Identify if this is a terminal station by:
                  // 1. Check if this is Grand Central (stop_id 1)
                  // 2. Check if this is the last stop sequence

                  // Grand Central is stop_id 1 for MNR
                  const isGrandCentral = stuOriginalStopId === "1";

                  // If we're at Grand Central or it's the last stop of the trip
                  if (isGrandCentral || isLastStop) {
                    isTerminalArrival = true;
                    logger.debug(
                      `[MNR Terminal] Identified terminal arrival at ${stuOriginalStopId} for trip ${tripIdFromFeed} (isGrandCentral: ${isGrandCentral}, isLastStop: ${isLastStop})`,
                    );
                  }
                } else if (isLastStop) {
                  // For LIRR and other systems, just use the last stop check
                  isTerminalArrival = true;
                }

                // Always include terminal arrivals now (changed from false to true)
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

            // --- 5. Create Departure Object ---
            try {
              // Gather details from STU (track, delay, status)
              // Get peak status from staticTripInfo
              // Get route info

              const hasRealtimePrediction = true;
              // --- Route Info Lookup ---
              const actualRouteId = rtTrip?.route_id?.trim();
              let routeInfo: StaticRouteInfo | undefined | null = null;

              if (actualRouteId && systemName) {
                const routeMapKey = `${systemName}-${actualRouteId}`;
                routeInfo = staticData.routes.get(routeMapKey);
                if (!routeInfo)
                  logger.debug(
                    `[Route Lookup] Failed for key: "${routeMapKey}"`,
                  );
              }

              // --- Direction ---
              const direction = tripDirection;

              // --- Track ---
              const nyctExtension =
                stu[".transit_realtime.nyct_stop_time_update"];
              const mtarrExtension =
                stu[".transit_realtime.mta_railroad_stop_time_update"];

              // Determine track from available sources
              let track =
                nyctExtension?.actualTrack ||
                mtarrExtension?.track ||
                stu.departure?.track ||
                stu.arrival?.track ||
                undefined;

              // Flag suspicious MNR trains (heuristic approach)
              let isSuspiciousTrip = false;

              if (systemName === "MNR") {
                // Log sources for debugging
                if (
                  mtarrExtension ||
                  stu.departure?.track ||
                  stu.arrival?.track
                ) {
                  logger.debug(
                    `[MNR Track] Trip ${tripIdFromFeed}, sources: MTARR=${!!mtarrExtension}, departure=${!!stu.departure?.track}, arrival=${!!stu.arrival?.track}, final=${track}`,
                  );
                }

                // Suspicious indicator #1: Trip not in static data AND has unusual track number
                if (!staticTripInfo && track) {
                  const trackNum = parseInt(track, 10);
                  // MNR Grand Central tracks are typically 1-48 (upper level 100+ tracks are suspicious for Grand Central)
                  if (isNaN(trackNum) || trackNum > 50) {
                    isSuspiciousTrip = true;
                    logger.warn(
                      `[MNR Suspicious] Trip ${tripIdFromFeed} has suspicious track number: ${track}`,
                    );
                  }
                }

                // Suspicious indicator #2: Trip with extremely short trip ID that doesn't exist in static data
                if (!staticTripInfo && tripIdFromFeed.length < 4) {
                  isSuspiciousTrip = true;
                  logger.warn(
                    `[MNR Suspicious] Trip ${tripIdFromFeed} has suspiciously short ID`,
                  );
                }
              }
              // --- End Track ---

              // --- Delay ---
              const delaySecs = hasRealtimePrediction
                ? (stu.departure?.delay ?? stu.arrival?.delay)
                : null;
              const delayMinutes =
                delaySecs != null ? Math.round(delaySecs / 60) : null;
              // --- End Delay ---

              // --- Status ---
              let status = "Scheduled";
              if (hasRealtimePrediction && relevantTime) {
                // Status based on RT prediction
                if (delayMinutes != null) {
                  if (delayMinutes > 1) status = `Delayed ${delayMinutes} min`;
                  else if (delayMinutes < -1)
                    status = `Early ${Math.abs(delayMinutes)} min`;
                  else status = "On Time";
                } else {
                  // Proximity if no delay but RT time exists
                  const diffMillis = relevantTime - now;
                  if (diffMillis < 120000 && diffMillis >= 30000)
                    status = "Approaching";
                  else if (diffMillis < 30000 && diffMillis >= -30000)
                    status = "Due";
                  // else remains "Scheduled" even with RT time if far out
                }

                // ---  Determine Peak Status
                const peakStatus = getPeakStatus(staticTripInfo?.peak_offpeak);

                // Log peak status for MNR trains (to help verify correct data flow)
                if (systemName === "MNR" && staticTripInfo?.peak_offpeak) {
                  logger.debug(
                    `[MNR Peak Status] Trip ${tripIdFromFeed} has peak_offpeak=${staticTripInfo.peak_offpeak}, parsed as ${peakStatus || "null"}`,
                  );
                }

                // Create the scheduled departureTime
                const scheduledTime = relevantTime
                  ? new Date(relevantTime)
                  : null;

                // Calculate the estimatedDepartureTime based on delays
                let estimatedTime: Date | null = null;
                if (scheduledTime && delayMinutes !== null) {
                  estimatedTime = new Date(
                    scheduledTime.getTime() + delayMinutes * 60 * 1000,
                  );
                } else {
                  estimatedTime = scheduledTime; // If no delay, estimated = scheduled
                }

                // For MNR trips, use the vehicle label as the tripId if available
                const mnrVehicleLabel = systemName === "MNR" ? entity.vehicle?.vehicle?.label?.trim() : null;
                const effectiveTripId = systemName === "MNR" && mnrVehicleLabel ? mnrVehicleLabel : tripIdFromFeed;
                
                // Extract trainStatus from MTA railroad extensions if available
                let trainStatus: string | null = null;
                
                // Get MTA railroad extensions if available
                const mtarrExtension = stu[".transit_realtime.mta_railroad_stop_time_update"];
                if (systemName === "MNR" && mtarrExtension && mtarrExtension.trainStatus) {
                  trainStatus = mtarrExtension.trainStatus;
                  
                  // Log trainStatus for debugging purposes
                  logger.debug(`[MNR Extension] Train ${effectiveTripId} has trainStatus: ${trainStatus}`);
                }
                
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
                  direction: direction,
                  system: systemName,
                  destinationBorough,
                  isTerminalArrival: isTerminalArrival || undefined,
                  source: "realtime",
                  // Add wheelchair and bike info from static data
                  wheelchair_accessible: staticTripInfo?.wheelchair_accessible || null,
                  bikes_allowed: staticTripInfo?.bikes_allowed || null,
                  // Add MNR-specific trainStatus from MTARR extensions
                  trainStatus,
                };
                realtimeDepartures.push(departure);
              }
            } catch (mappingError) {
              // Log the error for debugging purposes
              logger.error(`Error mapping trip update: ${mappingError}`);
            }
          } // End STU loop
        } // End if trip_update has STUs

        // Process vehicle entities to use vehicleTripId (vehicle.id/label) for MNR trip lookups
        // For MNR trips, the vehicle.id/label should be used to find the corresponding trip_id
        if (systemName === "MNR" && vehicle?.trip?.trip_id) {
          const vehicleTripId = vehicle.trip.trip_id.trim();
          if (vehicleTripId) {
            logger.debug(
              `[MNR Vehicle] Found vehicle with trip_id: ${vehicleTripId}`,
            );

            // For MNR, use the vehicle.vehicle.label (vehicleTripId) to lookup trip_id
            if (vehicle.vehicle?.label) {
              const vehicleLabel = vehicle.vehicle.label.trim();
              logger.debug(
                `[MNR Vehicle] trip_id: ${vehicleTripId}, vehicle label: ${vehicleLabel}`,
              );

              // Look up the static trip_id using the vehicle label as the vehicle ID
              const potentialStaticTripId = staticData.vehicleTripsMap?.get(
                vehicleLabel
              ) || staticData.tripsByShortName?.get(vehicleLabel);
              
              if (potentialStaticTripId) {
                logger.info(
                  `[MNR Analysis] Matched: vehicle.label (${vehicleLabel}) to static trip_id (${potentialStaticTripId})`,
                );
                
                // Add to processedRealtimeTripIds to ensure we don't duplicate this trip from static data
                processedRealtimeTripIds.add(vehicleLabel);
              }
            }
          }
        }
      } // End entity loop
    }

    // --- STATIC FALLBACK (Return Scheduled Departures from Static Schedules) ---
    let addedScheduled = 0;

    // Check if we should run fallback logic
    // 1. Always run for LIRR/MNR systems - many trips may only be in static data
    // 2. Run for SUBWAY systems ONLY if no realtime departures were found
    //
    // For duplicate prevention, we rely on processedRealtimeTripIds tracking
    // which ensures we don't add static trips that already exist in realtime data

    if (
      requestedStationInfo.system === "LIRR" ||
      requestedStationInfo.system === "MNR" ||
      (requestedStationInfo.system === "SUBWAY" &&
        realtimeDepartures.length === 0)
    ) {
      logger.debug(
        `[Departures] Checking static fallback for ${requestedStationInfo.system}...`,
      );

      try {
        const systemName = requestedStationInfo.system; // LIRR, MNR, or SUBWAY
        const activeServices = await getActiveServicesForToday(); // Get today's active service IDs
        logger.debug(
          `[Static Fallback ${systemName}] Active Service IDs (${activeServices.size}): ${Array.from(activeServices).join(", ")}`,
        );
        logger.debug(
          `[Static Fallback ${systemName}] Found ${activeServices.size} active services today.`,
        );
        // Iterate through the station's platforms (original IDs)
        for (const platformId of originalChildStopIds) {
          const tripsStoppingAtPlatform =
            staticData.stopTimeLookup?.get(platformId); // Map<tripId, StopTimeInfo>
          if (!tripsStoppingAtPlatform) continue;

          // Iterate through static trips scheduled to stop at this platform
          for (const [
            staticTripId,
            stopTimeInfo,
          ] of tripsStoppingAtPlatform.entries()) {
            // Skip if this trip was ALREADY processed via real-time feed
            // For MNR/LIRR, we need additional normalization to ensure proper matching
            let normalizedStaticTripId = staticTripId;

            if (systemName === "MNR" || systemName === "LIRR") {
              // Apply the same normalization as for realtime IDs
              normalizedStaticTripId = staticTripId.replace(/^0+/, "");

              if (normalizedStaticTripId !== staticTripId) {
                logger.debug(
                  `[Trip ID] Normalized static ${systemName} trip ID: ${staticTripId} -> ${normalizedStaticTripId}`,
                );
              }
            }

            // For MNR, also check if trip_short_name is in processedRealtimeTripIds
            // to avoid duplicates between static and realtime data
            let shouldSkip = processedRealtimeTripIds.has(
              normalizedStaticTripId,
            );

            if (!shouldSkip && systemName === "MNR") {
              // Get the trip info to check its trip_short_name
              const tripWithShortName = staticData.trips.get(staticTripId);
              if (
                tripWithShortName?.trip_short_name &&
                processedRealtimeTripIds.has(tripWithShortName.trip_short_name)
              ) {
                shouldSkip = true;
                logger.debug(
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
              } else {
                logger.silly(
                  `-> SKIP ${staticTripId}: Unknown reason within filter block.`,
                ); // Should not happen
              }
              continue; // Skip this trip
            }

            // Get scheduled time (prioritize departure)
            const scheduledTimeStr =
              stopTimeInfo.scheduledDepartureTime ||
              stopTimeInfo.scheduledArrivalTime;
            if (!scheduledTimeStr) continue;

            // Construct Date object for scheduled time today
            let scheduledTime: Date | null = null;
            try {
              // Handle times > 24:00:00 if necessary (simple parse assumes same day)
              // Example: "25:10:00" -> Need to add a day to todayStr
              let hours = parseInt(scheduledTimeStr.substring(0, 2), 10);
              let parseDateStr = todayStr;
              const adjustedHour = hours % 24;
              let parseTimeStr = scheduledTimeStr;

              if (!isNaN(hours) && hours >= 24) {
                // Time is on the next day
                const nextDay = new Date(now + 24 * 60 * 60 * 1000); // Add 24 hours
                parseDateStr = format(nextDay, "yyyy-MM-dd");
                // Adjust time string for parsing
                parseTimeStr = `${String(adjustedHour).padStart(2, "0")}:${scheduledTimeStr.substring(3)}`; // e.g., "01:10:00"
                logger.debug(
                  `  Adjusted next-day time: ${scheduledTimeStr} -> ${parseDateStr} ${parseTimeStr}`,
                );
              }
              // Use date-fns parse (safer than new Date with just time string)
              scheduledTime = dateParse(
                `${parseDateStr} ${parseTimeStr}`,
                "yyyy-MM-dd HH:mm:ss",
                new Date(),
              );
            } catch (parseError) {
              logger.error(
                `[Static Fallback] Error parsing time ${scheduledTimeStr}:`,
                parseError,
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
              // Log if skipping based on time
              if (relevantTime)
                logger.debug(
                  `-> Skipping scheduled ${staticTripId}, time ${new Date(relevantTime).toISOString()} out of window`,
                );
              continue;
            }

            // --- Create "Scheduled" Departure ---
            try {
              const actualRouteId = tripInfo.route_id;
              const routeMapKey = `${systemName}-${actualRouteId}`;
              const routeInfo = staticData.routes.get(routeMapKey);

              let finalDestination = tripInfo.destinationStopId
                ? staticData.stops.get(
                    `${systemName}-${tripInfo.destinationStopId}`,
                  )?.name
                : null;
              if (!finalDestination) finalDestination = tripInfo.trip_headsign;
              if (!finalDestination)
                finalDestination =
                  routeInfo?.route_short_name || "Unknown Destination";

              let direction: Direction = "Unknown";

              // Enhanced debugging for MNR direction_id issues
              if (systemName === "MNR") {
                logger.debug(
                  `[MNR Static Direction] Trip ${staticTripId} has direction_id=${tripInfo?.direction_id}, type=${typeof tripInfo?.direction_id}, stringified=${JSON.stringify(tripInfo?.direction_id)}`,
                );

                // Check if direction_id is getting parsed correctly
                if (
                  tripInfo?.direction_id !== undefined &&
                  tripInfo?.direction_id !== null
                ) {
                  const rawDirId = tripInfo.direction_id;
                  // Parse to string then back to number to ensure it's a clean numeric value
                  const parsedDirId = Number(String(rawDirId));
                  logger.debug(
                    `[MNR Static Direction] Trip ${staticTripId} direction_id=${rawDirId}, parsed=${parsedDirId}, equals 0: ${parsedDirId === 0}, equals 1: ${parsedDirId === 1}`,
                  );

                  if (parsedDirId === 1) {
                    direction = "Outbound";
                    logger.debug(
                      `[MNR Static Direction] Setting direction to Outbound for trip ${staticTripId}`,
                    );
                  } else if (parsedDirId === 0) {
                    direction = "Inbound";
                    logger.debug(
                      `[MNR Static Direction] Setting direction to Inbound for trip ${staticTripId}`,
                    );
                  } else {
                    logger.warn(
                      `[MNR Static Direction] Unexpected direction_id value: ${parsedDirId} for trip ${staticTripId}`,
                    );
                  }
                } else {
                  logger.warn(
                    `[MNR Static Direction] Missing direction_id for trip ${staticTripId}`,
                  );
                }
              } else {
                // Standard behavior for non-MNR trips
                if (tripInfo?.direction_id === 0) direction = "Outbound";
                else if (tripInfo?.direction_id === 1) direction = "Inbound";
              }

              // --- Determine Peak Status using tripInfo ---
              const peakStatus = getPeakStatus(tripInfo?.peak_offpeak);
              // ---
              //

              // For MNR static trips, include trip_short_name (which corresponds to vehicle.label)
              const effectiveTripId = systemName === "MNR" && tripInfo.trip_short_name 
                ? tripInfo.trip_short_name  // Use trip_short_name as the displayed tripId for MNR
                : staticTripId;             // Use static ID for other systems
                
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
                  staticData.stops.get(
                    `${systemName}-${tripInfo.destinationStopId}`,
                  )?.borough || null,
                system: systemName,
                // Determine if this is a terminal arrival for MNR at Grand Central
                isTerminalArrival:
                  systemName === "MNR" &&
                  (platformId === "1" || tripInfo.direction_id === 1)
                    ? true
                    : false,
                source: "scheduled",
                // Include accessibility information from static trip data
                wheelchair_accessible: tripInfo.wheelchair_accessible || null,
                bikes_allowed: tripInfo.bikes_allowed || null,
              };
              scheduledDepartures.push(departure);
              addedScheduled++;
            } catch (mapError) {
              logger.error(
                `[Static Fallback] Error mapping scheduled trip ${staticTripId}:`,
                mapError,
              );
            }
            // --- End Create Scheduled ---
          } // End loop tripsStoppingHere
        } // End loop platformId
        logger.debug(
          `[Departures] Added ${addedScheduled} scheduled departures via static fallback for ${systemName}.`,
        );
      } catch (fallbackError) {
        logger.error(
          `[Departures] Error during static fallback for ${requestedStationInfo.system}:`,
          fallbackError,
        );
      }
    }
    // --- END STATIC FALLBACK ---

    // Filter departures based on sourceFilter if provided
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

    // Combine and Sort Real-time + Scheduled
    const combinedDepartures = [...filteredRealtime, ...filteredScheduled];
    totalDeparturesCreated = combinedDepartures.length; // Final count

    logger.debug(
      `[Departures] Combined ${filteredRealtime.length} realtime and ${filteredScheduled.length} scheduled departures (source filter: ${sourceFilter || "none"})`,
    );

    // Final Sort (Direction then Time)
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
    logger.error("[Departures] Error processing departures:", err);
    return [];
  }
}
