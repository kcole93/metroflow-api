import { loadProtobufDefinitions } from "../utils/protobufLoader";
import { getCache, setCache, clearCacheKey } from "./cacheService";
import { getStaticData } from "./staticDataService";
import { getActiveServicesForToday } from "./calendarService"; // Add this import
import { parse as dateParse, format } from "date-fns"; // Add date-fns imports
import {
  Station,
  Departure,
  ServiceAlert,
  StaticRouteInfo,
  StaticStopInfo,
  Direction,
  SystemType,
  StaticData,
  PeakStatus,
} from "../types";
import * as dotenv from "dotenv";

dotenv.config();

// --- Feed URL Constants ---
const MTA_API_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds";

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

export const ALERT_FEEDS = {
  SUBWAY: `${MTA_API_BASE}/camsys%2Fsubway-alerts`,
  LIRR: `${MTA_API_BASE}/camsys%2Flirr-alerts`,
  MNR: `${MTA_API_BASE}/camsys%2Fmnr-alerts`,
  ALL: `${MTA_API_BASE}/camsys%2Fall-alerts`,
};
// --- End Constants ---

// --- Route ID to Feed URL Map ---
export const ROUTE_ID_TO_FEED_MAP: { [key: string]: string } = {
  "SUBWAY-A": SUBWAY_FEEDS.ACE,
  "SUBWAY-C": SUBWAY_FEEDS.ACE,
  "SUBWAY-E": SUBWAY_FEEDS.ACE,
  "SUBWAY-B": SUBWAY_FEEDS.BDFM,
  "SUBWAY-D": SUBWAY_FEEDS.BDFM,
  "SUBWAY-F": SUBWAY_FEEDS.BDFM,
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
  "SUBWAY-SI": SUBWAY_FEEDS.SI,
  "SUBWAY-7": SUBWAY_FEEDS.NUMERIC,
  "SUBWAY-7X": SUBWAY_FEEDS.NUMERIC,
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

// --- fetchAndParseFeed Function ---
async function fetchAndParseFeed(
  url: string,
  feedName: string,
): Promise<{ message: any; feedObject: any } | null> {
  const cacheKey = `feed_${feedName}_${url.replace(/[^a-zA-Z0-9]/g, "_")}`;
  const cachedData = getCache<{ message: any; feedObject: any }>(cacheKey);

  if (cachedData) {
    const messageIsEmpty =
      !cachedData.message ||
      !cachedData.message.entity ||
      cachedData.message.entity.length === 0;
    const objectIsEmpty =
      !cachedData.feedObject ||
      !cachedData.feedObject.entity ||
      cachedData.feedObject.entity.length === 0;
    if (messageIsEmpty && objectIsEmpty) {
      console.warn(
        `[Fetch] Cache hit for ${feedName} but data seems empty. Bypassing cache once.`,
      );
      clearCacheKey(cacheKey);
    } else {
      return cachedData;
    }
  }

  try {
    const FeedMessage = await loadProtobufDefinitions();
    const fetchOptions = {
      signal: AbortSignal.timeout(25000),
    };

    const response = await fetch(url, fetchOptions);
    if (!response.ok) {
      const errorBody = await response
        .text()
        .catch(() => "Could not read error body");
      console.error(
        `[Fetch] API Request Error Status ${
          response.status
        } for ${feedName}: ${errorBody.slice(0, 1000)}`,
      );
      return null;
    }

    const contentType = response.headers.get("content-type");
    if (contentType?.includes("html") || contentType?.includes("json")) {
      console.error(
        `[Fetch] Received likely error page Content-Type for ${feedName}: ${contentType}.`,
      );
      const textBody = await response.text().catch(() => "Could not read body");
      console.error(
        `[Fetch] Error page body for ${feedName}: ${textBody.slice(0, 500)}...`,
      );
      return null;
    } else if (
      !contentType?.includes("octet-stream") &&
      !contentType?.includes("protobuf")
    ) {
      // console.warn( // Reduce noise
      //     `[Fetch] Non-standard Content-Type for ${feedName}: ${contentType}. Attempting to parse anyway.`
      // );
    }

    const buffer = await response.arrayBuffer();
    if (buffer.byteLength === 0) {
      console.warn(
        `[Fetch] Received EMPTY buffer for feed ${feedName} (${url}).`,
      );
      return null;
    }

    let message: any = null;
    try {
      message = FeedMessage.decode(new Uint8Array(buffer));
      if (!message || !message.entity || !Array.isArray(message.entity)) {
        console.warn(
          `[Fetch] Decoded message for ${feedName} is missing 'entity' field, not an array, or message is null.`,
        );
        return null;
      }
      // console.log(`[Fetch] Decoded ${message.entity.length} entities for ${feedName}.`); // Reduce noise
    } catch (decodeError) {
      console.error(
        `[Fetch] Protobuf DECODING FAILED for ${feedName}:`,
        decodeError,
      );
      return null;
    }

    let feedObject: any = { entity: [], header: {} };
    try {
      if (message && message.entity && Array.isArray(message.entity)) {
        feedObject = FeedMessage.toObject(message, {
          longs: String,
          enums: String,
          bytes: String,
          arrays: true,
          objects: true,
          oneofs: true,
        });
      }
    } catch (toObjectError) {
      // console.warn(`[Fetch] FeedMessage.toObject failed for ${feedName}. Error:`, toObjectError); // Reduce noise
    }

    const result = { message, feedObject };
    const cacheUrl = url.toLowerCase();
    setCache(cacheKey, result);
    return result;
  } catch (error: any) {
    console.error(`[Fetch] Error for ${feedName} (${url}):`, error);
    if (error.name === "AbortError") {
      console.error(`[Fetch] Request TIMED OUT for ${feedName}`);
    }
    return null;
  }
}

// --- getStations Function ---
export async function getStations(
  query?: string,
  systemFilter?: StaticStopInfo["system"],
): Promise<Station[]> {
  let staticData: StaticData;
  try {
    staticData = getStaticData(); // Must be loaded successfully first
  } catch (err) {
    console.error("[Departures] Static data not available:", err);
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
    // Assuming '1' means Peak (VERIFY THIS!)
    return "Peak";
  } else if (peakOffpeakValue === "0") {
    // Assuming '0' means Off-Peak
    return "Off-Peak";
  }
  return null; // Return null if missing or other value
}
// ---

// --- getDeparturesForStation Function (UPDATED with Static Fallback) ---
export async function getDeparturesForStation(
  requestedUniqueStationId: string, // e.g., "SUBWAY-L11" or "LIRR-237"
  limitMinutes?: number,
): Promise<Departure[]> {
  let staticData: StaticData;
  try {
    staticData = getStaticData(); // Must be loaded successfully first
  } catch (err) {
    console.error("[Departures] Static data not available:", err);
    return [];
  }

  const requestedStationInfo = staticData.stops.get(requestedUniqueStationId); // Lookup by unique key
  const requestedStationName = requestedStationInfo?.name; // Store for subsequent comparison

  if (!requestedStationInfo) {
    console.warn(
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
      console.log(
        `[Departures] No child stops for ${requestedUniqueStationId}, using own original ID: ${requestedStationInfo.originalStopId}.`,
      );
      originalChildStopIds.add(requestedStationInfo.originalStopId);
    } else {
      console.warn(
        `[Departures] No child stops AND no original_stop_id for ${requestedUniqueStationId}. Cannot process.`,
      );
      return [];
    }
  }

  const feedUrlsToFetch = Array.from(requestedStationInfo.feedUrls);
  // Note: For LIRR/MNR, feedUrlsToFetch might be empty but we'll still run static fallback
  // if there are no feeds to fetch

  console.log(
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

  // --- Process Real-time Data ---
  try {
    const feedFetchResults = await Promise.all(feedPromises);

    console.log(
      `[Departures] Processing results for ${feedFetchResults.length} fetched feeds...`,
    );
    for (const { url: feedUrl, result: fetchedData } of feedFetchResults) {
      if (!fetchedData?.message?.entity) {
        console.warn(` -> Skipping feed ${feedUrl}: No valid data returned.`);
        continue; // Skip empty/failed feeds
      }

      const decodedEntities = fetchedData.message.entity;
      totalEntitiesFetched += decodedEntities.length;

      // Determine System for this specific feed's data
      let systemName: SystemType | null = null; // Use your actual SystemType
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
        console.warn(
          ` -> Could not determine system for ${feedUrl}. Skipping.`,
        );
        continue;
      }
      // console.log(`    -> Processing feed for System: ${systemName}`); // Reduce noise

      for (const entity of decodedEntities) {
        const trip_update = entity.trip_update; // Use snake_case

        if (trip_update?.stop_time_update?.length > 0) {
          totalUpdatesProcessed++;
          const stopTimeUpdates = trip_update.stop_time_update;
          const rtTrip = trip_update.trip; // Real-time TripDescriptor
          const tripIdFromFeed = rtTrip?.trip_id?.trim(); // Use RT feed trip ID

          // Track processed trip IDs for static fallback
          if (tripIdFromFeed) {
            processedRealtimeTripIds.add(tripIdFromFeed);
          }

          // --- Determine Destination using Last Stop in stopTimeUpdates ---
          let calculatedDestination = "Unknown Destination";
          let destinationBorough: string | null = null;
          let destSource = "Default";
          if (stopTimeUpdates.length > 0) {
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
            const lastStopId = lastStopUpdate?.stop_id?.trim(); // Trim just in case
            if (lastStopId && systemName) {
              // Use the systemName determined for this feed!
              // *** Construct the UNIQUE key for the destination stop ***
              const destStopKey = `${systemName}-${lastStopId}`;
              const destStopInfo = staticData.stops.get(destStopKey); // Lookup using unique key

              if (destStopInfo) {
                calculatedDestination = destStopInfo.name; // Use name from CORRECT stop object
                destSource = "Last Stop Name";
                // --- Add Destination Borough ---
                destinationBorough = destStopInfo.borough || null;
                console.log(
                  `      [Dest Calc] Using last stop: ${lastStopId} (Key: ${destStopKey}) -> Name: ${calculatedDestination}`,
                ); // Log lookup success
              } else {
                console.log(
                  `      [Dest Calc] Failed lookup for destination stop key: ${destStopKey}`,
                ); // Log lookup failure
              }
            }
          }
          // --- End Destination (Initial Calculation) ---

          for (const stu of stopTimeUpdates) {
            const stuOriginalStopId = stu.stop_id?.trim(); // Get the ID from the feed STU
            if (
              !stuOriginalStopId ||
              !originalChildStopIds.has(stuOriginalStopId)
            )
              continue;

            // Check if this original stop ID is one we care about for the requested station
            if (originalChildStopIds.has(stuOriginalStopId)) {
              // Time calculation and filtering
              const departureTimeLong = stu.departure?.time;
              const arrivalTimeLong = stu.arrival?.time;
              const primaryTimeLong =
                departureTimeLong && Number(departureTimeLong) > 0
                  ? departureTimeLong
                  : arrivalTimeLong;
              const relevantTime = primaryTimeLong
                ? Number(primaryTimeLong) * 1000
                : null;
              const isTimeValid =
                relevantTime !== null &&
                relevantTime >= now - 60000 &&
                relevantTime <= cutoffTime;
              // Check if there is a realtime prediction
              const hasRealtimePrediction =
                (departureTimeLong && Number(departureTimeLong)) > 0 ||
                (arrivalTimeLong && Number(arrivalTimeLong)) > 0;

              // Log occasionally for matched stops
              const timeDiffMinutes = relevantTime
                ? Math.round((relevantTime - now) / 60000)
                : null;
              console.log(
                `    ✅ [STU Match Original] Matched STU ID: ${stuOriginalStopId}. Time Valid? ${isTimeValid}`,
              );

              if (!isTimeValid) {
                continue;
              } // Skip if time invalid/past/cutoff

              // --- START: Create Departure Object Logic ---
              try {
                // --- Route Info Lookup ---
                const actualRouteId = rtTrip?.route_id?.trim();
                let routeInfo: StaticRouteInfo | undefined | null = null;
                if (actualRouteId && systemName) {
                  const routeMapKey = `${systemName}-${actualRouteId}`;
                  routeInfo = staticData.routes.get(routeMapKey);
                  // Optional failure logging
                  // if (!routeInfo) console.warn(`     [Route Lookup] Failed for key: "${routeMapKey}"`);
                }
                // --- End Route Info Lookup ---
                //

                // --- Final Destination Fallback ---
                let finalDestination = calculatedDestination; // Start with value from Last Stop logic
                const tripInfo = tripIdFromFeed
                  ? staticData.trips.get(tripIdFromFeed)
                  : null; // Optional lookup for headsign fallback
                if (
                  finalDestination === "Unknown Destination" &&
                  tripInfo?.trip_headsign
                ) {
                  finalDestination = tripInfo.trip_headsign;
                  destSource = "Trip Headsign Fallback"; // Update source if used
                }
                if (
                  finalDestination === "Unknown Destination" &&
                  routeInfo?.route_long_name
                ) {
                  finalDestination = routeInfo.route_long_name;
                  destSource = "Route Name Fallback"; // Update source if used
                }
                // --- End Final Destination ---

                // --- Determine Direction ---
                let direction: Direction = "Unknown"; // Default to Unknown
                if (systemName === "SUBWAY") {
                  const nyctTripExt =
                    rtTrip?.[".transit_realtime.nyct_trip_descriptor"]; // Access nested extension
                  console.log(
                    `      [Direction] NYCT Trip Descriptor: ${JSON.stringify(
                      nyctTripExt,
                    )}`,
                  );
                  if (systemName === "SUBWAY" && nyctTripExt?.direction) {
                    // Use the documented NYCT direction if available for SUBWAY
                    if (nyctTripExt.direction === 1) {
                      // 1 = NORTH in gtfs-realtime-NYCT.proto
                      direction = "N";
                    } else if (nyctTripExt.direction === 3) {
                      // 3 = SOUTH in Protobuff Enum
                      direction = "S";
                    }
                    console.log(
                      `      [Direction] Using NYCT Trip Descriptor Direction: ${nyctTripExt.direction} -> ${direction}`,
                    ); // Log source
                  }
                } else if (systemName === "LIRR" || systemName === "MNR") {
                  // Use static tripInfo.direction_id for LIRR/MNR
                  if (tripInfo?.direction_id != null) {
                    // Check if static trip info was found and has direction
                    const dirId = tripInfo.direction_id; // Should be 0 or 1
                    if (dirId === 0) direction = "W";
                    else if (dirId === 1) direction = "E";
                    else direction = "Unknown";
                    console.log(
                      `      [Direction Static Check ${systemName}] Found direction_id: ${dirId} = ${direction}for trip ${tripIdFromFeed}`,
                    );
                  } else {
                    console.log(
                      `      [Direction Static Check ${systemName}] Static tripInfo or direction_id not found for trip ${tripIdFromFeed}`,
                    );
                  }
                }

                // Fallback to Stop ID Suffix if NYCT direction wasn't found/used
                if (direction === "Unknown") {
                  const lastChar = stuOriginalStopId
                    .charAt(stuOriginalStopId.length - 1)
                    .toUpperCase();
                  if (lastChar === "N") direction = "N";
                  else if (lastChar === "S") direction = "S";
                  else if (lastChar === "E")
                    direction = "E"; // Keep E/W for LIRR/MNR
                  else if (lastChar === "W") direction = "W";
                  if (direction !== "Unknown") {
                    console.log(
                      `      [Direction] Using Stop ID Suffix Fallback: ${lastChar} -> ${direction}`,
                    ); // Log source
                  }
                }
                // If still Unknown, it remains Unknown
                // --- End Direction ---

                // --- Track ---
                const nyctExtension =
                  stu[".transit_realtime.nyct_stop_time_update"];
                const mtarrExtension =
                  stu[".transit_realtime.mta_railroad_stop_time_update"];
                let track =
                  nyctExtension?.actualTrack ||
                  mtarrExtension?.track ||
                  stu.departure?.track ||
                  stu.arrival?.track ||
                  undefined;
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
                    if (delayMinutes > 1)
                      status = `Delayed ${delayMinutes} min`;
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
                }
                if (relevantTime === null && !hasRealtimePrediction) {
                  console.log(
                    `    [STU Skip] Matched ${stuOriginalStopId}, but NO real-time prediction available in STU.`,
                  );
                  continue; // Skip if no time available at all
                }

                // If we somehow have hasRealtimePrediction=false but relevantTime!=null (shouldn't happen), log warning
                if (!hasRealtimePrediction && relevantTime !== null) {
                  console.warn(
                    `    [Logic Warn] hasRealtimePrediction is false but relevantTime has value for ${stuOriginalStopId}`,
                  );
                }
                // --- End Status ---

                // --- Determine Peak Status using tripInfo ---
                const peakStatus = getPeakStatus(tripInfo?.peak_offpeak);
                // ---

                // --- Create Departure Object ---
                const departure: Departure = {
                  id: `${tripIdFromFeed}-${stu.stop_id}-${relevantTime}`,
                  tripId: tripIdFromFeed,
                  routeId: actualRouteId,
                  routeShortName: routeInfo?.route_short_name || "",
                  routeLongName: routeInfo?.route_long_name || "",
                  peakStatus: peakStatus,
                  routeColor: routeInfo?.route_color || null,
                  destination: finalDestination,
                  departureTime: relevantTime ? new Date(relevantTime) : null,
                  delayMinutes: delayMinutes,
                  track: track,
                  status: status,
                  direction: direction,
                  system: systemName,
                  destinationBorough,
                };
                realtimeDepartures.push(departure);
                totalDeparturesCreated++;
                // --- End Create ---
              } catch (mappingError) {
                console.error(
                  `      -> [Mapping Error] Failed create Departure for stop ${stuOriginalStopId}, trip ${tripIdFromFeed}:`,
                  mappingError,
                );
              }
              // --- END: Create Departure Object Logic ---
            } // End if originalChildStopIds.has
          } // End loop stu
        } // End if trip_update check
      } // End loop entity
    } // End loop feedFetchResults
  } catch (error) {
    console.error(
      `[Departures] Error processing fetched feeds for ${requestedUniqueStationId}:`,
      error,
    );
    // Continue to static fallback instead of returning empty
  }

  // --- STATIC FALLBACK for LIRR/MNR ---
  const scheduledDepartures: Departure[] = [];
  let addedScheduled = 0;

  // Only run fallback for LIRR/MNR systems
  if (
    requestedStationInfo.system === "LIRR" ||
    requestedStationInfo.system === "MNR"
  ) {
    console.log(
      `[Departures] Checking static fallback for ${requestedStationInfo.system}...`,
    );

    try {
      const systemName = requestedStationInfo.system; // LIRR or MNR
      const activeServices = await getActiveServicesForToday(); // Get today's active service IDs
      console.log(
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
          if (processedRealtimeTripIds.has(staticTripId)) continue;

          // Get static trip details
          const tripInfo = staticData.trips.get(staticTripId);
          // Ensure trip exists, matches the system, and has an active service ID
          if (
            !tripInfo ||
            tripInfo.system !== systemName ||
            !tripInfo.service_id ||
            !activeServices.has(tripInfo.service_id)
          ) {
            continue;
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
            let parseTimeStr = scheduledTimeStr;
            if (!isNaN(hours) && hours >= 24) {
              // Time is on the next day
              const nextDay = new Date(now + 24 * 60 * 60 * 1000); // Add 24 hours
              parseDateStr = format(nextDay, "yyyy-MM-dd");
              // Adjust time string for parsing
              parseTimeStr = `${hours % 24}:${scheduledTimeStr.substring(3)}`; // e.g., "01:10:00"
              console.log(
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
            console.warn(
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
            // if (relevantTime) console.log(` -> Skipping scheduled ${staticTripId}, time ${new Date(relevantTime).toISOString()} out of window`);
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
                routeInfo?.route_long_name || "Unknown Destination";

            let direction: Direction = "Unknown";
            if (tripInfo?.direction_id === 0)
              direction = "E"; // Assuming 0=East/Outbound
            else if (tripInfo?.direction_id === 1) direction = "W"; // Assuming 1=West/Inbound

            let track = stopTimeInfo.track || undefined; // Use track from static stop_times

            // --- Determine Peak Status using tripInfo ---
            const peakStatus = getPeakStatus(tripInfo?.peak_offpeak);
            // ---

            const departure: Departure = {
              id: staticTripId,
              tripId: staticTripId, // Use static ID
              routeId: actualRouteId,
              routeShortName: routeInfo?.route_short_name || "",
              routeLongName: routeInfo?.route_long_name || "",
              peakStatus: peakStatus,
              routeColor: routeInfo?.route_color || null,
              destination: finalDestination,
              departureTime: scheduledTime, // Use SCHEDULED Date object
              delayMinutes: null, // No delay info
              track: track,
              status: "Scheduled", // Explicitly set status
              direction: direction,
              destinationBorough:
                staticData.stops.get(
                  `${systemName}-${tripInfo.destinationStopId}`,
                )?.borough || null,
              system: systemName,
            };
            scheduledDepartures.push(departure);
            addedScheduled++;
          } catch (mapError) {
            console.error(
              `[Static Fallback] Error mapping scheduled trip ${staticTripId}:`,
              mapError,
            );
          }
          // --- End Create Scheduled ---
        } // End loop tripsStoppingHere
      } // End loop platformId
      console.log(
        `[Departures] Added ${addedScheduled} scheduled departures via static fallback for ${systemName}.`,
      );
    } catch (fallbackError) {
      console.error(
        `[Departures] Error during static fallback for ${requestedStationInfo.system}:`,
        fallbackError,
      );
    }
  }
  // --- END STATIC FALLBACK ---

  // Combine and Sort Real-time + Scheduled
  const combinedDepartures = [...realtimeDepartures, ...scheduledDepartures];
  totalDeparturesCreated = combinedDepartures.length; // Final count

  console.log(
    `[Departures] Finished Processing. RT Updates: ${totalUpdatesProcessed}. Final Departures (RT+Sched): ${totalDeparturesCreated} for ${requestedUniqueStationId}.`,
  );

  // Final Sort (Direction then Time)
  combinedDepartures.sort((a, b) => {
    const dirOrder: Record<string, number> = {
      Uptown: 1,
      Northbound: 1,
      Downtown: 2,
      Southbound: 2,
      Eastbound: 3,
      Westbound: 4,
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
}

// --- getServiceAlerts Function ---
export async function getServiceAlerts(
  targetLines?: string[], // Optional: Filter by specific line short names
  filterActiveNow = false, // Optional: Only show alerts currently active
): Promise<ServiceAlert[]> {
  const feedUrl = ALERT_FEEDS.ALL;
  const feedName = "all_service_alerts";
  console.log(
    `[Alerts Service] Fetching ${feedName}. Filters: lines=${
      targetLines?.join(",") || "N/A"
    }, activeNow=${filterActiveNow}`,
  );

  const fetchedData = await fetchAndParseFeed(feedUrl, feedName);
  const message = fetchedData?.message;

  let allAlerts: ServiceAlert[] = []; // Start with empty list

  if (!message?.entity?.length) {
    console.warn(
      "[Alerts Service] No entities found in the alert feed message.",
    );
    return allAlerts;
  }

  const staticData = getStaticData(); // Needed for route mapping

  // --- Parse ALL alerts first ---
  for (const entity of message.entity) {
    const alert = entity.alert;
    if (alert) {
      try {
        const affectedLinesShortNames: string[] = []; // Store short names
        if (alert.informed_entity) {
          for (const informed of alert.informed_entity) {
            if (informed.route_id) {
              const alertRouteId = informed.route_id;
              // Try to find route info using potential keys
              let routeInfo: StaticRouteInfo | undefined | null = null;
              let potentialKeySubway = `SUBWAY-${alertRouteId}`;
              let potentialKeyLIRR = `LIRR-${alertRouteId}`;
              let potentialKeyMNR = `MNR-${alertRouteId}`;
              routeInfo =
                (await staticData).routes.get(potentialKeySubway) ||
                (await staticData).routes.get(potentialKeyLIRR) ||
                (await staticData).routes.get(potentialKeyMNR);

              const shortName = routeInfo?.route_short_name?.trim();
              // Use short name if available and not already added
              if (
                shortName &&
                shortName !== "" &&
                !affectedLinesShortNames.includes(shortName)
              ) {
                affectedLinesShortNames.push(shortName);
              } else if (
                !routeInfo &&
                !affectedLinesShortNames.includes(alertRouteId)
              ) {
                // Fallback to raw route ID if lookup failed
                // console.log(`[Alerts] Route info not found for affected route ID: ${alertRouteId}`);
                affectedLinesShortNames.push(alertRouteId);
              }
              // Note: LIRR/MNR might primarily use long names, adapt if needed
            }
            // TODO: Add logic for stopId, agencyId if needed
          }
        }

        const getText = (field: any): string | undefined =>
          field?.translation?.[0]?.text;
        const title = getText(alert.header_text) || "Untitled Alert";
        const description =
          getText(alert.description_text) || "No description.";
        const url = getText(alert.url);
        const startDateEpoch = alert.active_period?.[0]?.start;
        const endDateEpoch = alert.active_period?.[0]?.end;

        // Create the full alert object
        allAlerts.push({
          id: entity.id,
          title: title,
          description: description,
          affectedLines: affectedLinesShortNames.sort(), // Store processed short names
          startDate: startDateEpoch
            ? new Date(Number(startDateEpoch) * 1000)
            : undefined,
          endDate: endDateEpoch
            ? new Date(Number(endDateEpoch) * 1000)
            : undefined,
          url: url,
        });
      } catch (alertError) {
        /* ... error handling ... */
      }
    } // end if(alert)
  } // end for loop
  console.log(
    `[Alerts Service] Parsed ${allAlerts.length} total alerts from feed.`,
  );

  // --- Apply Filters ---
  let filteredAlerts = allAlerts;

  // 1. Filter by Active Period
  if (filterActiveNow) {
    const now = Date.now();
    filteredAlerts = filteredAlerts.filter((alert) => {
      const start = alert.startDate?.getTime();
      const end = alert.endDate?.getTime();
      // Include if: No start date OR start date is in the past/now
      const started = !start || start <= now;
      // Include if: No end date OR end date is in the future/now
      const notEnded = !end || end >= now;
      return started && notEnded;
    });
    console.log(
      `[Alerts Service] Filtered to ${filteredAlerts.length} active alerts.`,
    );
  }

  // 2. Filter by Target Lines (case-insensitive comparison recommended)
  if (targetLines && targetLines.length > 0) {
    // Normalize targetLines from query param (already done in route handler if needed)
    // Ensure case matches how affectedLinesShortNames were stored (e.g., both uppercase)
    const targetLinesUpper = targetLines.map((l) => l.toUpperCase()); // Example normalization

    filteredAlerts = filteredAlerts.filter((alert) => {
      // Check if ANY of the alert's affected lines match ANY of the target lines
      return alert.affectedLines.some(
        (alertLine) => targetLinesUpper.includes(alertLine.toUpperCase()), // Case-insensitive check
      );
    });
    console.log(
      `[Alerts Service] Filtered to ${
        filteredAlerts.length
      } alerts affecting lines: [${targetLines.join(", ")}]`,
    );
  }
  // --- End Apply Filters ---

  // Sort the *filtered* results (e.g., by start date descending)
  filteredAlerts.sort(
    (a, b) => (b.startDate?.getTime() ?? 0) - (a.startDate?.getTime() ?? 0),
  );

  return filteredAlerts;
}
