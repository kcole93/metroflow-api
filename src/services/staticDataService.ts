// src/services/staticDataService.ts
import { logger } from "../utils/logger";
import { parseCsvFile } from "../utils/csvParser";
import * as path from "path";
import {
  StaticData,
  StaticStopInfo,
  StaticRouteInfo,
  StaticTripInfo,
  StaticStopTimeInfo,
  SystemType,
} from "../types";
import * as dotenv from "dotenv";
import { getBoroughForCoordinates } from "./geoService";
import { ROUTE_ID_TO_FEED_MAP } from "../services/mtaService";

dotenv.config();

// Initialize with empty state or types asserting it might be null initially
let currentStaticData: StaticData | null = null;
let staticData: StaticData | null = null;
const BASE_DATA_PATH =
  process.env.STATIC_DATA_PATH || "./src/assets/gtfs-static";

// Base interface for raw stop time data from CSV
interface StopTimeBase {
  trip_id: string;
  arrival_time?: string;
  departure_time?: string;
  stop_id: string;
  stop_sequence: string;
  track?: string;
}

function addRouteToMap(
  r: any,
  system: SystemType,
  map: Map<string, StaticRouteInfo>, // Use the passed map
) {
  const routeId = r.route_id?.trim();
  if (!routeId) return;
  const uniqueKey = `${system}-${routeId}`;
  map.set(uniqueKey, {
    // Use the passed 'map' variable
    route_id: routeId,
    agency_id: r.agency_id?.trim() || undefined,
    route_short_name: r.route_short_name?.trim() || "",
    route_long_name: r.route_long_name?.trim() || "",
    route_type: r.route_type ? parseInt(r.route_type, 10) : undefined,
    route_color: r.route_color,
    route_text_color: r.route_text_color?.trim() || null,
    system: system,
  });
}

function addTripToMap(
  t: any,
  system: SystemType,
  map: Map<string, StaticTripInfo>, // Use the passed map
  destinations: Map<string, string>,
) {
  const tripId = t.trip_id?.trim();
  const routeId = t.route_id?.trim();
  if (!tripId || !routeId) return;
  let directionIdNum: number | null = null;
  const dirIdStr = t.direction_id;
  if (dirIdStr != null && dirIdStr !== "") {
    const p = parseInt(dirIdStr, 10);
    if (!isNaN(p)) directionIdNum = p;
  }
  const destStopId = destinations.get(tripId) || null;
  map.set(tripId, {
    // Use the passed 'map' variable
    trip_id: tripId,
    route_id: routeId,
    service_id: t.service_id?.trim() || "",
    trip_headsign: t.trip_headsign?.trim() || undefined,
    trip_short_name: t.trip_short_name?.trim() || undefined,
    peak_offpeak: t.peak_offpeak?.trim() || null,
    direction_id: directionIdNum,
    block_id: t.block_id?.trim() || undefined,
    shape_id: t.shape_id?.trim() || undefined,
    system: system,
    destinationStopId: destStopId,
  });
}

function processStop(
  rawStop: any,
  system: SystemType,
  map: Map<string, StaticStopInfo>, // Use the passed map
) {
  const originalStopId = rawStop.stop_id?.trim();
  if (!originalStopId) return;
  const uniqueStopKey = `${system}-${originalStopId}`;
  const latitude =
    typeof rawStop.stop_lat === "string"
      ? parseFloat(rawStop.stop_lat)
      : rawStop.stop_lat;
  const longitude =
    typeof rawStop.stop_lon === "string"
      ? parseFloat(rawStop.stop_lon)
      : rawStop.stop_lon;
  let locationTypeNum: number | null = null;
  const locStr = rawStop.location_type;
  if (typeof locStr === "string" && locStr.trim() !== "") {
    const p = parseInt(locStr, 10);
    if (!isNaN(p)) locationTypeNum = p;
  }
  const parentStationFileId = rawStop.parent_station?.trim() || null;
  const uniqueParentKey = parentStationFileId
    ? `${system}-${parentStationFileId}`
    : null;
  const borough = getBoroughForCoordinates(
    !isNaN(latitude) ? latitude : undefined,
    !isNaN(longitude) ? longitude : undefined,
  );

  // Check the passed 'map' before setting
  if (!map.has(uniqueStopKey)) {
    map.set(uniqueStopKey, {
      // Use the passed 'map' variable
      id: uniqueStopKey,
      originalStopId: originalStopId,
      name: rawStop.stop_name || "Unnamed Stop",
      latitude: !isNaN(latitude) ? latitude : undefined,
      longitude: !isNaN(longitude) ? longitude : undefined,
      parentStationId: uniqueParentKey,
      locationType: locationTypeNum,
      childStopIds: new Set<string>(),
      servedByRouteIds: new Set<string>(),
      feedUrls: new Set<string>(),
      system: system,
      borough: borough,
    });
  }
}

// --- Main Static Data Loading Function ---
export async function loadStaticData(): Promise<void> {
  logger.info("Starting to load/reload static GTFS data...");
  const startTime = Date.now();

  // Define system paths to locate static data files
  const systems = [
    { name: "LIRR" as SystemType, path: path.join(BASE_DATA_PATH, "LIRR") },
    { name: "SUBWAY" as SystemType, path: path.join(BASE_DATA_PATH, "NYCT") },
    { name: "MNR" as SystemType, path: path.join(BASE_DATA_PATH, "MNR") },
  ];

  // --- Load data into temporary vars for the load attempt
  const tempLoadedRoutes = new Map<string, StaticRouteInfo>();
  const tempLoadedStops = new Map<string, StaticStopInfo>();
  const tempLoadedTrips = new Map<string, StaticTripInfo>();
  const tempLoadedStopTimeLookup = new Map<
    string,
    Map<string, StaticStopTimeInfo>
  >();

  try {
    // --- 1. Load All Raw Files ---
    logger.info("Phase 1: Loading raw CSV files...");
    const promises = systems.flatMap((sys) => [
      parseCsvFile<any>(path.join(sys.path, "stops.txt"), logger),
      parseCsvFile<any>(path.join(sys.path, "routes.txt"), logger),
      parseCsvFile<any>(path.join(sys.path, "trips.txt"), logger),
      parseCsvFile<StopTimeBase>(path.join(sys.path, "stop_times.txt"), logger),
    ]);
    const results = await Promise.all(promises);
    const [
      lirrStopsRaw,
      lirrRoutesRaw,
      lirrTripsRaw,
      lirrStopTimesRaw,
      subwayStopsRaw,
      subwayRoutesRaw,
      subwayTripsRaw,
      subwayStopTimesRaw,
      mnrStopsRaw,
      mnrRoutesRaw,
      mnrTripsRaw,
      mnrStopTimesRaw,
    ] = results;
    const allStopTimesRaw = [
      ...lirrStopTimesRaw,
      ...subwayStopTimesRaw,
      ...mnrStopTimesRaw,
    ];
    const allTripsRaw = [...lirrTripsRaw, ...subwayTripsRaw, ...mnrTripsRaw];
    logger.info("Phase 1 finished.");

    // --- 2. Build tempRoutes Map (Key: SYSTEM-ROUTEID), populating tempLoadedRoutes map ---
    logger.info("Phase 2: Building routes map...");

    lirrRoutesRaw.forEach((r) => addRouteToMap(r, "LIRR", tempLoadedRoutes));
    subwayRoutesRaw.forEach((r) =>
      addRouteToMap(r, "SUBWAY", tempLoadedRoutes),
    );
    mnrRoutesRaw.forEach((r) => addRouteToMap(r, "MNR", tempLoadedRoutes));
    logger.info(`Phase 2 finished. Routes map size: ${tempLoadedRoutes.size}`);

    // --- 3. Process stop_times to find Trip Destinations (Key: raw trip_id) ---
    logger.info(
      "Pass 3: Processing stop_times to determine trip destinations...",
    );
    const tripDestinations = new Map<string, string>(); // Map: trip_id -> last_stop_id
    const tripMaxSequence = new Map<string, number>();

    const findDestinations = (stopTimes: StopTimeBase[]) => {
      for (const st of stopTimes) {
        const tripId = st.trip_id?.trim();
        if (!tripId || st.stop_sequence == null) continue;
        const stopSequence = parseInt(st.stop_sequence, 10);
        if (!isNaN(stopSequence)) {
          const currentMax = tripMaxSequence.get(tripId) ?? -1;
          if (stopSequence > currentMax) {
            tripMaxSequence.set(tripId, stopSequence);
            tripDestinations.set(tripId, st.stop_id?.trim() || "");
          }
        }
      }
    };
    findDestinations(allStopTimesRaw);
    logger.info(
      `Pass 3 finished. Found destinations for ${tripDestinations.size} trips.`,
    );

    // --- 4. Build FINAL tempTrips map (Key: raw trip_id) ---
    logger.info("Pass 4: Building final tempTrips map...");
    lirrTripsRaw.forEach((t) =>
      addTripToMap(t, "LIRR", tempLoadedTrips, tripDestinations),
    );
    subwayTripsRaw.forEach((t) =>
      addTripToMap(t, "SUBWAY", tempLoadedTrips, tripDestinations),
    );
    mnrTripsRaw.forEach((t) =>
      addTripToMap(t, "MNR", tempLoadedTrips, tripDestinations),
    );
    logger.info(
      `Pass 4 finished. Final tempTrips map size: ${tempLoadedTrips.size}`,
    );

    // --- 5. Enrich static data ---
    logger.info(
      "Pass 5: Processing raw stops into enriched map + geofencing...",
    );
    let stopsGeofencedCount = 0;
    lirrStopsRaw.forEach((s) => processStop(s, "LIRR", tempLoadedStops));
    subwayStopsRaw.forEach((s) => processStop(s, "SUBWAY", tempLoadedStops));
    mnrStopsRaw.forEach((s) => processStop(s, "MNR", tempLoadedStops));
    logger.info(
      `Pass 5 finished. enrichedStops size: ${tempLoadedStops.size}. Geofenced: ${stopsGeofencedCount}`,
    );

    // --- 6. Link children to parents (using unique keys) ---
    logger.info("Pass 6: Linking child stops to parent stations...");
    let linkedChildrenCount = 0;
    for (const [childKey, stopInfo] of tempLoadedStops.entries()) {
      if (stopInfo.parentStationId) {
        // parentStationId is unique key "SYSTEM-ID"
        const parentStopInfo = tempLoadedStops.get(stopInfo.parentStationId);
        if (parentStopInfo) {
          parentStopInfo.childStopIds.add(stopInfo.originalStopId); // Add ORIGINAL child ID
          linkedChildrenCount++;
        }
      }
    }
    logger.info(`Pass 6 finished. Linked ${linkedChildrenCount} children.`);

    // --- 7. Build StopTime Lookup Map (Key: original_stop_id -> trip_id -> info) ---
    logger.info("Pass 7: Building stopTimeLookup map...");
    for (const st of allStopTimesRaw) {
      const stopId = st.stop_id?.trim();
      const tripId = st.trip_id?.trim(); // Use raw tripId from stop_times
      const stopSequenceStr = st.stop_sequence;
      if (!stopId || !tripId || stopSequenceStr == null) continue;
      const stopSequence = parseInt(stopSequenceStr, 10);
      if (isNaN(stopSequence)) continue;

      if (!tempLoadedStopTimeLookup.has(stopId)) {
        tempLoadedStopTimeLookup.set(
          stopId,
          new Map<string, StaticStopTimeInfo>(),
        );
      }
      tempLoadedStopTimeLookup.get(stopId)?.set(tripId, {
        scheduledArrivalTime: st.arrival_time?.trim() || null,
        scheduledDepartureTime: st.departure_time?.trim() || null,
        stopSequence: stopSequence,
        track: st.track?.trim() || null,
      });
    }
    logger.info(
      `Pass 7 finished. Built stopTimeLookup map for ${tempLoadedStopTimeLookup.size} stops.`,
    );

    // --- 8. Process stop_times to link routes/feeds ---
    logger.info("Pass 8: Processing stop times to link routes/feeds...");
    const processStopTimesForFeeds = (stopTimes: StopTimeBase[]) => {
      let linksMade = 0;
      for (const st of stopTimes) {
        const stopTimeTripId = st.trip_id?.trim();
        if (!stopTimeTripId) continue;
        const trip = tempLoadedTrips.get(stopTimeTripId); // Lookup trip by raw ID
        if (!trip?.route_id || !trip.system) continue; // Need system from trip

        const routeMapKey = `${trip.system}-${trip.route_id}`; // Use trip's system + route_id
        const route = tempLoadedRoutes.get(routeMapKey);
        if (!route) continue; // Skip if route lookup fails

        const originalStopId = st.stop_id?.trim();
        if (!originalStopId) continue;
        const childStopKey = `${trip.system}-${originalStopId}`; // Use trip's system + ST stop_id
        const childStopInfo = tempLoadedStops.get(childStopKey);
        if (!childStopInfo) continue;

        const routeId = route.route_id;
        const routeSystem = route.system;
        const feedKey = `${routeSystem}-${routeId}`;
        const feedUrl = ROUTE_ID_TO_FEED_MAP[feedKey];

        if (feedUrl) {
          let addedLink = false;
          // Add using UNIQUE stop keys
          if (!childStopInfo.feedUrls.has(feedUrl)) {
            childStopInfo.feedUrls.add(feedUrl);
            addedLink = true;
          }
          if (!childStopInfo.servedByRouteIds.has(routeId)) {
            childStopInfo.servedByRouteIds.add(routeId);
            addedLink = true;
          }
          if (childStopInfo.parentStationId) {
            // parent ID is unique key
            const parentStopInfo = tempLoadedStops.get(
              childStopInfo.parentStationId,
            );
            if (parentStopInfo) {
              let addedToParent = false;
              if (!parentStopInfo.feedUrls.has(feedUrl)) {
                parentStopInfo.feedUrls.add(feedUrl);
                addedToParent = true;
              }
              if (!parentStopInfo.servedByRouteIds.has(routeId)) {
                parentStopInfo.servedByRouteIds.add(routeId);
                addedToParent = true;
              }
              if (addedToParent) addedLink = true;
            }
          }
          if (addedLink) linksMade++;
        }
      }
      logger.info(
        `Finished processing ${stopTimes.length} stop times for feeds. Links added/updated: ${linksMade}`,
      );
    };
    processStopTimesForFeeds(allStopTimesRaw); // Process combined list
    logger.info("Pass 8 finished.");

    // --- *** ATOMIC UPDATE *** ---
    // If ALL phases succeeded, overwrite the module-level variable
    // with the newly processed data.
    staticData = {
      routes: tempLoadedRoutes,
      stops: tempLoadedStops,
      trips: tempLoadedTrips,
      stopTimeLookup: tempLoadedStopTimeLookup,
      lastRefreshed: new Date(),
    };
    // Ensure your StaticData type includes lastRefreshed: Date;

    const duration = Date.now() - startTime;
    logger.info(`
      [Static Data] >>> Module variable 'staticData' ASSIGNED. Routes count: ${staticData.routes.size}, Stops count: ${staticData.stops.size}`);
    // No return needed
  } catch (error) {
    logger.error(`Fatal error loading/reloading static GTFS data!`, { error });
    // ** IMPORTANT: Do NOT update the module-level staticData variable if loading failed **
    // The application will continue using the potentially stale but valid older data.
    // Re-throw the error to signal failure to the caller (e.g., the refresh task or initial startup)
    throw error;
  }
}

// getStaticData function
export function getStaticData(): StaticData {
  if (!staticData) {
    // Consider attempting loadStaticData here again or throwing a more specific error
    throw new Error(
      "Static data accessed before successful loading or after a loading error.",
    );
  }
  return staticData;
}
