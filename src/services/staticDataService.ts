// src/services/staticDataService.ts
import { LoggerService } from "../utils/logger";
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

// getInstance logger specifically for static data
const logger = LoggerService.getInstance().createServiceLogger("Static Data");

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

// --- Main Static Data Loading Function ---
export async function loadStaticData(): Promise<StaticData> {
  if (staticData) {
    logger.log("Returning cached static data.");
    return staticData;
  }

  // Define system paths to locate static data files
  const systems = [
    { name: "LIRR" as SystemType, path: path.join(BASE_DATA_PATH, "LIRR") },
    { name: "SUBWAY" as SystemType, path: path.join(BASE_DATA_PATH, "NYCT") },
    { name: "MNR" as SystemType, path: path.join(BASE_DATA_PATH, "MNR") },
  ];

  logger.log("Loading static GTFS data...");

  try {
    // --- 1. Load All Raw Files ---
    logger.log("Phase 1: Loading raw CSV files...");
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
    logger.log("Phase 1 finished.");

    // --- 2. Build tempRoutes Map (Key: SYSTEM-ROUTEID) ---
    logger.log("Phase 2: Building routes map...");
    const tempRoutes = new Map<string, StaticRouteInfo>();
    const addRouteToMap = (r: any, system: SystemType) => {
      const routeId = r.route_id?.trim();
      if (!routeId) return;
      const uniqueKey = `${system}-${routeId}`;
      tempRoutes.set(uniqueKey, {
        route_id: routeId,
        agency_id: r.agency_id?.trim() || undefined,
        route_short_name: r.route_short_name?.trim() || "",
        route_long_name: r.route_long_name?.trim() || "",
        route_type: r.route_type ? parseInt(r.route_type, 10) : undefined,
        route_color: r.route_color,
        route_text_color: r.route_text_color?.trim() || null,
        system: system, // Assign system so that we can disambiguate later
      });
    };
    lirrRoutesRaw.forEach((r) => addRouteToMap(r, "LIRR"));
    subwayRoutesRaw.forEach((r) => addRouteToMap(r, "SUBWAY"));
    mnrRoutesRaw.forEach((r) => addRouteToMap(r, "MNR"));
    logger.log(`Phase 2 finished. Routes map size: ${tempRoutes.size}`);

    // --- 3. Process stop_times to find Trip Destinations (Key: raw trip_id) ---
    logger.log(
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
    logger.log(
      `Pass 3 finished. Found destinations for ${tripDestinations.size} trips.`,
    );

    // --- 4. Build FINAL tempTrips map (Key: raw trip_id) ---
    logger.log("Pass 4: Building final tempTrips map...");
    const tempTrips = new Map<string, StaticTripInfo>();
    const addTripToMap = (t: any, system: SystemType) => {
      const tripId = t.trip_id?.trim();
      const routeId = t.route_id?.trim();
      if (!tripId || !routeId) return;
      let directionIdNum: number | null = null;
      const dirIdStr = t.direction_id;
      if (dirIdStr != null && dirIdStr !== "") {
        const p = parseInt(dirIdStr, 10);
        if (!isNaN(p)) directionIdNum = p;
      }
      const destStopId = tripDestinations.get(tripId) || null; // Lookup destination ID
      tempTrips.set(tripId, {
        // Explicitly list fields from StaticTripInfo type
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
        destinationStopId: destStopId, // Store destination ID
      });
    };
    lirrTripsRaw.forEach((t) => addTripToMap(t, "LIRR"));
    subwayTripsRaw.forEach((t) => addTripToMap(t, "SUBWAY"));
    mnrTripsRaw.forEach((t) => addTripToMap(t, "MNR"));
    logger.log(`Pass 4 finished. Final tempTrips map size: ${tempTrips.size}`);

    // --- 5. Enrich static data ---
    logger.log(
      "Pass 5: Processing raw stops into enriched map + geofencing...",
    );
    const enrichedStops = new Map<string, StaticStopInfo>();
    let stopsGeofencedCount = 0;
    const processStop = (rawStop: any, system: SystemType) => {
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
      if (borough) stopsGeofencedCount++;

      if (!enrichedStops.has(uniqueStopKey)) {
        enrichedStops.set(uniqueStopKey, {
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
    };
    lirrStopsRaw.forEach((s) => processStop(s, "LIRR"));
    subwayStopsRaw.forEach((s) => processStop(s, "SUBWAY"));
    mnrStopsRaw.forEach((s) => processStop(s, "MNR"));
    logger.log(
      `Pass 5 finished. enrichedStops size: ${enrichedStops.size}. Geofenced: ${stopsGeofencedCount}`,
    );

    // --- 6. Link children to parents (using unique keys) ---
    logger.log("Pass 6: Linking child stops to parent stations...");
    let linkedChildrenCount = 0;
    for (const [childKey, stopInfo] of enrichedStops.entries()) {
      if (stopInfo.parentStationId) {
        // parentStationId is unique key "SYSTEM-ID"
        const parentStopInfo = enrichedStops.get(stopInfo.parentStationId);
        if (parentStopInfo) {
          parentStopInfo.childStopIds.add(stopInfo.originalStopId); // Add ORIGINAL child ID
          linkedChildrenCount++;
        }
      }
    }
    logger.log(`Pass 6 finished. Linked ${linkedChildrenCount} children.`);

    // --- 7. Build StopTime Lookup Map (Key: original_stop_id -> trip_id -> info) ---
    logger.log("Pass 7: Building stopTimeLookup map...");
    const stopTimeLookup = new Map<string, Map<string, StaticStopTimeInfo>>();
    for (const st of allStopTimesRaw) {
      const stopId = st.stop_id?.trim();
      const tripId = st.trip_id?.trim(); // Use raw tripId from stop_times
      const stopSequenceStr = st.stop_sequence;
      if (!stopId || !tripId || stopSequenceStr == null) continue;
      const stopSequence = parseInt(stopSequenceStr, 10);
      if (isNaN(stopSequence)) continue;

      if (!stopTimeLookup.has(stopId)) {
        stopTimeLookup.set(stopId, new Map<string, StaticStopTimeInfo>());
      }
      stopTimeLookup.get(stopId)?.set(tripId, {
        scheduledArrivalTime: st.arrival_time?.trim() || null,
        scheduledDepartureTime: st.departure_time?.trim() || null,
        stopSequence: stopSequence,
        track: st.track?.trim() || null,
      });
    }
    logger.log(
      `Pass 7 finished. Built stopTimeLookup map for ${stopTimeLookup.size} stops.`,
    );

    // --- 8. Process stop_times to link routes/feeds ---
    logger.log("Pass 8: Processing stop times to link routes/feeds...");
    const processStopTimesForFeeds = (stopTimes: StopTimeBase[]) => {
      let linksMade = 0;
      for (const st of stopTimes) {
        const stopTimeTripId = st.trip_id?.trim();
        if (!stopTimeTripId) continue;
        const trip = tempTrips.get(stopTimeTripId); // Lookup trip by raw ID
        if (!trip?.route_id || !trip.system) continue; // Need system from trip

        const routeMapKey = `${trip.system}-${trip.route_id}`; // Use trip's system + route_id
        const route = tempRoutes.get(routeMapKey);
        if (!route) continue; // Skip if route lookup fails

        const originalStopId = st.stop_id?.trim();
        if (!originalStopId) continue;
        const childStopKey = `${trip.system}-${originalStopId}`; // Use trip's system + ST stop_id
        const childStopInfo = enrichedStops.get(childStopKey);
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
            const parentStopInfo = enrichedStops.get(
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
      logger.log(
        `Finished processing ${stopTimes.length} stop times for feeds. Links added/updated: ${linksMade}`,
      );
    };
    processStopTimesForFeeds(allStopTimesRaw); // Process combined list
    logger.log("Pass 8 finished.");

    // Final staticData object
    staticData = {
      stops: enrichedStops,
      routes: tempRoutes,
      trips: tempTrips, // Map keyed by raw trip_id
      stopTimeLookup: stopTimeLookup,
    };
    logger.log(
      `Static data loaded: ${enrichedStops.size} total stops processed.`,
    );
    return staticData;
  } catch (error) {
    logger.error(`Fatal error loading static GTFS data: ${error}`);
    staticData = null; // Ensure staticData is null on error
    throw new Error("Could not load essential static GTFS data.");
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
