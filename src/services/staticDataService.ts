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
  Note,
} from "../types";
import * as dotenv from "dotenv";
import { getBoroughForCoordinates } from "./geoService";
import { ROUTE_ID_TO_FEED_MAP } from "../services/mtaService";

dotenv.config();

// Initialize with empty state or types asserting it might be null initially
let currentStaticData: StaticData | null = null;
let staticData: StaticData | null = null;
const BASE_DATA_PATH = path.join(__dirname, "..", "assets", "gtfs-static");
const STATIONS_CSV_PATH = path.join(
  __dirname,
  "..",
  "assets",
  "geodata",
  "stations.csv",
);

// Base interface for raw stop time data from CSV
interface StopTimeBase {
  trip_id: string;
  arrival_time?: string;
  departure_time?: string;
  stop_id: string;
  stop_sequence: string;
  track?: string;
  pickup_type?: string;
  drop_off_type?: string;
  note_id?: string;
}

// Interface for MTA Stations CSV row data
interface StationCsvRow {
  "GTFS Stop ID": string;
  Borough: string;
  "North Direction Label": string;
  "South Direction Label": string;
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

  // Parse direction_id
  let directionIdNum: number | null = null;
  const dirIdStr = t.direction_id;
  if (dirIdStr != null && dirIdStr !== "") {
    const p = parseInt(dirIdStr, 10);
    if (!isNaN(p)) directionIdNum = p;
  }

  // Parse wheelchair_accessible
  let wheelchairAccessible: number | null = null;
  if (t.wheelchair_accessible != null && t.wheelchair_accessible !== "") {
    const w = parseInt(t.wheelchair_accessible, 10);
    if (!isNaN(w) && w >= 0 && w <= 2) wheelchairAccessible = w;
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
    wheelchair_accessible: wheelchairAccessible,
    system: system,
    destinationStopId: destStopId,
  });
}

function processStop(
  rawStop: any,
  system: SystemType,
  map: Map<string, StaticStopInfo>, // Use the passed map
  stationDetailsMap: Map<
    string,
    {
      borough: string | null;
      northLabel: string | null;
      southLabel: string | null;
    }
  >,
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

  // Parse locationType
  let locationTypeNum: number | null = null;
  const locStr = rawStop.location_type;
  if (typeof locStr === "string" && locStr.trim() !== "") {
    const p = parseInt(locStr, 10);
    if (!isNaN(p)) locationTypeNum = p;
  }

  // Parse wheelchair_boarding
  let wheelchairBoardingNum: number | null = null;
  const wheelchairStr = rawStop.wheelchair_boarding;
  if (typeof wheelchairStr === "string" && wheelchairStr.trim() !== "") {
    const w = parseInt(wheelchairStr, 10);
    if (!isNaN(w) && w >= 0 && w <= 2) wheelchairBoardingNum = w;
  }

  const parentStationFileId = rawStop.parent_station?.trim() || null;
  const uniqueParentKey = parentStationFileId
    ? `${system}-${parentStationFileId}`
    : null;
  const borough = getBoroughForCoordinates(
    !isNaN(latitude) ? latitude : undefined,
    !isNaN(longitude) ? longitude : undefined,
  );

  logger.debug(
    `[Borough Check] Stop: ${originalStopId}, Lat: ${latitude}, Lon: ${longitude}, Result: ${borough}`,
  );

  // Determine if this is a terminal/major station
  // Currently based on stop names
  const stopName = rawStop.stop_name || "";
  const isTerminal = determineIfTerminal(system, stopName, originalStopId);

  // Get North/South Labels from the pre-processed station details map
  const stationDetails = stationDetailsMap.get(originalStopId); // Lookup by original GTFS Stop ID
  const northLabel = stationDetails?.northLabel || null;
  const southLabel = stationDetails?.southLabel || null;
  const finalBorough = stationDetails?.borough || borough; // Set borough for Subway stops from CSV

  // Log wheelchair accessibility info for LIRR stations
  if (system === "LIRR" && wheelchairBoardingNum !== null) {
    const accessibilityStatus =
      wheelchairBoardingNum === 0
        ? "No information"
        : wheelchairBoardingNum === 1
          ? "Accessible"
          : "Not accessible";

    logger.debug(
      `[LIRR Accessibility] Station ${stopName} (${originalStopId}): ${accessibilityStatus}`,
    );
  }

  // Check the passed 'map' before setting
  if (!map.has(uniqueStopKey)) {
    map.set(uniqueStopKey, {
      // Use the passed 'map' variable
      id: uniqueStopKey,
      originalStopId: originalStopId,
      name: stopName,
      latitude: !isNaN(latitude) ? latitude : undefined,
      longitude: !isNaN(longitude) ? longitude : undefined,
      parentStationId: uniqueParentKey,
      locationType: locationTypeNum,
      childStopIds: new Set<string>(),
      servedByRouteIds: new Set<string>(),
      feedUrls: new Set<string>(),
      system: system,
      borough: borough,
      isTerminal: isTerminal,
      wheelchairBoarding: wheelchairBoardingNum,
      northLabel: northLabel,
      southLabel: southLabel,
    });
  }
}

// Helper function to determine if a station is a terminal station based on name and system
function determineIfTerminal(
  system: SystemType,
  stopName: string,
  stopId: string,
): boolean {
  // Very common major terminals/hubs that warrant special handling
  if (system === "MNR") {
    // MNR Terminal Stations
    return (
      stopName.includes("Grand Central") ||
      stopName.includes("Stamford") ||
      stopName.includes("New Haven") ||
      stopId === "1"
    ); // Grand Central has ID 1
  } else if (system === "LIRR") {
    // LIRR Terminal Stations
    return (
      stopName.includes("Penn Station") ||
      stopName.includes("Atlantic Terminal") ||
      stopName.includes("Jamaica") ||
      stopName.includes("Hicksville") ||
      stopId === "349" || // Penn Station has ID 349
      stopId === "237" || // Atlantic Terminal
      stopId === "52"
    ); // Jamaica station
  }

  return false;
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
  const stationDetailsMap = new Map<string, StaticStopTimeInfo>();

  try {
    // --- Phase 0: Load Station Details CSV ---
    logger.info("Phase 0: Loading Station Details CSV...");
    try {
      const stationCsvRaw = await parseCsvFile<StationCsvRow>(
        STATIONS_CSV_PATH,
        logger,
      );
      let count = 0;
      for (const row of stationCsvRaw) {
        const stopId = row["GTFS Stop ID"]?.trim(); // Match exact header name
        if (stopId) {
          stationDetailsMap.set(stopId, {
            borough: row.Borough?.trim() || null,
            northLabel: row["North Direction Label"]?.trim() || null,
            southLabel: row["South Direction Label"]?.trim() || null,
          });
          count++;
        }
      }
      logger.info(
        `Phase 0 finished. Loaded details for ${count} stations from CSV.`,
      );
    } catch (csvError) {
      logger.error(
        `Fatal error loading Station Details CSV (${STATIONS_CSV_PATH}):`,
        csvError,
      );
      throw csvError; // Stop loading if this critical file fails
    }

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
    logger.info("Pass 5: Processing raw stops into enriched map...");
    lirrStopsRaw.forEach((s) =>
      processStop(s, "LIRR", tempLoadedStops, stationDetailsMap),
    );
    subwayStopsRaw.forEach((s) =>
      processStop(s, "SUBWAY", tempLoadedStops, stationDetailsMap),
    );
    mnrStopsRaw.forEach((s) =>
      processStop(s, "MNR", tempLoadedStops, stationDetailsMap),
    );
    logger.info(
      `Pass 5 finished. enrichedStops size: ${tempLoadedStops.size}.`,
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
      // Parse pickup_type and drop_off_type values if available
      let pickupType: number | null = null;
      let dropOffType: number | null = null;

      if (st.pickup_type) {
        const pickupVal = parseInt(st.pickup_type.trim(), 10);
        if (!isNaN(pickupVal) && pickupVal >= 0 && pickupVal <= 3) {
          pickupType = pickupVal;
        }
      }

      if (st.drop_off_type) {
        const dropOffVal = parseInt(st.drop_off_type.trim(), 10);
        if (!isNaN(dropOffVal) && dropOffVal >= 0 && dropOffVal <= 3) {
          dropOffType = dropOffVal;
        }
      }

      // Create the stop time info object once to reuse
      const stopTimeInfo: StaticStopTimeInfo = {
        scheduledArrivalTime: st.arrival_time?.trim() || null,
        scheduledDepartureTime: st.departure_time?.trim() || null,
        stopSequence: stopSequence,
        track: st.track?.trim() || null,
        pickupType: pickupType,
        dropOffType: dropOffType,
        noteId: st.note_id?.trim() || null,
        borough: st.borough?.trim() || null,
        northLabel: st.north_label?.trim() || null,
        southLabel: st.south_label?.trim() || null,
      };

      // Add the primary entry with the original tripId
      tempLoadedStopTimeLookup.get(stopId)?.set(tripId, stopTimeInfo);

      // Check if this trip exists in the trips map to determine the system
      // For MNR trips specifically, we need to also index by vehicle label/trip_short_name
      const tripInfo = tempLoadedTrips.get(tripId);
      if (tripInfo?.system === "MNR" && tripInfo.trip_short_name) {
        // For MNR, add another entry using trip_short_name as key
        // This allows lookup by vehicle.label in the real-time feed
        tempLoadedStopTimeLookup
          .get(stopId)
          ?.set(tripInfo.trip_short_name, stopTimeInfo);
        logger.silly(
          `[MNR StopTime] Added additional lookup key for stop ${stopId}: ${tripInfo.trip_short_name} -> ${tripId}`,
        );
      }
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

    // --- ATOMIC UPDATE ---
    // If ALL phases succeeded, overwrite the module-level variable
    // with the newly processed data.
    // --- 9. Build the tripsByShortName and vehicleTripsMap lookup maps for MNR
    logger.info("Pass 9: Building lookup maps for MNR...");
    const tempTripsByShortName = new Map<string, string>();
    const tempVehicleTripsMap = new Map<string, string>();

    // Iterate through trips and build the lookup maps
    for (const [tripId, tripInfo] of tempLoadedTrips.entries()) {
      if (tripInfo.system === "MNR" && tripInfo.trip_short_name) {
        // For MNR, the vehicle.label is the trip_short_name
        // Map trip_short_name to trip_id for easy lookup
        tempTripsByShortName.set(tripInfo.trip_short_name, tripId);

        // Also map trip_short_name as vehicleId to trip_id
        // This allows lookup by vehicle.label
        tempVehicleTripsMap.set(tripInfo.trip_short_name, tripId);
      }
    }

    logger.info(
      `Pass 9 finished. Built tripsByShortName map with ${tempTripsByShortName.size} entries and vehicleTripsMap with ${tempVehicleTripsMap.size} entries.`,
    );

    // --- 10. Load notes.txt file for MNR ---
    logger.info("Pass 10: Loading notes.txt for MNR...");
    const tempNotes = new Map<string, Note>();

    try {
      // Only MNR has notes.txt so we only need to check there
      const mnrNotesPath = path.join(systems[2].path, "notes.txt");
      const mnrNotesRaw = await parseCsvFile<any>(mnrNotesPath, logger);

      if (mnrNotesRaw && mnrNotesRaw.length > 0) {
        for (const note of mnrNotesRaw) {
          const noteId = note.note_id?.trim();
          if (!noteId) continue;

          tempNotes.set(noteId, {
            noteId: noteId,
            noteMark: note.note_mark?.trim() || "",
            noteTitle: note.note_title?.trim() || "",
            noteDesc: note.note_desc?.trim() || "",
          });
        }
        logger.info(`Loaded ${tempNotes.size} notes from MNR notes.txt`);
      } else {
        logger.warn(
          "No notes found in MNR notes.txt or file could not be parsed",
        );
      }
    } catch (error) {
      // Just log the error but continue - notes are non-critical
      logger.warn(`Failed to load MNR notes.txt: ${error}`);
    }

    logger.info(
      `Pass 10 finished. Loaded ${tempNotes.size} notes from MNR notes.txt.`,
    );

    staticData = {
      routes: tempLoadedRoutes,
      stops: tempLoadedStops,
      trips: tempLoadedTrips,
      tripsByShortName: tempTripsByShortName,
      vehicleTripsMap: tempVehicleTripsMap,
      stopTimeLookup: tempLoadedStopTimeLookup,
      notes: tempNotes,
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
