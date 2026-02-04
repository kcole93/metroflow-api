// src/services/staticDataService.ts
import { logger } from "../utils/logger";
import { parseCsvFile, processCsvFileStreaming } from "../utils/csvParser";
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
import { isTerminalStation } from "../config/systemConfig";

dotenv.config();

// Initialize with empty state
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
  ADA: string;
  "ADA Notes": string;
}

interface StationDetails {
  borough: string | null;
  northLabel: string | null;
  southLabel: string | null;
  adaStatus: number | null;
  adaNotes: string | null;
}

/**
 * Adds a route from GTFS data to the route map with proper system prefixing.
 */
function addRouteToMap(
  r: any,
  system: SystemType,
  map: Map<string, StaticRouteInfo>,
) {
  const routeId = r.route_id?.trim();
  if (!routeId) return;
  const uniqueKey = `${system}-${routeId}`;
  map.set(uniqueKey, {
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

/**
 * Adds a trip from GTFS data to the trip map.
 */
function addTripToMap(
  t: any,
  system: SystemType,
  map: Map<string, StaticTripInfo>,
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

  let wheelchairAccessible: number | null = null;
  if (t.wheelchair_accessible != null && t.wheelchair_accessible !== "") {
    const w = parseInt(t.wheelchair_accessible, 10);
    if (!isNaN(w) && w >= 0 && w <= 2) wheelchairAccessible = w;
  }

  const destStopId = destinations.get(tripId) || null;
  map.set(tripId, {
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
    adaStatus: t.ada?.trim() || null,
    adaNotes: t.ada_notes?.trim() || null,
  });
}

/**
 * Processes a stop from GTFS data with enhanced metadata.
 */
function processStop(
  rawStop: any,
  system: SystemType,
  map: Map<string, StaticStopInfo>,
  stationDetailsMap: Map<string, StationDetails>,
) {
  const originalStopId = rawStop.stop_id?.trim();
  if (!originalStopId) return;
  const uniqueStopKey = `${system}-${originalStopId}`;

  if (map.has(uniqueStopKey)) return; // Skip if already processed

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

  const stopName = rawStop.stop_name || "";
  const isTerminal = isTerminalStation(system, stopName, originalStopId);

  let northLabel: string | null = null;
  let southLabel: string | null = null;
  let adaStatus: number | null = null;
  let adaNotes: string | null = null;

  if (system === "SUBWAY") {
    const stationDetails = stationDetailsMap.get(originalStopId);
    if (stationDetails) {
      northLabel = stationDetails.northLabel ?? null;
      southLabel = stationDetails.southLabel ?? null;
      adaStatus = stationDetails.adaStatus ?? null;
      adaNotes = stationDetails.adaNotes ?? null;
    }
  }

  map.set(uniqueStopKey, {
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
    adaStatus: adaStatus,
    adaNotes: adaNotes,
  });
}

// Terminal station detection now uses isTerminalStation from systemConfig

/**
 * Processes stop_times for a single system using streaming.
 * Extracts destinations, builds stopTimeLookup, and links routes/feeds in a single pass.
 */
async function processStopTimesForSystem(
  system: SystemType,
  systemPath: string,
  tempLoadedTrips: Map<string, StaticTripInfo>,
  tempLoadedStops: Map<string, StaticStopInfo>,
  tempLoadedRoutes: Map<string, StaticRouteInfo>,
  tempLoadedStopTimeLookup: Map<string, Map<string, StaticStopTimeInfo>>,
  tripDestinations: Map<string, string>,
  tripMaxSequence: Map<string, number>,
): Promise<void> {
  const stopTimesPath = path.join(systemPath, "stop_times.txt");

  await processCsvFileStreaming<StopTimeBase>(
    stopTimesPath,
    logger,
    (st) => {
      const tripId = st.trip_id?.trim();
      const stopId = st.stop_id?.trim();
      const stopSequenceStr = st.stop_sequence;

      if (!tripId || !stopId || stopSequenceStr == null) return;

      const stopSequence = parseInt(stopSequenceStr, 10);
      if (isNaN(stopSequence)) return;

      // --- Update trip destination (find max sequence) ---
      const currentMax = tripMaxSequence.get(tripId) ?? -1;
      if (stopSequence > currentMax) {
        tripMaxSequence.set(tripId, stopSequence);
        tripDestinations.set(tripId, stopId);
      }

      // --- Build stopTimeLookup ---
      if (!tempLoadedStopTimeLookup.has(stopId)) {
        tempLoadedStopTimeLookup.set(stopId, new Map<string, StaticStopTimeInfo>());
      }

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

      const stopTimeInfo: StaticStopTimeInfo = {
        scheduledArrivalTime: st.arrival_time?.trim() || null,
        scheduledDepartureTime: st.departure_time?.trim() || null,
        stopSequence: stopSequence,
        track: st.track?.trim() || null,
        pickupType: pickupType,
        dropOffType: dropOffType,
        noteId: st.note_id?.trim() || null,
        noteText: null,
        borough: null,
        northLabel: null,
        southLabel: null,
        adaStatus: null,
        adaNotes: null,
      };

      tempLoadedStopTimeLookup.get(stopId)?.set(tripId, stopTimeInfo);
    },
  );
}

/**
 * Second pass: Link routes/feeds after trips are fully loaded with destinations.
 */
async function linkRoutesAndFeedsForSystem(
  system: SystemType,
  systemPath: string,
  tempLoadedTrips: Map<string, StaticTripInfo>,
  tempLoadedStops: Map<string, StaticStopInfo>,
  tempLoadedRoutes: Map<string, StaticRouteInfo>,
  tempLoadedStopTimeLookup: Map<string, Map<string, StaticStopTimeInfo>>,
): Promise<number> {
  const stopTimesPath = path.join(systemPath, "stop_times.txt");
  let linksMade = 0;

  await processCsvFileStreaming<StopTimeBase>(
    stopTimesPath,
    logger,
    (st) => {
      const stopTimeTripId = st.trip_id?.trim();
      if (!stopTimeTripId) return;

      const trip = tempLoadedTrips.get(stopTimeTripId);
      if (!trip?.route_id || !trip.system) return;

      const routeMapKey = `${trip.system}-${trip.route_id}`;
      const route = tempLoadedRoutes.get(routeMapKey);
      if (!route) return;

      const originalStopId = st.stop_id?.trim();
      if (!originalStopId) return;

      const childStopKey = `${trip.system}-${originalStopId}`;
      const childStopInfo = tempLoadedStops.get(childStopKey);
      if (!childStopInfo) return;

      const routeId = route.route_id;
      const routeSystem = route.system;
      const feedKey = `${routeSystem}-${routeId}`;
      const feedUrl = ROUTE_ID_TO_FEED_MAP[feedKey];

      if (feedUrl) {
        let addedLink = false;

        if (!childStopInfo.feedUrls.has(feedUrl)) {
          childStopInfo.feedUrls.add(feedUrl);
          addedLink = true;
        }
        if (!childStopInfo.servedByRouteIds.has(routeId)) {
          childStopInfo.servedByRouteIds.add(routeId);
          addedLink = true;
        }

        if (childStopInfo.parentStationId) {
          const parentStopInfo = tempLoadedStops.get(childStopInfo.parentStationId);
          if (parentStopInfo) {
            if (!parentStopInfo.feedUrls.has(feedUrl)) {
              parentStopInfo.feedUrls.add(feedUrl);
              addedLink = true;
            }
            if (!parentStopInfo.servedByRouteIds.has(routeId)) {
              parentStopInfo.servedByRouteIds.add(routeId);
              addedLink = true;
            }
          }
        }

        if (addedLink) linksMade++;
      }

      // For MNR, add trip_short_name lookup
      if (system === "MNR") {
        const tripInfo = tempLoadedTrips.get(stopTimeTripId);
        if (tripInfo?.trip_short_name) {
          const stopTimeMap = tempLoadedStopTimeLookup.get(originalStopId);
          if (stopTimeMap && !stopTimeMap.has(tripInfo.trip_short_name)) {
            const existingInfo = stopTimeMap.get(stopTimeTripId);
            if (existingInfo) {
              stopTimeMap.set(tripInfo.trip_short_name, existingInfo);
            }
          }
        }
      }
    },
  );

  return linksMade;
}

/**
 * Loads data for a single transit system sequentially to minimize memory usage.
 */
async function loadSystemData(
  system: SystemType,
  systemPath: string,
  tempLoadedRoutes: Map<string, StaticRouteInfo>,
  tempLoadedStops: Map<string, StaticStopInfo>,
  tempLoadedTrips: Map<string, StaticTripInfo>,
  tempLoadedStopTimeLookup: Map<string, Map<string, StaticStopTimeInfo>>,
  stationDetailsMap: Map<string, StationDetails>,
  tripDestinations: Map<string, string>,
  tripMaxSequence: Map<string, number>,
): Promise<void> {
  logger.info(`Loading ${system} data...`);

  // Load routes (small file)
  const routesRaw = await parseCsvFile<any>(path.join(systemPath, "routes.txt"), logger);
  for (const r of routesRaw) {
    addRouteToMap(r, system, tempLoadedRoutes);
  }
  logger.info(`${system}: Loaded ${routesRaw.length} routes`);

  // Load stops (small file)
  const stopsRaw = await parseCsvFile<any>(path.join(systemPath, "stops.txt"), logger);
  for (const s of stopsRaw) {
    processStop(s, system, tempLoadedStops, stationDetailsMap);
  }
  logger.info(`${system}: Loaded ${stopsRaw.length} stops`);

  // Load trips (medium file)
  const tripsRaw = await parseCsvFile<any>(path.join(systemPath, "trips.txt"), logger);

  // First pass: process stop_times to find destinations
  logger.info(`${system}: Processing stop_times (first pass - destinations)...`);
  await processStopTimesForSystem(
    system,
    systemPath,
    tempLoadedTrips,
    tempLoadedStops,
    tempLoadedRoutes,
    tempLoadedStopTimeLookup,
    tripDestinations,
    tripMaxSequence,
  );

  // Now add trips with destination info
  for (const t of tripsRaw) {
    addTripToMap(t, system, tempLoadedTrips, tripDestinations);
  }
  logger.info(`${system}: Loaded ${tripsRaw.length} trips`);

  // Clear trips raw data
  tripsRaw.length = 0;
}

// --- Main Static Data Loading Function ---
export async function loadStaticData(): Promise<void> {
  logger.info("Starting to load/reload static GTFS data...");
  const startTime = Date.now();

  const systems: { name: SystemType; path: string }[] = [
    { name: "LIRR", path: path.join(BASE_DATA_PATH, "LIRR") },
    { name: "SUBWAY", path: path.join(BASE_DATA_PATH, "NYCT") },
    { name: "MNR", path: path.join(BASE_DATA_PATH, "MNR") },
  ];

  const tempLoadedRoutes = new Map<string, StaticRouteInfo>();
  const tempLoadedStops = new Map<string, StaticStopInfo>();
  const tempLoadedTrips = new Map<string, StaticTripInfo>();
  const tempLoadedStopTimeLookup = new Map<string, Map<string, StaticStopTimeInfo>>();
  const stationDetailsMap = new Map<string, StationDetails>();
  const tripDestinations = new Map<string, string>();
  const tripMaxSequence = new Map<string, number>();

  try {
    // --- Phase 0: Load Station Details CSV ---
    logger.info("Phase 0: Loading Station Details CSV...");
    try {
      const stationCsvRaw = await parseCsvFile<StationCsvRow>(STATIONS_CSV_PATH, logger);
      let count = 0;
      for (const row of stationCsvRaw) {
        const stopId = row["GTFS Stop ID"]?.trim();
        if (stopId) {
          let parsedAdaStatus: number | null = null;
          const adaValue = row["ADA"]?.trim();
          if (adaValue !== undefined && adaValue !== null && adaValue !== "") {
            const parsedInt = parseInt(adaValue, 10);
            if (!isNaN(parsedInt)) {
              parsedAdaStatus = parsedInt;
            }
          }

          stationDetailsMap.set(stopId, {
            borough: row.Borough?.trim() || null,
            northLabel: row["North Direction Label"]?.trim() || null,
            southLabel: row["South Direction Label"]?.trim() || null,
            adaStatus: parsedAdaStatus,
            adaNotes: row["ADA Notes"]?.trim() || null,
          });
          count++;
        }
      }
      logger.info(`Phase 0 finished. Loaded details for ${count} stations from CSV.`);
    } catch (csvError) {
      logger.error(`Fatal error loading Station Details CSV (${STATIONS_CSV_PATH}):`, csvError);
      throw csvError;
    }

    // --- Phase 1: Load each system sequentially ---
    logger.info("Phase 1: Loading systems sequentially...");
    for (const sys of systems) {
      await loadSystemData(
        sys.name,
        sys.path,
        tempLoadedRoutes,
        tempLoadedStops,
        tempLoadedTrips,
        tempLoadedStopTimeLookup,
        stationDetailsMap,
        tripDestinations,
        tripMaxSequence,
      );
    }
    logger.info(`Phase 1 finished. Routes: ${tempLoadedRoutes.size}, Stops: ${tempLoadedStops.size}, Trips: ${tempLoadedTrips.size}`);

    // Clear destination tracking maps - no longer needed
    tripDestinations.clear();
    tripMaxSequence.clear();

    // --- Phase 2: Link children to parents ---
    logger.info("Phase 2: Linking child stops to parent stations...");
    let linkedChildrenCount = 0;
    for (const [, stopInfo] of tempLoadedStops.entries()) {
      if (stopInfo.parentStationId) {
        const parentStopInfo = tempLoadedStops.get(stopInfo.parentStationId);
        if (parentStopInfo) {
          parentStopInfo.childStopIds.add(stopInfo.originalStopId);
          linkedChildrenCount++;
        }
      }
    }
    logger.info(`Phase 2 finished. Linked ${linkedChildrenCount} children.`);

    // --- Phase 3: Link routes/feeds (second pass through stop_times) ---
    logger.info("Phase 3: Linking routes and feeds...");
    let totalLinks = 0;
    for (const sys of systems) {
      const links = await linkRoutesAndFeedsForSystem(
        sys.name,
        sys.path,
        tempLoadedTrips,
        tempLoadedStops,
        tempLoadedRoutes,
        tempLoadedStopTimeLookup,
      );
      totalLinks += links;
      logger.info(`${sys.name}: Added ${links} route/feed links`);
    }
    logger.info(`Phase 3 finished. Total links: ${totalLinks}`);

    // --- Phase 4: Build MNR lookup maps ---
    logger.info("Phase 4: Building lookup maps for MNR...");
    const tempTripsByShortName = new Map<string, string>();
    const tempVehicleTripsMap = new Map<string, string>();

    for (const [tripId, tripInfo] of tempLoadedTrips.entries()) {
      if (tripInfo.system === "MNR" && tripInfo.trip_short_name) {
        tempTripsByShortName.set(tripInfo.trip_short_name, tripId);
        tempVehicleTripsMap.set(tripInfo.trip_short_name, tripId);
      }
    }
    logger.info(`Phase 4 finished. MNR lookup maps: ${tempTripsByShortName.size} entries`);

    // --- Phase 5: Load MNR notes ---
    logger.info("Phase 5: Loading notes.txt for MNR...");
    const tempNotes = new Map<string, Note>();

    try {
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
      }
    } catch (error) {
      logger.warn(`Failed to load MNR notes.txt: ${error}`);
    }
    logger.info(`Phase 5 finished.`);

    // --- Assign to module variable ---
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

    const duration = Date.now() - startTime;
    logger.info(
      `[Static Data] Load complete in ${duration}ms. Routes: ${staticData.routes.size}, Stops: ${staticData.stops.size}, Trips: ${staticData.trips.size}`,
    );
  } catch (error) {
    logger.error(`Fatal error loading/reloading static GTFS data!`, { error });
    throw error;
  }
}

/**
 * Retrieves the currently loaded static data.
 */
export function getStaticData(): StaticData {
  if (!staticData) {
    throw new Error(
      "Static data accessed before successful loading or after a loading error.",
    );
  }
  return staticData;
}
