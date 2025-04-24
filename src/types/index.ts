// src/types/index.ts

// --- API Response Types (Used by Express routes) ---

// --- API Response Types ---
export interface Station {
  id: string;
  name: string;
  latitude?: number;
  longitude?: number;
  lines?: string[]; // Array of route short names (e.g., ["1"], ["B", "C"])
  system?: string;
  borough?: string;
  wheelchair_boarding?: number; // 0=No info, 1=Some accessible boarding, 2=Not possible
  accessibilityStatus?: string | null; // ADA status of the station
  accessibilityNotes?: string | null; // ADA direction notes of the station
}

export type Direction = "N" | "S" | "Inbound" | "Outbound" | "Unknown";
export type SystemType = "LIRR" | "SUBWAY" | "MNR" | "MIXED" | "UNKNOWN";
export type PeakStatus = "Peak" | "Off-Peak";

export interface StaticStopTimeInfo {
  scheduledDepartureTime?: string | null; // Store as HH:MM:SS string
  scheduledArrivalTime?: string | null; // Store as HH:MM:SS string
  stopSequence?: number;
  track?: string | null; // Include track if available in stop_times.txt
  pickupType?: number | null; // 0=Regular, 1=No pickup, 2=Call agency, 3=Coordinate with driver
  dropOffType?: number | null; // 0=Regular, 1=No drop off, 2=Call agency, 3=Coordinate with driver
  noteId?: string | null; // References specific notes for this stop time (MNR/LIRR)
  noteText?: string | null; // The actual note text associated with the noteId
  borough: string | null;
  northLabel: string | null; // The label for the north direction (e.g., "Northbound")
  southLabel: string | null; // The label for the south direction (e.g., "Southbound")
  adaStatus: number | null;
  adaNotes: string | null;
}

/**
 * Helper function to convert our internal system type to GTFS format
 * @param system The system type to convert
 * @returns The GTFS-compatible system type (lowercase)
 */
export function toGtfsSystemType(system: SystemType): string {
  switch (system) {
    case "SUBWAY":
      return "subway";
    case "UNKNOWN":
      return "unknown";
    default:
      return system.toLowerCase();
  }
}

/**
 * Helper function to normalize system type casing for GTFS compatibility
 * @param system The system type to normalize
 * @returns The normalized system type
 */
export function normalizeSystemType(system: SystemType): SystemType {
  if (system === "SUBWAY") return "SUBWAY";
  if (system === "UNKNOWN") return "UNKNOWN";
  return system;
}

export type DepartureSource = "realtime" | "scheduled";

export interface Departure {
  id: string;
  tripId?: string; // For debugging or advanced use
  routeId?: string; // From trips.txt -> routes.txt
  routeShortName?: string; // e.g., "4", "Port Washington" from routes.txt
  routeLongName?: string; // e.g. "Lexington Avenue Express" from routes.txt
  peakStatus?: PeakStatus | null;
  routeColor?: string | null;
  destination: string; // Often derived from trip headsign or stop sequence
  direction?: Direction | null;
  direction_id?: number | null; // 0 or 1, for LIRR/MNR only
  departureTime: Date | null; // Scheduled departure time as a Date object
  estimatedDepartureTime: Date | null; // Departure time adjusted for delays
  delayMinutes: number | null; // Calculated delay (null if unknown/scheduled)
  track?: string; // Sometimes available in LIRR/NYCT feed extensions
  status: string; // e.g., "On Time", "Delayed X min", "Scheduled", "Due"
  system: SystemType;
  destinationBorough: string | null;
  isTerminalArrival?: boolean; // Indicates if this is an arrival at a terminal station
  source: DepartureSource; // Indicates if this departure is from realtime or scheduled data
  wheelchair_accessible?: number | null; // 0 = no info, 1 = accessible, 2 = not accessible
  trainStatus?: string | null; // Train status from MTARR extensions (MNR/LIRR)
  pickupType?: number | null; // 0=Regular, 1=No pickup, 2=Call agency, 3=Coordinate with driver
  dropOffType?: number | null; // 0=Regular, 1=No drop off, 2=Call agency, 3=Coordinate with driver
  noteId?: string | null; // References specific notes for this stop time (MNR/LIRR)
  noteText?: string | null; // The actual note text associated with the noteId
}

export interface ServiceAlert {
  id: string; // From the alert feed entity id
  agency_id: string;
  title: string;
  description: string;
  affectedLines: string[]; // Route short names or IDs (e.g., ["SUBWAY-6", "LIRR-Babylon"])
  affectedStations: string[]; // Station IDs directly affected (e.g., ["LIRR-349"])
  affectedLinesLabels?: string[]; // Human-readable route names (e.g., ["6 Train", "Babylon Branch"])
  affectedStationsLabels?: string[]; // Human-readable station names (e.g., ["Penn Station"])
  startDate?: Date; // Date object or null
  endDate?: Date; // Date object or null
  url?: string;
}

// --- Internal Static Data Types (Used by services) ---

/**
 * Represents detailed information about a stop/station loaded from GTFS static files,
 * including which routes serve it and which real-time feeds cover those routes.
 */
export interface StaticStopInfo {
  id: string; // stop_id
  originalStopId: string; // stop_id from original GTFS files
  name: string; // stop_name
  latitude?: number; // stop_lat
  longitude?: number; // stop_lon
  parentStationId?: string | null; // parent_station from stops.txt (useful for distinguishing platforms vs stations)
  locationType?: number | null;
  childStopIds: Set<string>;
  servedByRouteIds: Set<string>;
  feedUrls: Set<string>;
  system: SystemType;
  borough?: string | null; // Optional borough name
  isTerminal?: boolean; // Indicates if this is a terminal station (major hub)
  wheelchairBoarding?: number | null; // 0=No information, 1=Some accessible boarding, 2=Not possible
  northLabel: string | null; // Human-readable label for northbound directions, Subway only
  southLabel: string | null; // Human-readable label for southbound directions, Subway only
  adaStatus?: number | null; // ADA status of the station
  adaNotes?: string | null; // ADA direction notes of the station
}

// Helper function to create a case-insensitive Set
export function createCaseInsensitiveSet(): Set<
  string & { __caseInsensitive__: true }
> {
  return new Set<string>() as Set<string & { __caseInsensitive__: true }>;
}

// Helper function to add to case-insensitive Set
export function addToCaseInsensitiveSet(
  set: Set<string & { __caseInsensitive__: true }>,
  value: string,
): void {
  set.add(value.toLowerCase() as string & { __caseInsensitive__: true });
}

// Helper function to check case-insensitive Set
export function hasInCaseInsensitiveSet(
  set: Set<string & { __caseInsensitive__: true }>,
  value: string,
): boolean {
  return set.has(value.toLowerCase() as string & { __caseInsensitive__: true });
}

/**
 * Represents a specific route loaded from GTFS static routes.txt.
 */
export interface StaticRouteInfo {
  route_id: string;
  agency_id?: string;
  route_short_name: string; // e.g., "6", "A", "PW" (verify LIRR/MNR)
  route_long_name: string; // e.g., "Lexington Avenue Local", "Port Washington Branch"
  route_desc?: string;
  route_type?: number; // GTFS route type (e.g., 1=Subway, 2=Rail)
  route_url?: string;
  route_color?: string; // e.g., "EE352E"
  route_text_color?: string; // e.g., "FFFFFF"
  system: SystemType;
}

/**
 * Represents a specific trip loaded from GTFS static trips.txt.
 */
export interface StaticTripInfo {
  route_id: string;
  service_id: string; // Links to calendar.txt/calendar_dates.txt for service patterns
  trip_id: string;
  trip_headsign?: string; // Usually the destination shown on the train
  trip_short_name?: string;
  peak_offpeak?: string | null;
  track?: string | null;
  direction_id?: number | null; // 0 or 1, often indicates direction (e.g., Uptown/Downtown)
  block_id?: string;
  shape_id?: string; // Links to shapes.txt for drawing route path
  destinationStopId?: string | null;
  start_date?: string;
  start_time?: string;
  wheelchair_accessible?: number | null; // 0 = no info, 1 = at least one wheelchair accommodation, 2 = no accommodations
  system: SystemType;
  adaStatus: number | null;
  adaNotes: string | null;
}

/**
 * Container for all loaded and processed static GTFS data.
 * This structure holds the maps used for efficient lookups within the services.
 */
export interface Note {
  noteId: string; // Unique identifier, like "H" or "B"
  noteMark: string; // Display marker
  noteTitle: string; // Title/heading for the note
  noteDesc: string; // Full description text
}

export type AccessibilityStatus =
  | "Fully Accessible"
  | "Partially Accessible"
  | "Not Accessible"
  | "No Information";

export interface StaticData {
  /** Enriched map where key is stop_id, value contains full stop info including related routes and feed URLs. */
  stops: Map<string, StaticStopInfo>;
  /** Map where key is route_id, value contains full route information. */
  routes: Map<string, StaticRouteInfo>;
  /** Map where key is trip_id, value contains full trip information. */
  trips: Map<string, StaticTripInfo>;
  /** Map where key is trip_short_name, value is trip_id - used for MNR trip lookups where vehicle.label = static trip_short_name */
  tripsByShortName?: Map<string, string>;
  /** Map where key is vehicle ID, value is trip_id - used for lookups where vehicleTripId is needed */
  vehicleTripsMap?: Map<string, string>;
  tripsBySchedule?: Map<string, StaticTripInfo>; // Keyed by schedule info
  stopTimeLookup: Map<string, Map<string, StaticStopTimeInfo>>;
  /** Map where key is note_id, value contains note information (used for MNR notes.txt) */
  notes?: Map<string, Note>;
  lastRefreshed: Date;
}
