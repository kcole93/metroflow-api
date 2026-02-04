// src/config/systemConfig.ts
import { SystemType } from "../types";

/**
 * Configuration for a transit system's behavior and characteristics.
 */
export interface SystemConfig {
  /** System identifier */
  name: SystemType;

  /** Whether this is a commuter rail system (MNR/LIRR) vs subway */
  isCommuterRail: boolean;

  /** Whether direction labels come from station data (subway) or trip data */
  usesStationDirectionLabels: boolean;

  /** Whether to use trip_short_name for trip lookups (MNR) */
  usesTripShortName: boolean;

  /** Whether direction_id values are inverted (MNR: 0=Outbound, 1=Inbound) */
  invertedDirectionId: boolean;

  /** Whether arrival times should be used for terminal stations */
  useArrivalTimesForTerminals: boolean;

  /** GTFS-RT extension key for this system */
  realtimeExtensionKey: string | null;

  /** Terminal station identifiers (stop IDs) */
  terminalStopIds: Set<string>;

  /** Terminal station name patterns */
  terminalNamePatterns: string[];

  /** Direction label for direction_id=0 */
  direction0Label: "Outbound" | "N";

  /** Direction label for direction_id=1 */
  direction1Label: "Inbound" | "S";
}

/**
 * System configurations indexed by SystemType.
 */
const SYSTEM_CONFIGS: Record<SystemType, SystemConfig> = {
  LIRR: {
    name: "LIRR",
    isCommuterRail: true,
    usesStationDirectionLabels: false,
    usesTripShortName: false,
    invertedDirectionId: false,
    useArrivalTimesForTerminals: true,
    realtimeExtensionKey: ".transit_realtime.mta_railroad_stop_time_update",
    terminalStopIds: new Set(["349", "237", "52"]), // Penn Station, Atlantic Terminal, Jamaica
    terminalNamePatterns: ["Penn Station", "Atlantic Terminal", "Jamaica", "Hicksville"],
    direction0Label: "Outbound",
    direction1Label: "Inbound",
  },
  MNR: {
    name: "MNR",
    isCommuterRail: true,
    usesStationDirectionLabels: false,
    usesTripShortName: true,
    invertedDirectionId: true, // MNR: 0=Outbound, 1=Inbound (opposite of typical)
    useArrivalTimesForTerminals: true,
    realtimeExtensionKey: ".transit_realtime.mta_railroad_stop_time_update",
    terminalStopIds: new Set(["1"]), // Grand Central
    terminalNamePatterns: ["Grand Central", "Stamford", "New Haven"],
    direction0Label: "Outbound",
    direction1Label: "Inbound",
  },
  SUBWAY: {
    name: "SUBWAY",
    isCommuterRail: false,
    usesStationDirectionLabels: true,
    usesTripShortName: false,
    invertedDirectionId: false,
    useArrivalTimesForTerminals: false,
    realtimeExtensionKey: ".transit_realtime.nyct_stop_time_update",
    terminalStopIds: new Set(),
    terminalNamePatterns: [],
    direction0Label: "N",
    direction1Label: "S",
  },
  MIXED: {
    name: "MIXED",
    isCommuterRail: false,
    usesStationDirectionLabels: false,
    usesTripShortName: false,
    invertedDirectionId: false,
    useArrivalTimesForTerminals: false,
    realtimeExtensionKey: null,
    terminalStopIds: new Set(),
    terminalNamePatterns: [],
    direction0Label: "Outbound",
    direction1Label: "Inbound",
  },
  UNKNOWN: {
    name: "UNKNOWN",
    isCommuterRail: false,
    usesStationDirectionLabels: false,
    usesTripShortName: false,
    invertedDirectionId: false,
    useArrivalTimesForTerminals: false,
    realtimeExtensionKey: null,
    terminalStopIds: new Set(),
    terminalNamePatterns: [],
    direction0Label: "Outbound",
    direction1Label: "Inbound",
  },
};

/**
 * Gets the configuration for a transit system.
 */
export function getSystemConfig(system: SystemType): SystemConfig {
  return SYSTEM_CONFIGS[system] || SYSTEM_CONFIGS.UNKNOWN;
}

/**
 * Checks if a system is commuter rail (MNR or LIRR).
 */
export function isCommuterRail(system: SystemType): boolean {
  return SYSTEM_CONFIGS[system]?.isCommuterRail ?? false;
}

/**
 * Checks if a system uses trip_short_name for lookups.
 */
export function usesTripShortName(system: SystemType): boolean {
  return SYSTEM_CONFIGS[system]?.usesTripShortName ?? false;
}

/**
 * Determines if a station is a terminal based on system configuration.
 */
export function isTerminalStation(
  system: SystemType,
  stopName: string,
  stopId: string,
): boolean {
  const config = SYSTEM_CONFIGS[system];
  if (!config) return false;

  // Check by stop ID
  if (config.terminalStopIds.has(stopId)) {
    return true;
  }

  // Check by name pattern
  for (const pattern of config.terminalNamePatterns) {
    if (stopName.includes(pattern)) {
      return true;
    }
  }

  return false;
}

/**
 * Gets the direction label for a given direction_id, accounting for system-specific inversions.
 */
export function getDirectionLabel(
  system: SystemType,
  directionId: number | null | undefined,
): "Inbound" | "Outbound" | "N" | "S" | null {
  if (directionId === null || directionId === undefined) {
    return null;
  }

  const config = SYSTEM_CONFIGS[system];
  if (!config) return null;

  // Handle MNR's inverted direction_id
  if (config.invertedDirectionId) {
    return directionId === 0 ? "Outbound" : "Inbound";
  }

  return directionId === 0 ? config.direction0Label : config.direction1Label;
}

/**
 * Gets the GTFS-RT extension key for extracting system-specific data.
 */
export function getRealtimeExtensionKey(system: SystemType): string | null {
  return SYSTEM_CONFIGS[system]?.realtimeExtensionKey ?? null;
}
