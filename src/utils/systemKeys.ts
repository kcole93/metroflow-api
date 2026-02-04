// src/utils/systemKeys.ts

import { SystemType } from "../types";

/**
 * Creates a unique key by combining a system name with an identifier.
 * This is the fundamental pattern used throughout the codebase for
 * uniquely identifying entities across different transit systems.
 *
 * @param system - The transit system (SUBWAY, LIRR, MNR, etc.)
 * @param id - The entity identifier (stop ID, route ID, etc.)
 * @returns A unique key in the format "SYSTEM-id"
 */
export function createSystemKey(system: SystemType | string, id: string): string {
  return `${system}-${id}`;
}

/**
 * Creates a unique key for a route.
 *
 * @param system - The transit system
 * @param routeId - The route identifier
 * @returns A unique route key in the format "SYSTEM-routeId"
 */
export function createRouteKey(system: SystemType | string, routeId: string): string {
  return createSystemKey(system, routeId);
}

/**
 * Creates a unique key for a stop/station.
 *
 * @param system - The transit system
 * @param stopId - The stop identifier
 * @returns A unique stop key in the format "SYSTEM-stopId"
 */
export function createStopKey(system: SystemType | string, stopId: string): string {
  return createSystemKey(system, stopId);
}
