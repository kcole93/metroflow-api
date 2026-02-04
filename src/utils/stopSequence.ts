// src/utils/stopSequence.ts

/**
 * Represents a stop time update from a GTFS-RT feed.
 * This interface captures the minimal structure needed for sequence operations.
 */
export interface StopTimeUpdate {
  stop_id?: string;
  stop_sequence?: string | number;
  [key: string]: unknown;
}

/**
 * Result of finding first and last stops in a sequence.
 */
export interface FirstLastStopsResult<T extends StopTimeUpdate> {
  firstStop: T;
  lastStop: T;
  minSequence: number;
  maxSequence: number;
}

/**
 * Finds the stop with the minimum sequence number from an array of stop time updates.
 *
 * @param updates - Array of stop time updates to search
 * @returns The stop time update with the minimum sequence number, or undefined if array is empty
 */
export function findFirstStop<T extends StopTimeUpdate>(updates: T[]): T | undefined {
  if (updates.length === 0) return undefined;

  let firstStop = updates[0];
  let minSequence = Number(firstStop.stop_sequence) || 0;

  for (let i = 1; i < updates.length; i++) {
    const currentSequence = Number(updates[i].stop_sequence) || 0;
    if (currentSequence < minSequence) {
      minSequence = currentSequence;
      firstStop = updates[i];
    }
  }

  return firstStop;
}

/**
 * Finds the stop with the maximum sequence number from an array of stop time updates.
 *
 * @param updates - Array of stop time updates to search
 * @returns The stop time update with the maximum sequence number, or undefined if array is empty
 */
export function findLastStop<T extends StopTimeUpdate>(updates: T[]): T | undefined {
  if (updates.length === 0) return undefined;

  let lastStop = updates[0];
  let maxSequence = Number(lastStop.stop_sequence) || 0;

  for (let i = 1; i < updates.length; i++) {
    const currentSequence = Number(updates[i].stop_sequence) || 0;
    if (currentSequence > maxSequence) {
      maxSequence = currentSequence;
      lastStop = updates[i];
    }
  }

  return lastStop;
}

/**
 * Finds both the first and last stops in a single pass through the array.
 * More efficient than calling findFirstStop and findLastStop separately.
 *
 * @param updates - Array of stop time updates to search
 * @returns Object containing firstStop, lastStop, and their sequence numbers, or undefined if array is empty
 */
export function findFirstAndLastStops<T extends StopTimeUpdate>(
  updates: T[]
): FirstLastStopsResult<T> | undefined {
  if (updates.length === 0) return undefined;

  let firstStop = updates[0];
  let lastStop = updates[0];
  let minSequence = Number(firstStop.stop_sequence) || 0;
  let maxSequence = minSequence;

  for (let i = 1; i < updates.length; i++) {
    const currentSequence = Number(updates[i].stop_sequence) || 0;
    if (currentSequence < minSequence) {
      minSequence = currentSequence;
      firstStop = updates[i];
    }
    if (currentSequence > maxSequence) {
      maxSequence = currentSequence;
      lastStop = updates[i];
    }
  }

  return { firstStop, lastStop, minSequence, maxSequence };
}
