// src/utils/parseValidate.ts

/**
 * Options for parsing integers safely.
 */
export interface SafeParseIntOptions {
  /** Minimum allowed value (inclusive) */
  min?: number;
  /** Maximum allowed value (inclusive) */
  max?: number;
  /** Default value to return if parsing fails or value is out of range */
  defaultValue?: number;
}

/**
 * Safely parses an integer with optional range validation.
 *
 * @param value - The value to parse (string, number, null, or undefined)
 * @param options - Optional parsing configuration
 * @returns The parsed integer, default value, or null if parsing fails
 */
export function safeParseInt(
  value: string | number | null | undefined,
  options: SafeParseIntOptions = {}
): number | null {
  const { min, max, defaultValue } = options;

  if (value === null || value === undefined) {
    return defaultValue !== undefined ? defaultValue : null;
  }

  const parsed = typeof value === "number" ? value : parseInt(String(value), 10);

  if (isNaN(parsed)) {
    return defaultValue !== undefined ? defaultValue : null;
  }

  // Range validation
  if (min !== undefined && parsed < min) {
    return defaultValue !== undefined ? defaultValue : null;
  }
  if (max !== undefined && parsed > max) {
    return defaultValue !== undefined ? defaultValue : null;
  }

  return parsed;
}

/**
 * Parses a direction ID value (0 or 1).
 * GTFS uses direction_id: 0 = outbound, 1 = inbound
 *
 * @param value - The value to parse
 * @returns 0 or 1 if valid, null otherwise
 */
export function parseDirectionId(
  value: string | number | null | undefined
): 0 | 1 | null {
  const parsed = safeParseInt(value, { min: 0, max: 1 });
  return parsed === 0 || parsed === 1 ? parsed : null;
}

/**
 * Parses a wheelchair accessibility value (0, 1, or 2).
 * GTFS uses: 0 = no info, 1 = accessible, 2 = not accessible
 *
 * @param value - The value to parse
 * @returns 0, 1, or 2 if valid, null otherwise
 */
export function parseWheelchairAccessibility(
  value: string | number | null | undefined
): 0 | 1 | 2 | null {
  const parsed = safeParseInt(value, { min: 0, max: 2 });
  return parsed === 0 || parsed === 1 || parsed === 2 ? parsed : null;
}
