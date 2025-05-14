// src/services/geoService.ts
import { logger } from "../utils/logger";
import * as fs from "fs/promises";
import * as path from "path";
import * as turfBooleanPointInPolygon from "@turf/boolean-point-in-polygon";
import * as turfHelpers from "@turf/helpers";
import { Feature, Polygon, MultiPolygon } from "geojson";
import * as dotenv from "dotenv";
dotenv.config();

// --- Configuration ---
const GEOJSON_PATH = path.join(
  __dirname,
  "..",
  "assets",
  "geodata",
  "nyc-boroughs.geojson",
);

// Property name in GeoJSON features containing the borough name
const BOROUGH_NAME_PROPERTY = process.env.BOROUGH_NAME_PROPERTY || "BoroName";

// --- Module State ---
// Store loaded features in memory
let boroughFeatures: Feature<Polygon | MultiPolygon>[] = [];
let isDataLoaded = false;
let isLoadingError = false;

// --- Initialization Function ---
/**
 * Loads NYC borough boundary data from GeoJSON file.
 * 
 * This function reads and parses the borough boundary polygons that are used
 * for geocoding transit stops and determining which borough a coordinate falls within.
 * The function implements safety mechanisms:
 * - Only loads once per application lifecycle
 * - Avoids retrying after failed attempts
 * - Reports detailed logging of the loading process
 * 
 * @returns Promise that resolves when loading is complete
 */
export async function loadBoroughData(): Promise<void> {
  if (isDataLoaded || isLoadingError) {
    // Avoid reloading if already loaded or if loading previously failed
    return;
  }

  const absoluteGeoJsonPath = path.resolve(GEOJSON_PATH);
  logger.info(`Loading borough boundaries from: ${absoluteGeoJsonPath}`);

  try {
    const geoJsonContent = await fs.readFile(absoluteGeoJsonPath, "utf8");
    const boroughData = JSON.parse(geoJsonContent);

    if (
      boroughData &&
      boroughData.type === "FeatureCollection" &&
      Array.isArray(boroughData.features)
    ) {
      boroughFeatures = boroughData.features.filter(
        // Basic validation: ensure features have geometry and properties with the name field
        (f: any) =>
          f.geometry && f.properties && f.properties[BOROUGH_NAME_PROPERTY],
      );
      isDataLoaded = true;
      logger.info(
        `Successfully loaded and parsed ${boroughFeatures.length} valid borough boundary features.`,
      );
    } else {
      logger.error(
        "Invalid GeoJSON format. Expected FeatureCollection with features array.",
      );
      isLoadingError = true; // Mark loading as failed
    }
  } catch (geoError: any) {
    if (geoError.code === "ENOENT") {
      logger.error(
        `Error loading boundaries: File not found at ${absoluteGeoJsonPath}. Geofencing disabled.`,
      );
    } else {
      logger.error(
        "Error loading or parsing borough boundaries GeoJSON:",
        geoError,
      );
    }
    isLoadingError = true; // Mark loading as failed
    boroughFeatures = []; // Ensure features array is empty on error
  }
}

// --- Lookup Function ---
/**
 * Determines which NYC borough a set of coordinates falls within.
 * 
 * This function uses point-in-polygon calculations with the loaded borough
 * boundary data to determine which borough (if any) contains the specified
 * coordinates. It's used to enrich transit data with borough information.
 * 
 * The function includes robust input validation and graceful fallback if:
 * - Boundary data failed to load
 * - Invalid coordinates are provided
 * - Coordinates don't fall within any known borough
 * 
 * @param latitude - Decimal latitude coordinate
 * @param longitude - Decimal longitude coordinate
 * @returns Borough name as string, or null if undetermined/invalid
 */
export function getBoroughForCoordinates(
  latitude?: number,
  longitude?: number,
): string | null {
  // Return null immediately if data failed to load, or if input is invalid
  if (
    isLoadingError ||
    !isDataLoaded ||
    typeof latitude !== "number" ||
    typeof longitude !== "number" ||
    isNaN(latitude) ||
    isNaN(longitude)
  ) {
    return null;
  }

  try {
    const stopPoint = turfHelpers.point([longitude, latitude]); // GeoJSON is [lon, lat]

    for (const feature of boroughFeatures) {
      // Use booleanPointInPolygon (handles Polygon and MultiPolygon)
      if (turfBooleanPointInPolygon.default(stopPoint, feature.geometry)) {
        // Extract borough name using the configured property name
        const boroughName = feature.properties?.[BOROUGH_NAME_PROPERTY];
        return typeof boroughName === "string" ? boroughName.trim() : null;
      }
    }
  } catch (error) {
    // Log any errors during the check itself
    logger.error(
      `Error checking point [${longitude}, ${latitude}] against polygons:`,
    );
  }

  // If no containing polygon found
  return null;
}

/**
 * Checks if borough boundary data has been successfully loaded.
 * 
 * This utility function allows other parts of the application to
 * verify that the geographic data is available before attempting
 * operations that depend on it, enabling graceful degradation.
 * 
 * @returns Boolean indicating if geo data is loaded and ready to use
 */
export function isGeoDataReady(): boolean {
  return isDataLoaded;
}
