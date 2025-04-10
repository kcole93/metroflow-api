// src/services/geoService.ts
import * as fs from "fs/promises";
import * as path from "path";
import * as turfBooleanPointInPolygon from "@turf/boolean-point-in-polygon";
import * as turfHelpers from "@turf/helpers";
import { Feature, Polygon, MultiPolygon } from "geojson";
import * as dotenv from "dotenv";

dotenv.config();

// --- Configuration ---
// Make the path configurable via .env, default to a reasonable location
const GEOJSON_PATH =
  process.env.BOROUGH_GEOJSON_PATH || "src/assets/geodata/nyc-boroughs.geojson";
// Property name in GeoJSON features containing the borough name (adjust if needed)
const BOROUGH_NAME_PROPERTY = process.env.BOROUGH_NAME_PROPERTY || "BoroName";

// --- Module State ---
// Store loaded features in memory
let boroughFeatures: Feature<Polygon | MultiPolygon>[] = [];
let isDataLoaded = false;
let isLoadingError = false;

// --- Initialization Function ---
export async function loadBoroughData(): Promise<void> {
  if (isDataLoaded || isLoadingError) {
    // Avoid reloading if already loaded or if loading previously failed
    return;
  }

  const absoluteGeoJsonPath = path.resolve(GEOJSON_PATH);
  console.log(
    `[GeoService] Loading borough boundaries from: ${absoluteGeoJsonPath}`,
  );

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
      console.log(
        `[GeoService] Successfully loaded and parsed ${boroughFeatures.length} valid borough boundary features.`,
      );
    } else {
      console.error(
        "[GeoService] Invalid GeoJSON format. Expected FeatureCollection with features array.",
      );
      isLoadingError = true; // Mark loading as failed
    }
  } catch (geoError: any) {
    if (geoError.code === "ENOENT") {
      console.error(
        `[GeoService] Error loading boundaries: File not found at ${absoluteGeoJsonPath}. Geofencing disabled.`,
      );
    } else {
      console.error(
        "[GeoService] Error loading or parsing borough boundaries GeoJSON:",
        geoError,
      );
    }
    isLoadingError = true; // Mark loading as failed
    boroughFeatures = []; // Ensure features array is empty on error
  }
}

// --- Lookup Function ---
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
  } catch (checkError) {
    // Log error during the check itself (less likely)
    console.error(
      `[GeoService] Error checking point [${longitude}, ${latitude}] against polygons:`,
      checkError,
    );
  }

  // If no containing polygon found
  return null;
}

// Optional: Function to check if data is ready
export function isGeoDataReady(): boolean {
  return isDataLoaded;
}
