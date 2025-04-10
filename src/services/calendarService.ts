// src/services/calendarService.ts
import * as fs from "fs/promises";
import * as path from "path";
import Papa from "papaparse";
// Import necessary functions from date-fns
import { format, parse, startOfDay, isToday } from "date-fns";
import * as dotenv from "dotenv";

dotenv.config();

// --- Configuration ---
const BASE_DATA_PATH =
  process.env.STATIC_DATA_PATH || "./src/assets/gtfs-static";
// Define paths consistently (ensure these match staticDataService.ts and your structure)
const LIRR_PATH = path.join(BASE_DATA_PATH, "LIRR");
const SUBWAY_PATH = path.join(BASE_DATA_PATH, "NYCT");
const MNR_PATH = path.join(BASE_DATA_PATH, "MNR");

// --- Module State ---
let activeServicesToday = new Set<string>();
let lastLoadedDate: Date | null = null; // Track when the data was last calculated for
let isLoadingError = false; // Flag to prevent retrying if loading failed critically

// --- Helper to Parse CSV (Self-contained or move to shared utils) ---
async function parseCsvFile<T extends object>(filePath: string): Promise<T[]> {
  try {
    // console.log(`[CalendarService] Reading ${filePath}`); // Reduce noise
    const fileContent = await fs.readFile(filePath, "utf8");
    const result = Papa.parse<T>(fileContent, {
      header: true,
      skipEmptyLines: true,
      dynamicTyping: false, // Keep false to get consistent "0" / "1" strings
    });
    if (result.errors.length > 0) {
      console.warn(
        `[CalendarService] Parsing errors in ${path.basename(filePath)}:`,
        result.errors.slice(0, 2),
      );
    }
    return result.data;
  } catch (error) {
    console.error(
      `[CalendarService] Error reading/parsing CSV ${path.basename(filePath)}:`,
      error,
    );
    // Decide if error loading calendar is fatal for the whole app start?
    // For now, just throw, which will be caught by calculateActiveServices
    throw error;
  }
}

// --- Define type for raw calendar entry from calendar.txt ---
// Adjust property names if they differ in your specific calendar.txt files
interface RawCalendarEntry {
  service_id: string;
  monday: string; // Expect "0" or "1"
  tuesday: string;
  wednesday: string;
  thursday: string;
  friday: string;
  saturday: string;
  sunday: string;
  start_date: string; // Expect YYYYMMDD
  end_date: string; // Expect YYYYMMDD
}

// --- Define type for raw calendar date entry from calendar_dates.txt ---
interface RawCalendarDateEntry {
  service_id: string;
  date: string; // Expect YYYYMMDD
  exception_type: string; // Expect "1" (added) or "2" (removed)
}

// --- Calculate Active Services for a Specific Date ---
async function calculateActiveServices(targetDate: Date): Promise<Set<string>> {
  const calculationDateStr = format(targetDate, "yyyy-MM-dd");
  console.log(
    `[CalendarService] Calculating active services for ${calculationDateStr}...`,
  );
  const activeServices = new Set<string>();
  isLoadingError = false; // Reset error flag for this attempt

  const targetDateStrYYYYMMDD = format(targetDate, "yyyyMMdd");
  // Array matching Date.getDay() index to GTFS calendar column names
  const weekdayColumns: (keyof RawCalendarEntry)[] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
  ];
  const targetDayOfWeekKey = weekdayColumns[targetDate.getDay()];

  try {
    // Load all relevant calendar files concurrently
    const [
      lirrCalendarRaw,
      subwayCalendarRaw,
      mnrCalendarRaw,
      lirrCalendarDatesRaw,
      subwayCalendarDatesRaw,
      mnrCalendarDatesRaw,
    ] = await Promise.all([
      // Use .catch() to return empty array if a file is missing, preventing Promise.all failure
      parseCsvFile<RawCalendarEntry>(
        path.join(LIRR_PATH, "calendar.txt"),
      ).catch(() => []),
      parseCsvFile<RawCalendarEntry>(
        path.join(SUBWAY_PATH, "calendar.txt"),
      ).catch(() => []),
      parseCsvFile<RawCalendarEntry>(path.join(MNR_PATH, "calendar.txt")).catch(
        () => [],
      ),
      parseCsvFile<RawCalendarDateEntry>(
        path.join(LIRR_PATH, "calendar_dates.txt"),
      ).catch(() => []),
      parseCsvFile<RawCalendarDateEntry>(
        path.join(SUBWAY_PATH, "calendar_dates.txt"),
      ).catch(() => []),
      parseCsvFile<RawCalendarDateEntry>(
        path.join(MNR_PATH, "calendar_dates.txt"),
      ).catch(() => []),
    ]);

    // Combine data from all systems
    const allCalendarRaw = [
      ...lirrCalendarRaw,
      ...subwayCalendarRaw,
      ...mnrCalendarRaw,
    ];
    const allCalendarDatesRaw = [
      ...lirrCalendarDatesRaw,
      ...subwayCalendarDatesRaw,
      ...mnrCalendarDatesRaw,
    ];

    console.log(
      `[CalendarService] Processing ${allCalendarRaw.length} calendar entries and ${allCalendarDatesRaw.length} date exceptions.`,
    );

    // Process calendar.txt (base weekly schedule)
    for (const cal of allCalendarRaw) {
      const serviceId = cal.service_id?.trim();
      // Ensure all required fields are present and day key is valid
      if (
        !serviceId ||
        !cal.start_date ||
        !cal.end_date ||
        !targetDayOfWeekKey ||
        typeof cal[targetDayOfWeekKey] === "undefined"
      )
        continue;

      try {
        // Use startOfDay for reliable date comparisons (ignores time part)
        const startDate = startOfDay(
          parse(cal.start_date, "yyyyMMdd", new Date()),
        );
        const endDate = startOfDay(parse(cal.end_date, "yyyyMMdd", new Date()));
        const targetStartOfDay = startOfDay(targetDate);

        // Check date range AND if the flag for the target day of the week is "1"
        if (
          targetStartOfDay >= startDate &&
          targetStartOfDay <= endDate &&
          cal[targetDayOfWeekKey] === "1"
        ) {
          activeServices.add(serviceId);
        }
      } catch (dateParseError) {
        console.warn(
          `[CalendarService] Error parsing calendar dates for service ${serviceId}:`,
          dateParseError,
        );
      }
    }

    // Process calendar_dates.txt (exceptions for the target date)
    for (const cd of allCalendarDatesRaw) {
      const serviceId = cd.service_id?.trim();
      // Ensure required fields exist
      if (!serviceId || !cd.date || !cd.exception_type) continue;

      // Check if the exception applies to the target date
      if (cd.date === targetDateStrYYYYMMDD) {
        if (cd.exception_type === "1") {
          activeServices.add(serviceId); // Service added today
        } else if (cd.exception_type === "2") {
          activeServices.delete(serviceId); // Service removed today
        }
      }
    }

    console.log(
      `[CalendarService] Found ${activeServices.size} services active for ${calculationDateStr}.`,
    );
    return activeServices;
  } catch (error) {
    // Catch errors from file loading/parsing if .catch() wasn't used in Promise.all
    console.error(
      "[CalendarService] Error during active service calculation:",
      error,
    );
    isLoadingError = true; // Mark loading as failed
    return new Set<string>(); // Return empty set on error
  }
}

// --- Public Function to Get Active Services for Today ---
// Handles caching based on date and avoids recalculation if not needed.
export async function getActiveServicesForToday(): Promise<Set<string>> {
  const today = startOfDay(new Date()); // Use start of day for comparison consistency

  // Recalculate if:
  // - Never loaded before (lastLoadedDate is null)
  // - OR Loaded date is not today
  // - OR Previous loading attempt resulted in an error
  if (!lastLoadedDate || !isToday(lastLoadedDate) || isLoadingError) {
    console.log(
      `[CalendarService] Recalculating active services for today (${format(today, "yyyy-MM-dd")}). Previous load date: ${lastLoadedDate ? format(lastLoadedDate, "yyyy-MM-dd") : "N/A"}`,
    );
    activeServicesToday = await calculateActiveServices(today); // Perform calculation
    // Update lastLoadedDate only if calculation was successful
    if (!isLoadingError) {
      lastLoadedDate = today;
    }
  } else {
    // console.log("[CalendarService] Using cached active services for today."); // Reduce noise
  }

  // Return the current set (either newly calculated or cached)
  return activeServicesToday;
}

// --- Force Refresh Function ---
// Useful if you want to trigger a reload manually (e.g., after midnight)
export async function refreshActiveServices(): Promise<void> {
  console.log(
    "[CalendarService] Forcing refresh of active services for today...",
  );
  lastLoadedDate = null; // Invalidate cache date
  isLoadingError = false; // Reset error flag
  await getActiveServicesForToday(); // Trigger recalculation immediately
}
