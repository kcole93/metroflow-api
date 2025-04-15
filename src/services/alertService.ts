import { ServiceAlert } from "../types";
import { logger } from "../utils/logger";
import { fetchAndParseFeed } from "../utils/gtfsFeedParser";
import { getStaticData } from "./staticDataService";
import TurndownService from "turndown";

const MTA_API_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds";

export const ALERT_FEEDS = {
  SUBWAY: `${MTA_API_BASE}/camsys%2Fsubway-alerts`,
  LIRR: `${MTA_API_BASE}/camsys%2Flirr-alerts`,
  MNR: `${MTA_API_BASE}/camsys%2Fmnr-alerts`,
  ALL: `${MTA_API_BASE}/camsys%2Fall-alerts`,
};

const AGENCY_ID_TO_SYSTEM: { [key: string]: string } = {
  MTASBWY: "SUBWAY",
  "MTA NYCT": "SUBWAY",
  MTABC: "BUS",
  LI: "LIRR",
  MNR: "MNR",
};

// -- Initialize TurndownService
const turndownService = new TurndownService();

// Temporary internal type to hold raw HTML before final conversion
interface IntermediateServiceAlert extends Omit<ServiceAlert, "description"> {
  description: string; // Holds plain text fallback initially
  rawHtmlDescription?: string | null;
}

// --- getServiceAlerts Function ---
export async function getServiceAlerts(
  targetLines?: string[], // Expects ["SUBWAY-1", "LIRR-1"], etc.
  filterActiveNow = false,
): Promise<ServiceAlert[]> {
  const feedUrl = ALERT_FEEDS.ALL;
  const feedName = "all_service_alerts";
  logger.info(
    `[Alerts Service] Fetching ${feedName}. Filters: targetLines=${
      targetLines?.join(",") || "N/A"
    }, activeNow=${filterActiveNow}`,
  );

  const staticData = getStaticData();
  const staticRoutes = staticData.routes;

  const fetchedData = await fetchAndParseFeed(feedUrl, feedName);
  const message = fetchedData?.message;

  // --- Use Intermediate Type for Initial Parsing ---
  let intermediateAlerts: IntermediateServiceAlert[] = [];

  let allAlerts: ServiceAlert[] = [];

  if (!message?.entity?.length) {
    logger.warn(
      "[Alerts Service] No entities found in the alert feed message.",
    );
    return allAlerts;
  }

  logger.info(
    `[Alerts Service] Processing ${message.entity.length} entities from feed...`,
  );

  for (const entity of message.entity) {
    const alert = entity.alert;
    if (alert) {
      try {
        const affectedSystemRouteIds = new Set<string>(); // Use Set for automatic deduplication

        if (alert.informed_entity) {
          for (const informed of alert.informed_entity) {
            const agencyId = informed.agency_id; // Get agency ID first

            // --- Skip Bus Routes (MTABC) ---
            if (agencyId === "MTABC") {
              // logger.warn(`[Alerts Service] Alert ${entity.id}: Skipping informed_entity with agency_id "MTABC".`);
              continue; // Skip the rest of the loop for this informed_entity
            }

            // --- Disambiguation Logic (for non-skipped entities) ---
            let potentialSystemRouteId: string | null = null;
            const routeId = informed.route_id; // Raw route_id ("1")

            // We only care about entities that specify a route
            if (routeId) {
              if (agencyId && AGENCY_ID_TO_SYSTEM[agencyId]) {
                // We have a known agency and a route ID
                const systemPrefix = AGENCY_ID_TO_SYSTEM[agencyId];
                potentialSystemRouteId = `${systemPrefix}-${routeId}`;
                // logger.warn(`[Alerts Service] Alert ${entity.id}: Mapped agency "${agencyId}" + route "${routeId}" to "${potentialSystemRouteId}"`);
              } else {
                // Fallback/Warning: No agency_id or unknown agency_id for a route
                // If no agency context, we CANNOT reliably disambiguate.
                logger.warn(
                  `[Alerts Service] Alert ${entity.id}: Cannot reliably map route_id "${routeId}" to a system. Missing or unknown agency_id "${agencyId}". Skipping this route entity.`,
                );
                potentialSystemRouteId = null; // Ensure it's not added
              }

              // Validate constructed ID against static data AND add to Set
              if (
                potentialSystemRouteId &&
                staticRoutes.has(potentialSystemRouteId)
              ) {
                affectedSystemRouteIds.add(potentialSystemRouteId);
              } else if (potentialSystemRouteId) {
                // Log if we constructed an ID (meaning it wasn't MTABC and had agency info)
                // but it's not found in our static data map. This might indicate
                // stale static data or a new route ID in the feed.
                logger.debug(
                  `[Alerts Service] Alert ${entity.id}: Constructed System-RouteId "${potentialSystemRouteId}" but it's not found in staticRoutes map. Skipping.`,
                );
              }
            }
            // TODO: Handle informed.stop_id if needed (map stop to routes?)
          } // end informed_entity loop
        } // end if(informed_entity)

        const getText = (field: any): string | undefined =>
          field?.translation?.[0]?.text;
        const title = getText(alert.header_text) || "Untitled Alert";

        // --- Retrieve Description Variants ---
        let plainTextDescription = "No description.";
        let rawHtmlDescription: string | null = null;
        const descriptionTranslations = alert.description_text?.translation;

        if (descriptionTranslations && Array.isArray(descriptionTranslations)) {
          const htmlTranslation = descriptionTranslations.find(
            (t: any) => t?.language === "en-html",
          );
          const plainTranslation = descriptionTranslations.find(
            (t: any) => t?.language === "en",
          );

          if (htmlTranslation?.text) {
            rawHtmlDescription = htmlTranslation.text; // Store raw HTML
          }
          if (plainTranslation?.text) {
            plainTextDescription = plainTranslation.text; // Store plain text
          } else if (rawHtmlDescription && !plainTranslation?.text) {
            // If we only got HTML, use that as the initial description too
            // It will be converted later if this alert survives filtering
            plainTextDescription = rawHtmlDescription;
          }
        }

        const url = getText(alert.url);
        const startDateEpoch = alert.active_period?.[0]?.start;
        const endDateEpoch = alert.active_period?.[0]?.end;

        // Create the full alert object
        intermediateAlerts.push({
          id: entity.id,
          agency_id: entity.agency_id,
          title: title,
          description: plainTextDescription,
          rawHtmlDescription: rawHtmlDescription,
          affectedLines: Array.from(affectedSystemRouteIds).sort(), // Store processed short names
          startDate: startDateEpoch
            ? new Date(Number(startDateEpoch) * 1000)
            : undefined,
          endDate: endDateEpoch
            ? new Date(Number(endDateEpoch) * 1000)
            : undefined,
          url: url,
        });
      } catch (alertError) {
        logger.error(
          `[Alerts Service] Error processing alert entity ${entity.id}:`,
          alertError,
        );
      }
    } // end if(alert)
  } // end for loop
  logger.info(
    `[Alerts Service] Parsed ${intermediateAlerts.length} total alerts from feed.`,
  );

  // --- Apply Filters ---
  let filteredIntermediateAlerts = intermediateAlerts;

  // 1. Filter by Active Period
  if (filterActiveNow) {
    const now = Date.now();
    filteredIntermediateAlerts = filteredIntermediateAlerts.filter((alert) => {
      const start = alert.startDate?.getTime();
      const end = alert.endDate?.getTime();
      // Include if: No start date OR start date is in the past/now
      const started = !start || start <= now;
      // Include if: No end date OR end date is in the future/now
      const notEnded = !end || end >= now;
      return started && notEnded;
    });
    logger.info(
      `[Alerts Service] Filtered to ${filteredIntermediateAlerts.length} active alerts.`,
    );
  }

  // 2. Filter by Target Lines
  if (targetLines && targetLines.length > 0) {
    // Normalize targetLines from query param
    // Ensure case matches how affectedLinesShortNames were stored
    const targetLinesUpper = targetLines.map((l) => l.toUpperCase());

    filteredIntermediateAlerts = filteredIntermediateAlerts.filter((alert) => {
      // Check if ANY of the alert's affected lines match ANY of the target lines
      return alert.affectedLines.some(
        (alertLine) => targetLinesUpper.includes(alertLine.toUpperCase()), // Case-insensitive check
      );
    });
    logger.info(
      `[Alerts Service] Filtered to ${
        filteredIntermediateAlerts.length
      } alerts affecting lines: [${targetLines.join(", ")}]`,
    );
  }
  // --- End Apply Filters ---

  // Sort the *filtered* results (e.g., by start date descending)
  filteredIntermediateAlerts.sort(
    (a, b) => (b.startDate?.getTime() ?? 0) - (a.startDate?.getTime() ?? 0),
  );

  // Convert HTML to Markdown only for the filtered alerts
  const finalAlerts: ServiceAlert[] = filteredIntermediateAlerts.map(
    (alert) => {
      let finalDescription = alert.description; // Start with the plain text/fallback

      //If raw HTML exists for this alert, try converting it
      if (alert.rawHtmlDescription) {
        try {
          finalDescription = turndownService
            .turndown(alert.rawHtmlDescription)
            .trim();
          logger.debug(
            `[Alerts Service] Final Conversion: Converted HTML to Markdown for alert ${alert.id}.`,
          );
        } catch (conversionError) {
          logger.error(
            `[Alerts Service] Final Conversion: Failed for alert ${alert.id}. Using fallback description.`,
            { error: conversionError },
          );
          // Keep the plainTextDescription already in alert.description
        }
      }

      // Create the final ServiceAlert object without the temporary rawHtmlDescription field
      const { rawHtmlDescription, ...rest } = alert; // Destructure to remove rawHtmlDescription
      return {
        ...rest, // Spread the rest of the properties (id, title, affectedLines, etc.)
        description: finalDescription, // Use the final (potentially converted) description
      };
    },
  );

  return finalAlerts;
}
