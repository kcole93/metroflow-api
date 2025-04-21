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
  stationId?: string,  // Optional station ID to filter alerts affecting a specific station
): Promise<ServiceAlert[]> {
  const feedUrl = ALERT_FEEDS.ALL;
  const feedName = "all_service_alerts";
  logger.info(
    `[Alerts Service] Fetching ${feedName}. Filters: targetLines=${
      targetLines?.join(",") || "N/A"
    }, activeNow=${filterActiveNow}, stationId=${stationId || "N/A"}`,
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
        const affectedStationIds = new Set<string>(); // Set to track directly affected stations

        if (alert.informed_entity) {
          for (const informed of alert.informed_entity) {
            const agencyId = informed.agency_id; // Get agency ID first

            // --- Skip Bus Routes (MTABC) ---
            if (agencyId === "MTABC") {
              // logger.debug(`[Alerts Service] Alert ${entity.id}: Skipping informed_entity with agency_id "MTABC".`);
              continue; // Skip the rest of the loop for this informed_entity
            }

            // Get system prefix from agency ID (e.g., "MTASBWY" -> "SUBWAY")
            const systemPrefix = agencyId && AGENCY_ID_TO_SYSTEM[agencyId];
            if (!systemPrefix) {
              logger.debug(
                `[Alerts Service] Alert ${entity.id}: Unknown agency_id "${agencyId}". May affect stop-level data.`
              );
              // Don't skip here - we still want to process stop_id even if agency is unknown
            }

            // Handle route_id first
            const routeId = informed.route_id; // Raw route_id ("1")
            if (routeId && systemPrefix) {
              // We have a known agency and a route ID
              const potentialSystemRouteId = `${systemPrefix}-${routeId}`;
              logger.debug(
                `[Alerts Service] Alert ${entity.id}: Mapped agency "${agencyId}" + route "${routeId}" to "${potentialSystemRouteId}"`,
              );

              // Validate constructed ID against static data AND add to Set
              if (staticRoutes.has(potentialSystemRouteId)) {
                affectedSystemRouteIds.add(potentialSystemRouteId);
              } else {
                // Log if we constructed an ID but it's not found in our static data map
                logger.debug(
                  `[Alerts Service] Alert ${entity.id}: Constructed System-RouteId "${potentialSystemRouteId}" but it's not found in staticRoutes map. Skipping.`,
                );
              }
            }
            
            // Handle stop_id - important for station-specific alerts (elevator outages, etc.)
            const stopId = informed.stop_id;
            if (stopId) {
              // For alerts without a systemPrefix (like elevator alerts), try all possible systems
              const systemsToTry = systemPrefix ? [systemPrefix] : Object.values(AGENCY_ID_TO_SYSTEM);
              
              for (const system of systemsToTry) {
                const stationId = `${system}-${stopId}`;
                
                // Check if this station exists in our static data
                if (staticData.stops.has(stationId)) {
                  const stopInfo = staticData.stops.get(stationId);
                  
                  // Add the actual stop ID from the alert
                  affectedStationIds.add(stationId);
                  
                  // If this is a child stop, also add its parent station ID
                  // This ensures the alert shows up when filtered by the parent station ID
                  if (stopInfo && stopInfo.parentStationId) {
                    affectedStationIds.add(stopInfo.parentStationId);
                    logger.debug(
                      `[Alerts Service] Alert ${entity.id}: Added parent station ${stopInfo.parentStationId} for child stop ${stationId}`
                    );
                  }
                  
                  logger.debug(
                    `[Alerts Service] Alert ${entity.id}: Added affected station ${stationId}`
                  );
                  break; // Found a match, no need to try other systems
                }
              }
              
              // Log if we couldn't find a matching station after trying all systems
              if (systemsToTry.length > 0 && !Array.from(affectedStationIds).some(id => id.endsWith(`-${stopId}`))) {
                logger.debug(
                  `[Alerts Service] Alert ${entity.id}: Stop ID ${stopId} not found in static data with any system prefix`
                );
              }
            }
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
          affectedStations: Array.from(affectedStationIds).sort(), // Store affected station IDs
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
  
  // 3. Filter by Station ID
  if (stationId) {
    // Get the system type from the station ID (e.g., "LIRR-123" => "LIRR")
    const [system] = stationId.split('-');
    
    // Look up the station info to get the routes that serve this station
    const staticData = getStaticData();
    const stationInfo = staticData.stops.get(stationId);
    
    if (stationInfo) {
      // Get routes that serve this station
      const stationRoutesWithSystem = Array.from(stationInfo.servedByRouteIds || [])
        .map(routeId => `${system}-${routeId}`);
      
      logger.debug(
        `[Alerts Service] Station ${stationId} (${stationInfo.name}) is served by routes: [${stationRoutesWithSystem.join(', ') || 'none'}]`
      );
      
      // Filter alerts - include if affecting this station directly OR if affecting lines serving this station
      filteredIntermediateAlerts = filteredIntermediateAlerts.filter(alert => {
        // Check if alert directly affects this station (direct stop_id match)
        const directlyAffectsStation = alert.affectedStations.includes(stationId);
        
        // Check if alert affects any routes serving this station
        const affectsStationRoutes = stationRoutesWithSystem.length > 0 && 
          alert.affectedLines.some(alertLine => stationRoutesWithSystem.includes(alertLine));
        
        return directlyAffectsStation || affectsStationRoutes;
      });
      
      logger.info(
        `[Alerts Service] Filtered to ${filteredIntermediateAlerts.length} alerts affecting station: ${stationId} (${stationInfo.name})`
      );
    } else {
      logger.warn(`[Alerts Service] Could not find station with ID: ${stationId}`);
    }
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
