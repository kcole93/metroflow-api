import { getCache, setCache, clearCacheKey } from "../services/cacheService";
import { loadProtobufDefinitions } from "../utils/protobufLoader";
import { LoggerService } from "./logger";

const logger =
  LoggerService.getInstance().createServiceLogger("GFTS Feed Parser");

// --- Fetch and parse a provided GTFS feed ---
export async function fetchAndParseFeed(
  url: string,
  feedName: string,
): Promise<{ message: any; feedObject: any } | null> {
  const cacheKey = `feed_${feedName}_${url.replace(/[^a-zA-Z0-9]/g, "_")}`;
  const cachedData = getCache<{ message: any; feedObject: any }>(cacheKey);

  if (cachedData) {
    const messageIsEmpty =
      !cachedData.message ||
      !cachedData.message.entity ||
      cachedData.message.entity.length === 0;
    const objectIsEmpty =
      !cachedData.feedObject ||
      !cachedData.feedObject.entity ||
      cachedData.feedObject.entity.length === 0;
    if (messageIsEmpty && objectIsEmpty) {
      logger.warn(
        `[Fetch] Cache hit for ${feedName} but data seems empty. Bypassing cache once.`,
      );
      clearCacheKey(cacheKey);
    } else {
      return cachedData;
    }
  }

  try {
    const FeedMessage = await loadProtobufDefinitions();
    const fetchOptions = {
      signal: AbortSignal.timeout(25000),
    };

    const response = await fetch(url, fetchOptions);
    if (!response.ok) {
      const errorBody = await response
        .text()
        .catch(() => "Could not read error body");
      logger.error(
        `[Fetch] API Request Error Status ${
          response.status
        } for ${feedName}: ${errorBody.slice(0, 1000)}`,
      );
      return null;
    }

    const contentType = response.headers.get("content-type");
    if (contentType?.includes("html") || contentType?.includes("json")) {
      logger.error(
        `[Fetch] Received likely error page Content-Type for ${feedName}: ${contentType}.`,
      );
      const textBody = await response.text().catch(() => "Could not read body");
      logger.error(
        `[Fetch] Error page body for ${feedName}: ${textBody.slice(0, 500)}...`,
      );
      return null;
    } else if (
      !contentType?.includes("octet-stream") &&
      !contentType?.includes("protobuf")
    ) {
      // logger.warn( // Reduce noise
      //     `[Fetch] Non-standard Content-Type for ${feedName}: ${contentType}. Attempting to parse anyway.`
      // );
    }

    const buffer = await response.arrayBuffer();
    if (buffer.byteLength === 0) {
      logger.warn(
        `[Fetch] Received EMPTY buffer for feed ${feedName} (${url}).`,
      );
      return null;
    }

    let message: any = null;
    try {
      message = FeedMessage.decode(new Uint8Array(buffer));
      if (!message || !message.entity || !Array.isArray(message.entity)) {
        logger.warn(
          `[Fetch] Decoded message for ${feedName} is missing 'entity' field, not an array, or message is null.`,
        );
        return null;
      }
      // logger.log(`[Fetch] Decoded ${message.entity.length} entities for ${feedName}.`); // Reduce noise
    } catch (decodeError) {
      logger.error(
        `[Fetch] Protobuf DECODING FAILED for ${feedName}:`,
        decodeError,
      );
      return null;
    }

    let feedObject: any = { entity: [], header: {} };
    try {
      if (message && message.entity && Array.isArray(message.entity)) {
        feedObject = FeedMessage.toObject(message, {
          longs: String,
          enums: String,
          bytes: String,
          arrays: true,
          objects: true,
          oneofs: true,
        });
      }
    } catch (toObjectError) {
      // logger.warn(`[Fetch] FeedMessage.toObject failed for ${feedName}. Error:`, toObjectError); // Reduce noise
    }

    const result = { message, feedObject };
    const cacheUrl = url.toLowerCase();
    setCache(cacheKey, result);
    return result;
  } catch (error: any) {
    logger.error(`[Fetch] Error for ${feedName} (${url}):`, error);
    if (error.name === "AbortError") {
      logger.error(`[Fetch] Request TIMED OUT for ${feedName}`);
    }
    return null;
  }
}
