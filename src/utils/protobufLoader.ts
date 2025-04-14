// src/utils/protobufLoader.ts
import { logger } from "./logger";
import * as protobuf from "protobufjs";
import * as path from "path";
import * as dotenv from "dotenv";

dotenv.config();

let root: protobuf.Root | null = null;
let feedMessageDefinition: protobuf.Type | null = null;

// Paths to the actual files in their new location
const protoFiles = [
  process.env.PROTO_BASE_PATH ||
    "src/assets/com/google/transit/realtime/gtfs-realtime.proto",
  process.env.PROTO_NYCT_PATH ||
    "src/assets/com/google/transit/realtime/gtfs-realtime-NYCT.proto",
  process.env.PROTO_MTARR_PATH ||
    "src/assets/com/google/transit/realtime/gtfs-realtime-MTARR.proto",
];

// The directory containing the start of the import path ('com')
const includeBaseDir = path.resolve("src/assets"); // Points to the directory holding 'com'

export async function loadProtobufDefinitions(): Promise<protobuf.Type> {
  if (feedMessageDefinition) {
    return feedMessageDefinition;
  }

  const absoluteProtoPaths = protoFiles.map((p) => path.resolve(p));
  logger.debug(
    `Resolved absolute paths for proto files: ${absoluteProtoPaths.join(", ")}`,
  );

  try {
    logger.info(
      `Attempting to load protobuf definitions from: ${absoluteProtoPaths.join(", ")}`,
    );
    logger.debug(
      `Setting include path for protobuf imports: ${includeBaseDir}`,
    ); // Log the base dir

    // Create a new Root instance
    root = new protobuf.Root();

    // --- Set the search path for imports ---
    // This tells protobufjs: "When you see an import like 'com/...',
    // look for a 'com' directory inside this path."
    root.resolvePath = (origin: string, target: string) => {
      return path.resolve(includeBaseDir, target);
    };

    // Load the files specified by absolute paths
    await root.load(absoluteProtoPaths, { keepCase: true });

    // Lookup the main FeedMessage type
    feedMessageDefinition = root.lookupType("transit_realtime.FeedMessage");

    if (!feedMessageDefinition) {
      throw new Error("FeedMessage type not found after loading proto files.");
    }

    logger.info(
      "Protobuf definitions loaded successfully. FeedMessage type resolved.",
    );
    return feedMessageDefinition;
  } catch (error) {
    // Log the specific error to understand what failed (e.g., file not found, parsing error)
    logger.error(
      `Failed during protobuf load. Check paths and imports. Error details:`,
      error,
    );
    throw new Error("Could not load GTFS-Realtime protobuf definitions.");
  }
}

export function getProtobufRoot(): protobuf.Root {
  if (!root) {
    throw new Error("Protobuf root accessed before loading definitions.");
  }
  return root;
}
