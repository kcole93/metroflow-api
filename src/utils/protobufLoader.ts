// src/utils/protobufLoader.ts
import { logger } from "./logger";
import * as protobuf from "protobufjs";
import * as path from "path";

let root: protobuf.Root | null = null;
let feedMessageDefinition: protobuf.Type | null = null;

// --- Calculate paths relative to the compiled JS file location ---
// __dirname in the compiled /app/dist/utils/protobufLoader.js is /app/dist/utils
// We need the /app/dist/assets directory
const assetsDir = path.resolve(__dirname, "..", "assets");

// The base directory from which proto imports (like 'com/...') should be resolved
// This should also be the assets directory copied into dist
const protoIncludeBaseDir = assetsDir;

// Full paths to the main .proto files to load initially
const protoFilesToLoad = [
  path.join(
    protoIncludeBaseDir,
    "com/google/transit/realtime/gtfs-realtime.proto",
  ),
  path.join(
    protoIncludeBaseDir,
    "com/google/transit/realtime/gtfs-realtime-NYCT.proto",
  ),
  path.join(
    protoIncludeBaseDir,
    "com/google/transit/realtime/gtfs-realtime-MTARR.proto",
  ),
];

export async function loadProtobufDefinitions(): Promise<protobuf.Type> {
  if (feedMessageDefinition) {
    return feedMessageDefinition;
  }

  // Log the calculated paths for verification
  logger.info(
    `Attempting to load protobuf definitions from: ${protoFilesToLoad.join(", ")}`,
  );
  logger.debug(`Setting protobuf include path base to: ${protoIncludeBaseDir}`);

  try {
    // Create a new Root instance
    root = new protobuf.Root();

    // --- Set the search path for imports within .proto files ---
    // When a .proto file has `import "com/google/other.proto";`,
    // protobufjs will call this function. We resolve the target path
    // relative to our include base directory.
    root.resolvePath = (origin: string, target: string): string | null => {
      const resolved = path.resolve(protoIncludeBaseDir, target);
      logger.debug(
        `Resolving proto import: origin='${origin}', target='${target}' -> resolved='${resolved}'`,
      );
      return resolved;
    };

    // Load the main files using their absolute paths
    // protobufjs will use the resolvePath function above if these files contain imports
    await root.load(protoFilesToLoad, { keepCase: true });

    // Lookup the main FeedMessage type
    feedMessageDefinition = root.lookupType("transit_realtime.FeedMessage");

    if (!feedMessageDefinition) {
      throw new Error("FeedMessage type not found after loading proto files.");
    }

    logger.info(
      "Protobuf definitions loaded successfully. FeedMessage type resolved.",
    );
    return feedMessageDefinition;
  } catch (error: any) {
    logger.error(
      `Failed during protobuf load. Check paths and imports. Error details:`,
      {
        message: error.message,
        code: error.code,
        path: error.path,
        stack: error.stack?.substring(0, 500),
      },
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
