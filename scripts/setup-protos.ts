// scripts/setup-protos.ts
// Downloads required GTFS Realtime protobuf definition files and geodata

import * as fs from "fs";
import * as path from "path";

const PROTO_DIR = path.join(__dirname, "..", "src", "assets", "com", "google", "transit", "realtime");
const GEODATA_DIR = path.join(__dirname, "..", "src", "assets", "geodata");

const PROTO_FILES = [
  {
    name: "gtfs-realtime.proto",
    url: "https://raw.githubusercontent.com/google/transit/master/gtfs-realtime/proto/gtfs-realtime.proto",
  },
  {
    name: "gtfs-realtime-NYCT.proto",
    url: "https://raw.githubusercontent.com/OneBusAway/onebusaway-gtfs-realtime-api/master/src/main/proto/com/google/transit/realtime/gtfs-realtime-NYCT.proto",
  },
  {
    name: "gtfs-realtime-MTARR.proto",
    url: "https://raw.githubusercontent.com/OneBusAway/onebusaway-gtfs-realtime-api/master/src/main/proto/com/google/transit/realtime/gtfs-realtime-MTARR.proto",
  },
];

async function downloadFile(url: string, destPath: string): Promise<void> {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to download ${url}: ${response.status} ${response.statusText}`);
  }
  const content = await response.text();
  fs.writeFileSync(destPath, content, "utf-8");
}

const GEODATA_FILES = [
  {
    name: "nyc-boroughs.geojson",
    url: "https://data.cityofnewyork.us/api/geospatial/tqmj-j8zm?method=export&format=GeoJSON",
  },
];

async function main(): Promise<void> {
  console.log("Setting up GTFS Realtime proto files...\n");

  // Create directory structure
  if (!fs.existsSync(PROTO_DIR)) {
    fs.mkdirSync(PROTO_DIR, { recursive: true });
    console.log(`Created directory: ${PROTO_DIR}`);
  }

  // Download each proto file
  for (const file of PROTO_FILES) {
    const destPath = path.join(PROTO_DIR, file.name);

    if (fs.existsSync(destPath)) {
      console.log(`[SKIP] ${file.name} already exists`);
      continue;
    }

    console.log(`[DOWNLOAD] ${file.name} from ${file.url}`);
    try {
      await downloadFile(file.url, destPath);
      console.log(`[OK] ${file.name}`);
    } catch (error) {
      console.error(`[ERROR] Failed to download ${file.name}:`, error);
      process.exit(1);
    }
  }

  console.log("\nSetting up geodata files...\n");

  // Create geodata directory
  if (!fs.existsSync(GEODATA_DIR)) {
    fs.mkdirSync(GEODATA_DIR, { recursive: true });
    console.log(`Created directory: ${GEODATA_DIR}`);
  }

  // Download geodata files
  for (const file of GEODATA_FILES) {
    const destPath = path.join(GEODATA_DIR, file.name);

    if (fs.existsSync(destPath)) {
      console.log(`[SKIP] ${file.name} already exists`);
      continue;
    }

    console.log(`[DOWNLOAD] ${file.name} from ${file.url}`);
    try {
      await downloadFile(file.url, destPath);
      console.log(`[OK] ${file.name}`);
    } catch (error) {
      console.error(`[ERROR] Failed to download ${file.name}:`, error);
      process.exit(1);
    }
  }

  console.log("\nSetup complete!");
}

main().catch((error) => {
  console.error("Setup failed:", error);
  process.exit(1);
});
