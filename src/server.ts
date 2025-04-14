// src/server.ts
import express, { Express, Request, Response, NextFunction } from "express";
import { logger } from "./utils/logger";
import * as dotenv from "dotenv";
import routes from "./routes"; // Import combined routes
import { loadProtobufDefinitions } from "./utils/protobufLoader";
import { loadStaticData } from "./services/staticDataService";
import { loadBoroughData } from "./services/geoService";
import { refreshActiveServices } from "./services/calendarService";

dotenv.config();

const app: Express = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(express.json()); // Parse JSON bodies
// Add other middleware like CORS if needed, e.g., app.use(cors());
// Add logging middleware if desired

// Use Routes
app.use("/", routes);

// Basic Root Response
app.get("/", (req: Request, res: Response) => {
  res.send("MTA API Wrapper is running!");
});

// Global Error Handler (Optional - basic example)
app.use((err: Error, req: Request, res: Response, next: NextFunction) => {
  logger.error("Unhandled error:", err.stack);
  res.status(500).json({ error: "Something went wrong on the server." });
});

// --- Server Startup ---
async function startServer() {
  try {
    logger.info("Initializing server...");
    // Load essential data in parallel or sequence
    await Promise.all([
      loadProtobufDefinitions(),
      loadBoroughData(), // <-- Load borough boundaries
      refreshActiveServices(), // Calculate active services on startup
    ]);
    // Static data often depends on other setups, load last or separately
    await loadStaticData(); // Ensure this runs after others if needed

    app.listen(port, () => {
      console.info(
        `⚡️[server]: Server is running at http://localhost:${port}`,
      );
    });
  } catch (error) {
    console.error("🚨 Failed to start server:", error);
    process.exit(1);
  }
}

startServer();
