// src/server.ts
import express, { Express, Request, Response, NextFunction } from "express";
import * as dotenv from "dotenv";
import routes from "./routes"; // Import combined routes
import { loadProtobufDefinitions } from "./utils/protobufLoader";
import { loadStaticData } from "./services/staticDataService";

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
  console.error("Unhandled error:", err.stack);
  res.status(500).json({ error: "Something went wrong on the server." });
});

// --- Server Startup ---
async function startServer() {
  try {
    // Pre-load necessary data before starting the server
    console.log("Initializing server...");
    await loadProtobufDefinitions();
    await loadStaticData(); // Load static data into memory

    app.listen(port, () => {
      console.log(`⚡️[server]: Server is running at http://localhost:${port}`);
    });
  } catch (error) {
    console.error("🚨 Failed to start server:", error);
    process.exit(1); // Exit if essential data failed to load
  }
}

startServer();
