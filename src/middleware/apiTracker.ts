// src/middleware/apiTracker.ts
import { Request, Response, NextFunction } from "express";
import { analyticsService } from "../services/analyticsService";

export const trackApiUsage = (
  req: Request,
  res: Response,
  next: NextFunction,
): void => {
  const endpoint = `${req.baseUrl || ""}${req.path}`;

  // Track the hit *after* the response is finished to not delay the response
  res.on("finish", () => {
    // Exclude common noise like favicon requests or OPTIONS requests
    if (endpoint !== "/favicon.ico" && req.method !== "OPTIONS") {
      analyticsService.trackApiHit(endpoint);
    }
  });

  // Continue to the next middleware/route handler
  next();
};
