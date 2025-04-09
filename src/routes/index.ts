// src/routes/index.ts
import { Router } from "express";
import mtaRoutes from "./mtaRoutes";

const router = Router();

// Prefix all MTA routes with /api/v1
router.use("/api/v1", mtaRoutes);

// Health check endpoint
router.get("/health", (req, res) => {
  res.status(200).json({ status: "OK", timestamp: new Date().toISOString() });
});

export default router;
