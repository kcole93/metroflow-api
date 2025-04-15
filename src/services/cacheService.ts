// src/services/cacheService.ts
import { logger } from "../utils/logger";
import NodeCache from "node-cache";
import * as dotenv from "dotenv";

dotenv.config();

const ttlSeconds = parseInt(process.env.CACHE_TTL_SECONDS || "60", 10);
const cache = new NodeCache({
  stdTTL: ttlSeconds,
  checkperiod: ttlSeconds * 0.2,
  useClones: false,
});

logger.info(`Cache initialized with TTL: ${ttlSeconds} seconds.`);

export function getCache<T>(key: string): T | undefined {
  return cache.get<T>(key);
}

export function setCache<T>(
  key: string,
  value: T,
  ttl: number = ttlSeconds,
): boolean {
  logger.info(`Setting cache for key: ${key} with TTL: ${ttl}`);
  return cache.set(key, value, ttl);
}

export function clearCacheKey(key: string): void {
  cache.del(key);
}

export function flushCache(): void {
  cache.flushAll();
  logger.info("Cache flushed.");
}
