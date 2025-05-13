// src/services/cacheService.ts
import { logger } from "../utils/logger";
import NodeCache from "node-cache";
import * as dotenv from "dotenv";

dotenv.config();

interface SystemTtls {
  SUBWAY: number;
  LIRR: number;
  MNR: number;
  ALERTS: number;
  DEFAULT: number;
}

// Get TTL from environment variable or use default by system
const defaultTtl = parseInt(process.env.CACHE_TTL_SECONDS || "60", 10);

// Create system-specific TTLs to optimize for different data refresh rates
const systemTtls: SystemTtls = {
  SUBWAY: parseInt(process.env.CACHE_TTL_SUBWAY || String(defaultTtl), 10), // Subway data refreshes quickly
  LIRR: parseInt(process.env.CACHE_TTL_LIRR || String(defaultTtl * 2), 10), // LIRR data changes less frequently
  MNR: parseInt(process.env.CACHE_TTL_MNR || String(defaultTtl * 2), 10),   // MNR data changes less frequently
  ALERTS: parseInt(process.env.CACHE_TTL_ALERTS || "300", 10),              // Alerts can be cached longer
  DEFAULT: defaultTtl
};

// Create cache with default TTL
const cache = new NodeCache({
  stdTTL: defaultTtl,
  checkperiod: Math.max(Math.floor(defaultTtl * 0.2), 10), // At least check every 10 seconds
  useClones: false, // Better performance by not cloning objects
  deleteOnExpire: true, // Clean up expired items
  maxKeys: parseInt(process.env.CACHE_MAX_KEYS || "1000", 10) // Prevent memory leaks
});

// Log cache configuration at startup
logger.info(`Cache initialized with default TTL: ${defaultTtl} seconds`);
logger.info(`System-specific TTLs: ${JSON.stringify(systemTtls)}`);

// Helper to determine TTL based on cache key pattern
function determineTtl(key: string, explicitTtl?: number): number {
  if (explicitTtl && explicitTtl > 0) {
    return explicitTtl; // Always honor explicit TTL if provided
  }
  
  // Otherwise determine based on key pattern
  if (key.includes('_SUBWAY_')) return systemTtls.SUBWAY;
  if (key.includes('_LIRR_')) return systemTtls.LIRR;
  if (key.includes('_MNR_')) return systemTtls.MNR;
  if (key.includes('_alerts') || key.includes('_service_alerts')) return systemTtls.ALERTS;
  
  return systemTtls.DEFAULT;
}

// Get value from cache with logging
export function getCache<T>(key: string): T | undefined {
  const value = cache.get<T>(key);
  
  if (process.env.NODE_ENV !== 'production') {
    // Only log cache hits/misses in non-production environments to reduce log noise
    if (value === undefined) {
      logger.debug(`Cache MISS for key: ${key}`);
    } else {
      logger.debug(`Cache HIT for key: ${key}`);
    }
  }
  
  return value;
}

// Set value in cache with intelligent TTL
export function setCache<T>(
  key: string,
  value: T,
  explicitTtl?: number,
): boolean {
  // Don't cache null or undefined values
  if (value === null || value === undefined) {
    logger.debug(`Not caching null/undefined value for key: ${key}`);
    return false;
  }
  
  // Determine appropriate TTL
  const ttl = determineTtl(key, explicitTtl);
  
  // For arrays, don't cache empty arrays or ones with empty results
  if (Array.isArray(value) && value.length === 0) {
    logger.debug(`Not caching empty array for key: ${key}`);
    return false;
  }
  
  const success = cache.set(key, value, ttl);
  logger.debug(`${success ? 'Successfully cached' : 'Failed to cache'} key: ${key} with TTL: ${ttl}s`);
  return success;
}

// Clear specific cache key
export function clearCacheKey(key: string): void {
  if (cache.has(key)) {
    cache.del(key);
    logger.debug(`Cleared cache key: ${key}`);
  }
}

// Clear all keys matching a pattern
export function clearCachePattern(pattern: string): void {
  const keys = cache.keys().filter(key => key.includes(pattern));
  if (keys.length > 0) {
    keys.forEach(key => cache.del(key));
    logger.info(`Cleared ${keys.length} cache keys matching pattern: ${pattern}`);
  }
}

// Flush entire cache
export function flushCache(): void {
  const keyCount = cache.keys().length;
  cache.flushAll();
  logger.info(`Cache flushed. ${keyCount} keys removed.`);
}

interface CacheStats {
  keys: number;
  hits: number;
  misses: number;
  ksize: number;
  vsize: number;
}

// Get cache statistics for monitoring
export function getCacheStats(): CacheStats {
  return {
    keys: cache.keys().length,
    hits: cache.getStats().hits,
    misses: cache.getStats().misses,
    ksize: cache.getStats().ksize,
    vsize: cache.getStats().vsize
  };
}
