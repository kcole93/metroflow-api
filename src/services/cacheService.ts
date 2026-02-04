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
const DEFAULT_TTL = parseInt(process.env.CACHE_TTL_SECONDS || "60", 10);

// Create system-specific TTLs to optimize for different data refresh rates
const systemTtls: SystemTtls = {
  SUBWAY: parseInt(process.env.CACHE_TTL_SUBWAY || String(DEFAULT_TTL), 10), // Subway data refreshes quickly
  LIRR: parseInt(process.env.CACHE_TTL_LIRR || String(DEFAULT_TTL * 2), 10), // LIRR data changes less frequently
  MNR: parseInt(process.env.CACHE_TTL_MNR || String(DEFAULT_TTL * 2), 10),   // MNR data changes less frequently
  ALERTS: parseInt(process.env.CACHE_TTL_ALERTS || "300", 10),              // Alerts can be cached longer
  DEFAULT: DEFAULT_TTL
};

// Create cache with default TTL
const cache = new NodeCache({
  stdTTL: DEFAULT_TTL,
  checkperiod: Math.max(Math.floor(DEFAULT_TTL * 0.2), 10), // At least check every 10 seconds
  useClones: false, // Better performance by not cloning objects
  deleteOnExpire: true, // Clean up expired items
  maxKeys: parseInt(process.env.CACHE_MAX_KEYS || "1000", 10) // Prevent memory leaks
});

// Log cache configuration at startup
logger.info(`Cache initialized with default TTL: ${DEFAULT_TTL} seconds`);
logger.info(`System-specific TTLs: ${JSON.stringify(systemTtls)}`);

// Helper to determine TTL based on cache key pattern
/**
 * Determines the appropriate time-to-live (TTL) for a cache item based on its key.
 * 
 * This function implements an intelligent TTL selection strategy:
 * 1. Honors explicitly provided TTL values when available
 * 2. Uses system-specific TTLs based on key pattern matching
 * 3. Falls back to a default TTL when no specific match is found
 * 
 * Different transit systems have different data refresh patterns, which is why
 * they get assigned different TTL values.
 * 
 * @param key - The cache key to determine TTL for
 * @param explicitTtl - Optional explicit TTL override in seconds
 * @returns The determined TTL value in seconds
 */
function determineTtl(key: string, explicitTtl?: number): number {
  if (explicitTtl && explicitTtl > 0) {
    return explicitTtl; // Always honor explicit TTL if provided
  }
  
  // Otherwise determine based on key pattern
  if (key.includes('_SUBWAY_')) return systemTtls.SUBWAY;
  if (key.includes('_LIRR_')) return systemTtls.LIRR;
  if (key.includes('_MNR_')) return systemTtls.MNR;
  if (key.includes('_alerts') || key.includes('_service_alerts')) return systemTtls.ALERTS;
  
  return DEFAULT_TTL; // Fall back to default
}

// Get value from cache with logging
/**
 * Retrieves a value from the cache by its key.
 * 
 * This function provides type-safe access to cached values and includes
 * diagnostic logging of cache hits/misses in non-production environments.
 * Logging is suppressed in production to reduce overhead and log noise.
 * 
 * @template T - The expected type of the cached value
 * @param key - The cache key to retrieve
 * @returns The cached value typed as T, or undefined if not found
 */
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
/**
 * Removes a specific item from the cache by its key.
 * 
 * This function safely removes a cached item if it exists, with
 * appropriate logging. It's a no-op if the key doesn't exist in the cache.
 * 
 * @param key - The cache key to remove
 */
export function clearCacheKey(key: string): void {
  if (cache.has(key)) {
    cache.del(key);
    logger.debug(`Cleared cache key: ${key}`);
  }
}

// Clear all keys matching a pattern
/**
 * Removes multiple cache items matching a pattern.
 *
 * This function is useful for invalidating groups of related cached items,
 * such as all items related to a particular transit system or data type.
 * It uses simple substring matching to identify keys to clear.
 *
 * @internal
 * @param pattern - The pattern to match against cache keys
 */
export function clearCachePattern(pattern: string): void {
  const keys = cache.keys().filter(key => key.includes(pattern));
  if (keys.length > 0) {
    keys.forEach(key => cache.del(key));
    logger.info(`Cleared ${keys.length} cache keys matching pattern: ${pattern}`);
  }
}

// Flush entire cache
/**
 * Completely clears all items from the cache.
 *
 * This function performs a full cache flush, removing all cached items
 * regardless of their keys or TTL values. It's typically used during
 * system initialization or when a major data refresh occurs.
 *
 * @internal
 */
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
/**
 * Retrieves current cache statistics for monitoring.
 * 
 * This function provides diagnostic information about the cache's
 * current state, including hit/miss counters and memory usage metrics.
 * It's useful for performance monitoring and debugging.
 * 
 * @returns A CacheStats object containing various cache performance metrics
 */
export function getCacheStats(): CacheStats {
  return {
    keys: cache.keys().length,
    hits: cache.getStats().hits,
    misses: cache.getStats().misses,
    ksize: cache.getStats().ksize,
    vsize: cache.getStats().vsize
  };
}
