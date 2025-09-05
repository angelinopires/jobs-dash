"""
Redis Cache Manager

A simplified Redis-only caching system for job search results.
Replaces the current file-based caching with a Redis → API strategy.

Key Features:
- Uses existing RedisManager for robust connection management
- Uses CacheKeyGenerator for consistent, normalized keys
- TTL-based expiration
- Graceful Redis failure handling
"""

import logging
from typing import Any, Dict, List, Optional

from core.cache.cache_key_generator import CacheKeyGenerator
from settings.environment import get_cache_config

from .redis_manager import RedisManager

logger = logging.getLogger(__name__)


class RedisCacheManager:
    """
    Simple Redis-only cache manager for job search results

    This is like a specialized caching service that sits between your job scrapers
    and Redis. Think of it as a smart cache layer that knows how to:
    - Generate consistent cache keys for job searches
    - Store/retrieve job data in Redis with proper TTL
    - Handle Redis failures gracefully (just skip caching, don't break the app)
    """

    def __init__(self, cache_ttl_seconds: Optional[int] = None) -> None:
        """
        Initialize the Redis cache manager

        Args:
            cache_ttl_seconds: Cache TTL in seconds (optional, uses Redis TTL from config)
        """
        # Get cache configuration (uses existing Redis TTL directly in seconds)
        cache_config = get_cache_config()
        self.cache_ttl_seconds = cache_ttl_seconds or cache_config.ttl_seconds

        # Initialize Redis manager and key generator
        self.redis_manager = RedisManager()
        self.key_generator = CacheKeyGenerator()

        # Performance tracking
        self._cache_stats = {"hits": 0, "misses": 0, "errors": 0, "total_requests": 0}

        logger.info(f"RedisCacheManager initialized - TTL: {self.cache_ttl_seconds}s (from Redis config)")

    def get_cached_result(
        self, scraper: str, search_term: str, country: str, **kwargs: Any
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Get cached job search results from Redis

        Args:
            scraper: Name of the scraper (e.g., 'indeed', 'linkedin')
            search_term: Job title or search term
            country: Country/location for the search
            **kwargs: Additional search parameters (remote, posting_age, etc.)

        Returns:
            Optional[List[Dict[str, Any]]]: Cached job results or None if not found/error
        """
        self._cache_stats["total_requests"] += 1

        # Skip cache if Redis is unhealthy (simple strategy: always cache when Redis available)
        if not self.redis_manager.is_healthy():
            logger.debug("Redis unhealthy, skipping cache lookup")
            self._cache_stats["errors"] += 1
            return None

        try:
            # Map time_filter to posting_age for cache key generation
            cache_kwargs = kwargs.copy()
            if "time_filter" in cache_kwargs:
                cache_kwargs["posting_age"] = cache_kwargs.pop("time_filter")
            # Generate cache key
            cache_key = self.key_generator.generate_cache_key(
                scraper=scraper, search_term=search_term, location=country, **cache_kwargs
            )

            # Try to get from Redis
            cached_data = self.redis_manager.get_json(cache_key)

            if cached_data is not None:
                self._cache_stats["hits"] += 1
                logger.debug(f"Cache HIT for key: {cache_key}")
                return cached_data if isinstance(cached_data, list) else None
            else:
                self._cache_stats["misses"] += 1
                logger.debug(f"Cache MISS for key: {cache_key}")
                return None

        except Exception as e:
            self._cache_stats["errors"] += 1
            logger.error(f"Error getting cached result for {scraper}/{search_term}: {e}")
            return None

    def cache_result(
        self, scraper: str, search_term: str, country: str, result: List[Dict[str, Any]], **kwargs: Any
    ) -> bool:
        """
        Store job search results in Redis cache

        Args:
            scraper: Name of the scraper
            search_term: Job title or search term
            country: Country/location for the search
            result: Job search results to cache
            **kwargs: Additional search parameters

        Returns:
            bool: True if successfully cached, False otherwise
        """
        # Skip cache if Redis is unhealthy (simple strategy: always cache when Redis available)
        if not self.redis_manager.is_healthy():
            logger.debug("Redis unhealthy, skipping cache storage")
            return False

        # Don't cache empty results
        if not result or len(result) == 0:
            logger.debug("Empty result, skipping cache storage")
            return False

        try:
            # Map time_filter to posting_age for cache key generation
            cache_kwargs = kwargs.copy()
            if "time_filter" in cache_kwargs:
                cache_kwargs["posting_age"] = cache_kwargs.pop("time_filter")
            # Generate cache key
            cache_key = self.key_generator.generate_cache_key(
                scraper=scraper, search_term=search_term, location=country, **cache_kwargs
            )

            # Store in Redis with TTL
            success = self.redis_manager.set_json(key=cache_key, value=result, ttl=self.cache_ttl_seconds)

            if success:
                logger.debug(f"Cached {len(result)} jobs for key: {cache_key} (TTL: {self.cache_ttl_seconds}s)")
                return True
            else:
                logger.warning(f"Failed to cache result for key: {cache_key}")
                return False

        except Exception as e:
            logger.error(f"Error caching result for {scraper}/{search_term}: {e}")
            return False

    def clear_scraper_cache(self, scraper_name: str) -> int:
        """
        Clear all cached results for a specific scraper

        Note: This is a best-effort operation. In a Redis-only strategy,
        we don't maintain a list of all keys, so this is more of a utility
        for development/testing rather than production use.

        Args:
            scraper_name: Name of the scraper to clear cache for

        Returns:
            int: Number of keys cleared (or -1 if not supported)
        """
        if not self.redis_manager.is_healthy():
            logger.debug("Redis unhealthy, cannot clear scraper cache")
            return 0

        try:
            # In a production Redis setup, we'd need to either:
            # 1. Maintain a set of active keys (adds complexity)
            # 2. Use Redis SCAN to find matching keys (expensive)
            # 3. Use Redis key namespacing (requires infrastructure changes)

            # For now, we log this as a limitation and return -1 to indicate
            # that this operation is not fully supported in Redis-only mode
            logger.warning(
                f"Clear scraper cache for '{scraper_name}' requested, but not fully "
                f"supported in Redis-only mode. Keys will naturally expire after "
                f"{self.cache_ttl_seconds} seconds."
            )
            return -1

        except Exception as e:
            logger.error(f"Error clearing scraper cache for {scraper_name}: {e}")
            return 0

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache performance statistics

        Returns:
            Dict[str, Any]: Cache statistics including hit rate, Redis health, etc.
        """
        total_requests = self._cache_stats["total_requests"]
        hits = self._cache_stats["hits"]

        # Calculate hit rate
        hit_rate_percent = (hits / total_requests * 100) if total_requests > 0 else 0.0

        return {
            "cache_type": "redis_only",
            "enabled": self.redis_manager.is_healthy(),  # Simple: enabled when Redis is healthy
            "ttl_seconds": self.cache_ttl_seconds,
            "hits": hits,
            "misses": self._cache_stats["misses"],
            "errors": self._cache_stats["errors"],
            "total_requests": total_requests,
            "hit_rate_percent": round(hit_rate_percent, 2),
            "redis_healthy": self.redis_manager.is_healthy(),
            "redis_connection": self.redis_manager.get_connection_info(),
        }

    def health_check(self) -> bool:
        """
        Perform a health check on the cache system

        Returns:
            bool: True if Redis is healthy, False if there are issues
        """
        return self.redis_manager.is_healthy()

    def force_health_check(self) -> bool:
        """
        Force an immediate health check on Redis

        Returns:
            bool: True if Redis is healthy, False otherwise
        """
        return self.redis_manager.force_health_check()

    def reset_stats(self) -> None:
        """Reset cache statistics (useful for testing)"""
        self._cache_stats = {"hits": 0, "misses": 0, "errors": 0, "total_requests": 0}
        logger.debug("Cache statistics reset")
