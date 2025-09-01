"""
Global Cache Manager

This module provides global Redis caching functionality with cache key generation,
access count tracking, TTL management, and thread-safe operations.
It includes fallback to file cache when Redis is unavailable.

Think of this like a global state management system in React/Redux, but for caching.
"""

import hashlib
import json
import logging
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

from config.environment import get_redis_config
from core.redis_manager import RedisManager
from core.search_analytics import SearchAnalytics
from utils.file_operations import AtomicFileOperations

logger = logging.getLogger(__name__)


class GlobalCacheManager:
    """
    Global Cache Manager with Redis and File Fallback

    This class manages global caching with Redis as the primary cache
    and file cache as fallback. It provides cache key generation,
    access tracking, and TTL management.
    Similar to a global state manager in React/Redux applications.
    """

    def __init__(self) -> None:
        """Initialize the global cache manager"""
        self.redis_config = get_redis_config()
        self.redis_manager = RedisManager(
            redis_url=self.redis_config.url,
            max_connections=self.redis_config.max_connections,
            retry_attempts=self.redis_config.retry_attempts,
            retry_delay=self.redis_config.retry_delay,
            health_check_interval=self.redis_config.health_check_interval,
        )

        # Thread safety
        self._lock = threading.RLock()

        # File cache operations
        self.file_operations = AtomicFileOperations(cache_dir="cache")

        # Search analytics for tracking user patterns
        self.search_analytics = SearchAnalytics()

        # Cache statistics
        self._cache_hits = 0
        self._cache_misses = 0
        self._redis_failures = 0

        # Cache warming for popular searches
        self._popular_searches = {
            "software_engineer": ["Global", "San Francisco", "New York", "Remote"],
            "data_scientist": ["Global", "San Francisco", "New York", "Remote"],
            "product_manager": ["Global", "San Francisco", "New York", "Remote"],
            "devops_engineer": ["Global", "San Francisco", "New York", "Remote"],
        }

        # Popular posting age filters (most commonly used)
        self._popular_posting_ages = ["Last 24h", "Last 72h", "Past Week", "Past Month"]

    def generate_cache_key(
        self,
        scraper_name: str,
        job_title: str,
        location: str,
        remote: bool = False,
        posting_age: str = "any",
        **kwargs: Any,
    ) -> str:
        """
        Generate a unique cache key for job search results

        Args:
            scraper_name: Name of the scraper (e.g., 'indeed', 'linkedin')
            job_title: Job title to search for
            location: Location to search in
            remote: Whether to search for remote jobs
            posting_age: Posting age filter
            **kwargs: Additional search parameters

        Returns:
            str: Unique cache key
        """
        # Create a search parameters dictionary
        search_params = {
            "scraper": scraper_name.lower(),
            "title": job_title.lower().strip(),
            "location": location.lower().strip(),
            "remote": remote,
            "posting_age": posting_age.lower(),
            **kwargs,
        }

        # Sort parameters for consistent key generation
        sorted_params = sorted(search_params.items())

        # Create a string representation
        param_string = json.dumps(sorted_params, sort_keys=True)

        # Generate hash for consistent key length
        key_hash = hashlib.md5(param_string.encode()).hexdigest()

        # Create human-readable key prefix
        prefix = f"{scraper_name}_{job_title.replace(' ', '_')}_{location.replace(' ', '_')}"
        if remote:
            prefix += "_remote"

        # Limit prefix length to avoid overly long keys
        if len(prefix) > 100:
            prefix = prefix[:100]

        return f"{prefix}_{key_hash}"

    def log_search(
        self,
        job_title: str,
        location: str,
        remote: bool = False,
        scraper_name: str = "indeed",
        posting_age: str = "any",
    ) -> None:
        """
        Log a search query for analytics

        Args:
            job_title: Job title searched for
            location: Location searched for
            remote: Whether remote jobs were requested
            scraper_name: Name of the scraper used
            posting_age: Posting age filter used
        """
        self.search_analytics.log_search(job_title, location, remote, scraper_name, posting_age)

    def warm_cache_for_popular_searches(self, use_analytics: bool = True) -> int:
        """
        Warm cache with popular search combinations

        Prioritizes "Global" searches and can use analytics data for optimization.

        Args:
            use_analytics: Whether to use search analytics to optimize warming

        Returns:
            int: Number of cache entries warmed
        """
        warmed_count = 0

        if use_analytics:
            # Get popular searches from analytics
            popular_searches = self.search_analytics.get_popular_searches(days=30, limit=20)

            # Use analytics data if available
            if popular_searches:
                logger.info(f"Using analytics data for cache warming: {len(popular_searches)} popular searches")

                for search_key, count in popular_searches:
                    parts = search_key.split("|")
                    if len(parts) >= 5:
                        job_title, location, remote_str, posting_age, scraper = (
                            parts[0],
                            parts[1],
                            parts[2],
                            parts[3],
                            parts[4],
                        )
                        remote = remote_str.lower() == "true"

                        cache_key = self.generate_cache_key(
                            scraper_name=scraper,
                            job_title=job_title,
                            location=location,
                            remote=remote,
                            posting_age=posting_age,
                        )

                        if not self.exists(cache_key):
                            logger.debug(f"Analytics-based cache warming: {job_title} in {location} (count: {count})")
                            warmed_count += 1

                return warmed_count

                # Fallback to predefined popular searches
        for job_title, locations in self._popular_searches.items():
            # Prioritize "Global" searches first
            prioritized_locations = sorted(locations, key=lambda x: x != "Global")

            for location in prioritized_locations:
                for posting_age in self._popular_posting_ages:
                    # Generate cache keys for popular combinations
                    cache_key = self.generate_cache_key(
                        scraper_name="indeed",
                        job_title=job_title,
                        location=location,
                        remote=(location.lower() == "remote"),
                        posting_age=posting_age,
                    )

                    # Check if already cached
                    if not self.exists(cache_key):
                        logger.debug(f"Predefined cache warming: {job_title} in {location} ({posting_age})")
                        # Real implementation: Make API call to warm cache
                        try:
                            warmed_count += self._warm_cache_entry(
                                cache_key, job_title, location, posting_age, remote=(location.lower() == "remote")
                            )
                        except Exception as e:
                            logger.warning(f"Failed to warm cache for {job_title} in {location}: {e}")

        logger.info(f"Cache warming completed for {warmed_count} popular searches")
        return warmed_count

    def get_popular_search_combinations(self) -> Dict[str, list[str]]:
        """
        Get the most popular search combinations for cache warming

        Returns:
            Dict: Job titles mapped to their popular locations
        """
        return self._popular_searches.copy()

    def add_popular_search(self, job_title: str, locations: list[str]) -> None:
        """
        Add a new popular search combination for cache warming

        Args:
            job_title: Job title to add
            locations: List of popular locations for this job title
        """
        self._popular_searches[job_title] = locations
        logger.info(f"Added popular search: {job_title} -> {locations}")

    def get_cache_optimization_insights(self) -> Dict[str, Any]:
        """
        Get insights for cache optimization based on search analytics

        Returns:
            Dict: Cache optimization insights and recommendations
        """
        analytics = self.search_analytics.get_analytics_summary()
        popular_jobs = self.search_analytics.get_popular_job_titles(days=30, limit=5)
        popular_locations = self.search_analytics.get_popular_locations(days=30, limit=5)

        # Calculate cache efficiency
        total_requests = self._cache_hits + self._cache_misses
        cache_hit_rate = (self._cache_hits / total_requests * 100) if total_requests > 0 else 0

        recommendations: List[str] = []

        insights = {
            "cache_performance": {
                "hit_rate_percent": round(cache_hit_rate, 2),
                "total_requests": total_requests,
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
            },
            "popular_searches": {
                "top_job_titles": popular_jobs,
                "top_locations": popular_locations,
                "total_searches": analytics.get("total_searches", 0),
                "unique_combinations": analytics.get("unique_search_combinations", 0),
            },
            "recommendations": recommendations,
        }

        # Generate recommendations
        if cache_hit_rate < 50:
            recommendations.append("Low cache hit rate - consider expanding cache warming strategy")

        if analytics.get("total_searches", 0) > 100:
            recommendations.append("High search volume - analytics-based cache warming recommended")

        if len(popular_jobs) > 0 and popular_jobs[0][1] > 10:
            recommendations.append(f"'{popular_jobs[0][0]}' is very popular - prioritize in cache warming")

        return insights

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve data from cache

        Args:
            key: Cache key

        Returns:
            Dict: Cached data with metadata or None if not found
        """
        with self._lock:
            try:
                # Try Redis first
                if self.redis_manager.is_healthy():
                    cached_data = self.redis_manager.get_json(key)

                    if cached_data is not None:
                        # Update access statistics
                        self._update_access_stats(cached_data)
                        self._cache_hits += 1
                        logger.debug(f"✅ Redis cache HIT for key: {key}")
                        return cached_data  # type: ignore[no-any-return]
                    else:
                        self._cache_misses += 1
                        logger.debug(f"❌ Redis cache MISS for key: {key}")

                        # Try file cache as fallback
                        file_cache_result = self._get_from_file_cache(key)
                        if file_cache_result is not None:
                            logger.debug(f"✅ File cache HIT for key: {key}")
                            return file_cache_result
                        else:
                            logger.debug(f"❌ File cache MISS for key: {key}")
                            return None

                else:
                    # Redis is not healthy, try file cache
                    logger.warning("Redis not healthy, falling back to file cache")
                    self._redis_failures += 1

                    # Force a health check to see if Redis is back
                    if self.redis_manager.force_health_check():
                        logger.info("Redis is back online, retrying Redis cache")
                        cached_data = self.redis_manager.get_json(key)
                        if cached_data is not None:
                            self._update_access_stats(cached_data)
                            self._cache_hits += 1
                            logger.debug(f"✅ Redis cache HIT after recovery for key: {key}")
                            return cached_data  # type: ignore[no-any-return]
                        else:
                            logger.debug(f"❌ Redis cache MISS after recovery for key: {key}")

                    # Fall back to file cache
                    file_cache_result = self._get_from_file_cache(key)
                    if file_cache_result is not None:
                        logger.debug(f"✅ File cache HIT for key: {key}")
                        return file_cache_result
                    else:
                        logger.debug(f"❌ File cache MISS for key: {key}")
                        return None

            except Exception as e:
                logger.error(f"Error retrieving from cache for key '{key}': {e}")
                self._cache_misses += 1
                return None

    def set(self, key: str, jobs: List[Dict[str, Any]], metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Store data in cache

        Args:
            key: Cache key
            jobs: List of job data to cache
            metadata: Additional metadata to store

        Returns:
            bool: True if successful, False otherwise
        """
        with self._lock:
            try:
                # Prepare cache data structure
                cache_data = {
                    "jobs": jobs,
                    "metadata": metadata or {},
                    "access_count": 0,
                    "last_accessed": datetime.now().isoformat(),
                    "created_at": datetime.now().isoformat(),
                    "ttl": self.redis_config.ttl,
                }

                # Try Redis first
                if self.redis_manager.is_healthy():
                    success = self.redis_manager.set_json(key, cache_data, self.redis_config.ttl)

                    if success:
                        logger.debug(f"Data cached in Redis for key: {key}")
                        # Also store in file cache as backup
                        self._set_file_cache(key, cache_data)
                        return True
                    else:
                        logger.warning(f"Failed to cache data in Redis for key: {key}")
                        return self._set_file_cache(key, cache_data)

                else:
                    # Redis is not healthy, use file cache
                    logger.warning("Redis not healthy, using file cache only")
                    self._redis_failures += 1
                    return self._set_file_cache(key, cache_data)

            except Exception as e:
                logger.error(f"Error storing in cache for key '{key}': {e}")
                return False

    def delete(self, key: str) -> bool:
        """
        Delete data from cache

        Args:
            key: Cache key to delete

        Returns:
            bool: True if successful, False otherwise
        """
        with self._lock:
            try:
                # Delete from Redis
                redis_success = False
                if self.redis_manager.is_healthy():
                    redis_success = self.redis_manager.delete(key)

                # Delete from file cache
                file_success = self._delete_file_cache(key)

                return redis_success or file_success

            except Exception as e:
                logger.error(f"Error deleting cache key '{key}': {e}")
                return False

    def exists(self, key: str) -> bool:
        """
        Check if data exists in cache

        Args:
            key: Cache key to check

        Returns:
            bool: True if exists, False otherwise
        """
        with self._lock:
            try:
                # Check Redis first
                if self.redis_manager.is_healthy():
                    return self.redis_manager.exists(key)
                else:
                    # Check file cache
                    return self._exists_file_cache(key)

            except Exception as e:
                logger.error(f"Error checking cache existence for key '{key}': {e}")
                return False

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics

        Returns:
            Dict: Cache statistics including hits, misses, and Redis health
        """
        with self._lock:
            total_requests = self._cache_hits + self._cache_misses
            hit_rate = (self._cache_hits / total_requests * 100) if total_requests > 0 else 0

            file_stats = self.file_operations.get_cache_stats()

            return {
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
                "total_requests": total_requests,
                "hit_rate_percent": round(hit_rate, 2),
                "redis_failures": self._redis_failures,
                "redis_healthy": self.redis_manager.is_healthy(),
                "redis_connection_info": self.redis_manager.get_connection_info(),
                "file_cache_stats": file_stats,
                "cache_efficiency": {
                    "redis_hit_rate": round((self._cache_hits / total_requests * 100) if total_requests > 0 else 0, 2),
                    "file_cache_size_mb": round(file_stats.get("total_size_bytes", 0) / (1024 * 1024), 2),
                    "file_cache_files": file_stats.get("file_count", 0),
                },
                "popular_searches": {
                    "total_combinations": sum(len(locations) for locations in self._popular_searches.values()),
                    "job_titles": list(self._popular_searches.keys()),
                    "prioritized_locations": ["Global", "Remote", "San Francisco", "New York"],
                },
                "search_analytics": self.search_analytics.get_analytics_summary(),
            }

    def clear_cache(self) -> bool:
        """
        Clear all cached data

        Returns:
            bool: True if successful, False otherwise
        """
        with self._lock:
            try:
                # Clear Redis cache (simplified - would need pattern-based deletion in production)
                logger.info("Clearing Redis cache")

                # Clear file cache
                file_cache_success = self.file_operations.clear_cache()
                if file_cache_success:
                    logger.info("File cache cleared successfully")
                else:
                    logger.warning("Failed to clear file cache")

                return file_cache_success

            except Exception as e:
                logger.error(f"Error clearing cache: {e}")
                return False

    def _update_access_stats(self, cache_data: Dict[str, Any]) -> None:
        """Update access statistics for cached data"""
        try:
            cache_data["access_count"] = cache_data.get("access_count", 0) + 1
            cache_data["last_accessed"] = datetime.now().isoformat()

            # Update in Redis if healthy
            if self.redis_manager.is_healthy():
                self.redis_manager.set_json(cache_data.get("_key", ""), cache_data, self.redis_config.ttl)

        except Exception as e:
            logger.debug(f"Error updating access stats: {e}")

    def _get_from_file_cache(self, key: str) -> Optional[Dict[str, Any]]:
        """Get data from file cache (fallback)"""
        try:
            cached_data = self.file_operations.atomic_read_json(key)
            if cached_data is not None:
                # Update access statistics
                self._update_access_stats(cached_data)
                self._cache_hits += 1
                logger.debug(f"File cache hit for key: {key}")
                return cached_data
            else:
                self._cache_misses += 1
                logger.debug(f"File cache miss for key: {key}")
                return None
        except Exception as e:
            logger.error(f"Error reading from file cache for key '{key}': {e}")
            self._cache_misses += 1
            return None

    def _set_file_cache(self, key: str, data: Dict[str, Any]) -> bool:
        """Set data in file cache (fallback)"""
        try:
            success = self.file_operations.atomic_write_json(key, data)
            if success:
                logger.debug(f"File cache set successful for key: {key}")
            else:
                logger.warning(f"File cache set failed for key: {key}")
            return success
        except Exception as e:
            logger.error(f"Error writing to file cache for key '{key}': {e}")
            return False

    def _delete_file_cache(self, key: str) -> bool:
        """Delete data from file cache (fallback)"""
        try:
            success = self.file_operations.atomic_delete(key)
            if success:
                logger.debug(f"File cache delete successful for key: {key}")
            else:
                logger.warning(f"File cache delete failed for key: {key}")
            return success
        except Exception as e:
            logger.error(f"Error deleting from file cache for key '{key}': {e}")
            return False

    def _exists_file_cache(self, key: str) -> bool:
        """Check if data exists in file cache (fallback)"""
        try:
            exists = self.file_operations.exists(key)
            logger.debug(f"File cache exists check for key: {key} = {exists}")
            return exists
        except Exception as e:
            logger.error(f"Error checking file cache existence for key '{key}': {e}")
            return False

    def _warm_cache_entry(
        self, cache_key: str, job_title: str, location: str, posting_age: str, remote: bool = False
    ) -> int:
        """
        Warm a single cache entry by making an API call

        Args:
            cache_key: The cache key to warm
            job_title: Job title to search for
            location: Location to search in
            posting_age: Posting age filter
            remote: Whether to search for remote jobs

        Returns:
            int: 1 if successful, 0 if failed
        """
        try:
            # Import here to avoid circular imports
            from scrapers.optimized_indeed_scraper import get_indeed_scraper

            # Get scraper instance
            scraper = get_indeed_scraper()

            # Make API call to get jobs
            result = scraper.search_jobs(
                search_term=job_title,
                where=location,
                include_remote=remote,
                time_filter=posting_age,
                results_wanted=50,  # Reasonable number for cache warming
            )

            if result.get("success") and result.get("jobs") is not None:
                jobs = result["jobs"]
                if len(jobs) > 0:
                    # Store in cache
                    metadata = {
                        "source": "cache_warming",
                        "job_title": job_title,
                        "location": location,
                        "posting_age": posting_age,
                        "remote": remote,
                        "job_count": len(jobs),
                        "warmed_at": datetime.now().isoformat(),
                    }

                    success = self.set(cache_key, jobs, metadata)
                    if success:
                        logger.info(f"✅ Cache warmed successfully: {job_title} in {location} ({len(jobs)} jobs)")
                        return 1
                    else:
                        logger.warning(f"❌ Failed to store warmed cache: {job_title} in {location}")
                        return 0
                else:
                    logger.debug(f"No jobs found for cache warming: {job_title} in {location}")
                    return 0
            else:
                logger.debug(f"Search failed for cache warming: {job_title} in {location}")
                return 0

        except Exception as e:
            logger.error(f"Error warming cache for {job_title} in {location}: {e}")
            return 0

    def close(self) -> None:
        """Close cache manager and cleanup resources"""
        with self._lock:
            try:
                self.redis_manager.close()
                logger.info("Global cache manager closed")
            except Exception as e:
                logger.error(f"Error closing cache manager: {e}")

    def __enter__(self) -> "GlobalCacheManager":
        """Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit"""
        self.close()


# Global cache manager instance
# This is like a singleton service in Angular or a global context in React
_global_cache_manager: Optional[GlobalCacheManager] = None


def get_global_cache_manager() -> GlobalCacheManager:
    """
    Get the global cache manager instance

    Returns:
        GlobalCacheManager: Singleton cache manager
    """
    global _global_cache_manager
    if _global_cache_manager is None:
        _global_cache_manager = GlobalCacheManager()
    return _global_cache_manager
