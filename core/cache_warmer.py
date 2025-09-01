"""
Production-Ready Cache Warmer

This module provides a cache warming system that integrates
with actual job scrapers to fetch and cache real data for popular searches.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import pandas as pd

from core.global_cache_manager import GlobalCacheManager
from scrapers.optimized_indeed_scraper import OptimizedIndeedScraper

logger = logging.getLogger(__name__)


class CacheWarmer:
    """
    Cache warmer that fetches and caches real job data
    """

    def __init__(self, cache_manager: Optional[GlobalCacheManager] = None, max_workers: int = 3):
        """
        Initialize cache warmer

        Args:
            cache_manager: Global cache manager instance
            max_workers: Maximum number of concurrent workers for warming
        """
        self.cache_manager = cache_manager or GlobalCacheManager()
        self.max_workers = max_workers
        self.scrapers = {
            "indeed": OptimizedIndeedScraper(),
        }

        # Warming statistics
        self._warming_stats = {
            "total_warmed": 0,
            "successful_warms": 0,
            "failed_warms": 0,
            "last_warming_time": 0.0,
            "warming_duration": 0.0,
        }

    def warm_cache_for_popular_searches(
        self, use_analytics: bool = True, max_results_per_search: int = 50, force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Warm cache with real job data for popular searches

        Args:
            use_analytics: Whether to use search analytics for optimization
            max_results_per_search: Maximum results to fetch per search
            force_refresh: Whether to refresh existing cache entries

        Returns:
            Dict: Warming statistics and results
        """
        start_time = time.time()
        logger.info("🔥 Starting cache warming...")

        # Get search combinations to warm
        search_combinations = self._get_search_combinations(use_analytics)

        if not search_combinations:
            logger.warning("No search combinations found for cache warming")
            return self._get_warming_stats()

        logger.info(f"📊 Warming cache for {len(search_combinations)} search combinations")

        # Warm cache using thread pool for concurrent execution
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit warming tasks
            future_to_search = {
                executor.submit(
                    self._warm_single_search, search_params, max_results_per_search, force_refresh
                ): search_params
                for search_params in search_combinations
            }

            # Process completed tasks
            for future in as_completed(future_to_search):
                search_params = future_to_search[future]
                try:
                    result = future.result()
                    if result["success"]:
                        self._warming_stats["successful_warms"] += 1
                        logger.debug(f"✅ Warmed cache for: {search_params}")
                    else:
                        self._warming_stats["failed_warms"] += 1
                        logger.warning(f"❌ Failed to warm cache for: {search_params} - {result['error']}")
                except Exception as e:
                    self._warming_stats["failed_warms"] += 1
                    logger.error(f"❌ Exception warming cache for {search_params}: {e}")

        # Update statistics
        self._warming_stats["total_warmed"] = len(search_combinations)
        self._warming_stats["last_warming_time"] = time.time()
        self._warming_stats["warming_duration"] = time.time() - start_time

        logger.info(f"🎉 Cache warming completed in {self._warming_stats['warming_duration']:.2f}s")
        logger.info(f"   ✅ Successful: {self._warming_stats['successful_warms']}")
        logger.info(f"   ❌ Failed: {self._warming_stats['failed_warms']}")

        return self._get_warming_stats()

    def _get_search_combinations(self, use_analytics: bool) -> List[Dict[str, Any]]:
        """
        Get search combinations to warm based on analytics or predefined list

        Args:
            use_analytics: Whether to use analytics data

        Returns:
            List: Search parameter combinations
        """
        combinations = []

        if use_analytics:
            # Get popular searches from analytics
            popular_searches = self.cache_manager.search_analytics.get_popular_searches(days=30, limit=20)

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

                    combinations.append(
                        {
                            "scraper": scraper,
                            "job_title": job_title,
                            "location": location,
                            "remote": remote,
                            "posting_age": posting_age,
                            "popularity": count,
                        }
                    )

            if combinations:
                logger.info(f"📈 Using analytics data: {len(combinations)} popular searches")
                return combinations

        # Fallback to predefined combinations
        logger.info("📋 Using predefined search combinations")

        for job_title, locations in self.cache_manager._popular_searches.items():
            prioritized_locations = sorted(locations, key=lambda x: x != "Global")

            for location in prioritized_locations:
                for posting_age in self.cache_manager._popular_posting_ages:
                    combinations.append(
                        {
                            "scraper": "indeed",
                            "job_title": job_title,
                            "location": location,
                            "remote": location.lower() == "remote",
                            "posting_age": posting_age,
                            "popularity": 1,  # Default popularity for predefined searches
                        }
                    )

        return combinations

    def _warm_single_search(
        self, search_params: Dict[str, Any], max_results: int, force_refresh: bool
    ) -> Dict[str, Any]:
        """
        Warm cache for a single search combination

        Args:
            search_params: Search parameters
            max_results: Maximum results to fetch
            force_refresh: Whether to refresh existing cache

        Returns:
            Dict: Warming result
        """
        try:
            # Generate cache key
            cache_key = self.cache_manager.generate_cache_key(
                scraper_name=search_params["scraper"],
                job_title=search_params["job_title"],
                location=search_params["location"],
                remote=search_params["remote"],
                posting_age=search_params["posting_age"],
            )

            # Check if already cached (unless force refresh)
            if not force_refresh and self.cache_manager.exists(cache_key):
                logger.debug(f"⏭️  Skipping {cache_key} - already cached")
                return {"success": True, "cached": True, "results_count": 0}

            # Get scraper instance
            scraper_name = search_params["scraper"]
            if scraper_name not in self.scrapers:
                return {"success": False, "error": f"Unsupported scraper: {scraper_name}"}

            scraper = self.scrapers[scraper_name]

            # Convert posting age to hours for scraper
            hours_old = self._convert_posting_age_to_hours(search_params["posting_age"])

            # Prepare search parameters for scraper
            scraper_params = {
                "search_term": search_params["job_title"],
                "location": search_params["location"],
                "results_wanted": max_results,
                "hours_old": hours_old,
            }

            # Add remote filter if needed
            if search_params["remote"]:
                scraper_params["search_term"] = f"{scraper_params['search_term']} remote"

            # Fetch jobs using scraper
            logger.debug(f"🔍 Fetching jobs for: {search_params}")
            search_result = scraper.search_jobs(**scraper_params)

            if not search_result.get("success", False):
                logger.debug(f"📭 Search failed for: {search_params} - {search_result.get('message', 'Unknown error')}")
                return {"success": False, "error": search_result.get("message", "Search failed")}

            jobs_data = search_result.get("jobs", pd.DataFrame())
            if jobs_data.empty:
                logger.debug(f"📭 No jobs found for: {search_params}")
                # Cache empty result to avoid repeated failed searches
                self.cache_manager.set(cache_key, [], {"search_params": search_params})
                return {"success": True, "cached": False, "results_count": 0}

            # Convert jobs to list format for cache
            jobs_list = jobs_data.to_dict("records")

            # Store jobs in cache with metadata
            metadata = {
                "search_params": search_params,
                "cached_at": time.time(),
                "count": len(jobs_list),
            }
            self.cache_manager.set(cache_key, jobs_list, metadata)

            logger.debug(f"💾 Cached {len(jobs_data)} jobs for: {search_params}")
            return {"success": True, "cached": False, "results_count": len(jobs_data)}

        except Exception as e:
            logger.error(f"❌ Error warming cache for {search_params}: {e}")
            return {"success": False, "error": str(e)}

    def _convert_posting_age_to_hours(self, posting_age: str) -> Optional[int]:
        """
        Convert posting age string to hours for scraper

        Args:
            posting_age: Posting age string (e.g., 'Last 24h', 'Last 72h', 'Past Week', 'Past Month')

        Returns:
            Optional[int]: Hours or None for 'Past Month' (no filter)
        """
        # Import here to avoid circular imports
        from utils.time_filters import get_hours_from_filter

        return get_hours_from_filter(posting_age)

    def _get_warming_stats(self) -> Dict[str, Any]:
        """Get current warming statistics"""
        return {
            "warming_stats": self._warming_stats.copy(),
            "cache_stats": self.cache_manager.get_cache_stats(),
            "analytics_summary": self.cache_manager.search_analytics.get_analytics_summary(),
        }

    def get_warming_recommendations(self) -> List[str]:
        """
        Get recommendations for cache warming optimization

        Returns:
            List: Optimization recommendations
        """
        recommendations = []
        stats = self._warming_stats

        # Analyze warming success rate
        if stats["total_warmed"] > 0:
            success_rate = (stats["successful_warms"] / stats["total_warmed"]) * 100
            if success_rate < 80:
                recommendations.append(f"Low warming success rate ({success_rate:.1f}%) - check scraper health")

        # Analyze warming duration
        if stats["warming_duration"] > 300:  # 5 minutes
            recommendations.append("Cache warming taking too long - consider reducing max_workers or max_results")

        # Check cache hit rate
        cache_stats = self.cache_manager.get_cache_stats()
        hit_rate = cache_stats.get("hit_rate_percent", 0)
        if hit_rate < 50:
            recommendations.append("Low cache hit rate - consider expanding warming strategy")

        return recommendations

    def schedule_periodic_warming(self, interval_hours: int = 6) -> None:
        """
        Schedule periodic cache warming

        Args:
            interval_hours: Hours between warming cycles
        """

        def warming_worker() -> None:
            while True:
                try:
                    logger.info("🕐 Scheduled cache warming starting...")
                    self.warm_cache_for_popular_searches(use_analytics=True)
                    logger.info(f"🕐 Scheduled cache warming completed. Next run in {interval_hours} hours")
                except Exception as e:
                    logger.error(f"❌ Scheduled cache warming failed: {e}")

                time.sleep(interval_hours * 3600)  # Convert hours to seconds

        # Start warming worker in background thread
        warming_thread = threading.Thread(target=warming_worker, daemon=True)
        warming_thread.start()
        logger.info(f"🕐 Scheduled cache warming started (every {interval_hours} hours)")

    def start_periodic_warming(self, interval_hours: int = 6) -> None:
        """
        Start periodic cache warming (call this during application startup)

        This method should be called when the application starts to ensure
        cache warming runs automatically in the background.

        Usage:
            # In your main application startup
            cache_warmer = get_cache_warmer()
            cache_warmer.start_periodic_warming(interval_hours=6)

        Args:
            interval_hours: Hours between warming cycles (default: 6 hours)
        """
        logger.info(f"🚀 Starting periodic cache warming service (every {interval_hours} hours)")
        self.schedule_periodic_warming(interval_hours)

        # Also run initial warming immediately
        logger.info("🔥 Running initial cache warming...")
        initial_results = self.warm_cache_for_popular_searches(use_analytics=True)
        successful_warms = initial_results["warming_stats"]["successful_warms"]
        logger.info(f"✅ Initial cache warming completed: {successful_warms} successful")


def get_cache_warmer() -> CacheWarmer:
    """
    Get a configured cache warmer instance

    Returns:
        CacheWarmer: Configured cache warmer
    """
    return CacheWarmer(max_workers=3)


def start_cache_warming_service(interval_hours: int = 6) -> CacheWarmer:
    """
    Start the cache warming service

    Usage:
        # In your main application startup (e.g., dashboard.py)
        from core.cache_warmer import start_cache_warming_service

        # Start cache warming service
        cache_warmer = start_cache_warming_service(interval_hours=6)

        # Your application continues...

    Args:
        interval_hours: Hours between warming cycles (default: 6 hours)

    Returns:
        CacheWarmer: The cache warmer instance for manual control
    """
    cache_warmer = get_cache_warmer()
    cache_warmer.start_periodic_warming(interval_hours)
    return cache_warmer
