"""
Cache Warming Manager for Jobs Dashboard

This module manages background cache warming based on analytics data and configuration.
It runs independently of the dashboard to avoid startup delays and can be scheduled
via cron, systemd, or container orchestration.

Features:
- Background cache warming based on analytics
- Configurable job titles, locations, and posting ages
- Automatic cache warming updates based on popular searches
- Integration with existing Redis and file cache systems
"""

import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from core.global_cache_manager import GlobalCacheManager
from core.lightweight_analytics import LightweightAnalytics
from scrapers.optimized_indeed_scraper import OptimizedIndeedScraper

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CacheWarmingManager:
    """
    Manages background cache warming based on analytics and configuration.

    This class runs independently of the dashboard to avoid startup delays.
    It can be scheduled via cron, systemd, or container orchestration.
    """

    def __init__(self, config_path: str = "config/initial_popular_searches.json"):
        """
        Initialize the cache warming manager.

        Args:
            config_path: Path to cache warming configuration file
        """
        self.config_path = Path(config_path)
        self.analytics = LightweightAnalytics()
        self.cache_manager = GlobalCacheManager()
        self.config = self._load_config()

        # Thread safety
        self._lock = threading.Lock()
        self._is_running = False

        logger.info("Cache warming manager initialized")

    def _load_config(self) -> Dict[str, Any]:
        """Load cache warming configuration from JSON file."""
        try:
            if not self.config_path.exists():
                logger.warning(f"Config file not found: {self.config_path}")
                return self._get_default_config()

            with open(self.config_path, "r", encoding="utf-8") as f:
                config = json.load(f)

            logger.info(f"Loaded cache warming config: {config.get('cache_warming', {}).get('enabled', False)}")
            return config if isinstance(config, dict) else self._get_default_config()

        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return self._get_default_config()

    def _get_default_config(self) -> Dict[str, Any]:
        """Get default cache warming configuration."""
        return {
            "cache_warming": {
                "enabled": True,
                "refresh_interval_hours": 168,
                "max_results_per_search": 50,
                "always_remote": True,
                "platforms": ["indeed"],
                "locations": ["Global", "Brazil"],
                "posting_ages": ["Last 24h", "Last 72h", "Past Week"],
                "job_titles": [
                    "Software Engineer",
                    "Data Scientist",
                    "Product Manager",
                    "DevOps Engineer",
                    "Frontend Developer",
                ],
            }
        }

    def update_config_from_analytics(self) -> None:
        """
        Update cache warming configuration based on analytics data.

        This method analyzes recent search patterns and updates the configuration
        to focus on the most popular searches.
        """
        try:
            with self._lock:
                # Get popular searches from last 7 days
                popular_data = self.analytics.get_popular_searches(days=7)

                if popular_data["total_searches"] == 0:
                    logger.info("No search data available for config update")
                    return

                # Get top job titles (up to 10)
                popular_job_titles = [job_title for job_title, _ in popular_data["popular_job_titles"][:10]]

                # Get top locations (up to 5)
                popular_locations = [location for location, _ in popular_data["popular_locations"][:5]]

                # Update configuration
                if popular_job_titles:
                    self.config["cache_warming"]["job_titles"] = popular_job_titles

                if popular_locations:
                    # Always include "Global" if it's popular
                    if "Global" in popular_locations:
                        popular_locations.remove("Global")
                        popular_locations = ["Global"] + popular_locations[:4]
                    else:
                        popular_locations = ["Global"] + popular_locations[:4]

                    self.config["cache_warming"]["locations"] = popular_locations

                # Save updated configuration
                self._save_config()

                logger.info(
                    f"Updated config with {len(popular_job_titles)} job titles and {len(popular_locations)} locations"
                )

        except Exception as e:
            logger.error(f"Failed to update config from analytics: {e}")

    def _save_config(self) -> None:
        """Save current configuration to file."""
        try:
            with open(self.config_path, "w", encoding="utf-8") as f:
                json.dump(self.config, f, indent=2, ensure_ascii=False)

            logger.debug("Configuration saved successfully")

        except Exception as e:
            logger.error(f"Failed to save configuration: {e}")

    def warm_cache(self) -> Dict[str, Any]:
        """
        Execute cache warming based on current configuration.

        Returns:
            Dictionary with warming results and statistics
        """
        if self._is_running:
            logger.warning("Cache warming already in progress")
            return {"status": "already_running"}

        try:
            with self._lock:
                self._is_running = True

            start_time = time.time()
            config = self.config.get("cache_warming", {})

            if not config.get("enabled", False):
                logger.info("Cache warming is disabled")
                return {"status": "disabled"}

            # Get configuration parameters
            job_titles = config.get("job_titles", [])
            platforms = config.get("platforms", ["indeed"])
            locations = config.get("locations", ["Global"])
            posting_ages = config.get("posting_ages", ["Last 24h"])
            max_results = config.get("max_results_per_search", 50)
            always_remote = config.get("always_remote", True)

            logger.info(f"Starting cache warming for {len(job_titles)} job titles")

            # Initialize scrapers for different platforms
            scrapers = {}
            for platform in platforms:
                if platform == "indeed":
                    scrapers[platform] = OptimizedIndeedScraper()
                else:
                    logger.warning(f"Unknown platform {platform}, skipping")
                    continue

            # Track results
            total_searches = 0
            successful_searches = 0
            cached_results = 0

            # Execute searches for each combination
            for platform in platforms:
                for job_title in job_titles:
                    for location in locations:
                        for posting_age in posting_ages:
                            try:
                                # Check if already cached
                                job_key = job_title.lower().replace(" ", "_")
                                location_key = location.lower()
                                age_key = posting_age.lower().replace(" ", "_")
                                cache_key = f"{platform}_{job_key}_{location_key}_{age_key}"

                                if self.cache_manager.get(cache_key):
                                    cached_results += 1
                                    logger.debug(f"Already cached: {job_title} in {location} ({posting_age})")
                                    continue

                                # Execute search
                                logger.info(f"Warming cache: {job_title} in {location} ({posting_age})")

                                # Convert posting age to scraper format
                                age_filter = self._convert_posting_age(posting_age)

                                # Execute search with platform-specific scraper
                                if platform not in scrapers:
                                    logger.warning(f"No scraper available for platform {platform}, skipping")
                                    continue

                                scraper = scrapers[platform]
                                search_response = scraper.search_jobs(
                                    query=job_title,
                                    location=location,
                                    remote_only=always_remote,
                                    posting_age=age_filter,
                                    max_results=max_results,
                                )

                                if (
                                    search_response
                                    and isinstance(search_response, dict)
                                    and search_response.get("jobs")
                                ):
                                    job_results = search_response["jobs"]
                                    # Cache results
                                    self.cache_manager.set(
                                        cache_key,
                                        job_results,
                                        metadata={
                                            "search_term": job_title,
                                            "location": location,
                                            "posting_age": posting_age,
                                            "is_remote": always_remote,
                                            "platform": platform,
                                            "cache_warmed": True,
                                            "warmed_at": datetime.now().isoformat(),
                                        },
                                    )

                                    successful_searches += 1
                                    logger.info(
                                        f"Successfully warmed cache: {job_title} in "
                                        f"{location} ({posting_age}) - {len(job_results)} jobs"
                                    )
                                else:
                                    logger.warning(f"No results for: {job_title} in {location} ({posting_age})")

                                total_searches += 1

                                # Rate limiting between searches
                                time.sleep(2)

                            except Exception as e:
                                logger.error(f"Failed to warm cache for {job_title} in {location} ({posting_age}): {e}")
                                total_searches += 1

            # Calculate statistics
            execution_time = time.time() - start_time
            success_rate = (successful_searches / total_searches * 100) if total_searches > 0 else 0

            results = {
                "status": "completed",
                "execution_time_seconds": round(execution_time, 2),
                "total_searches": total_searches,
                "successful_searches": successful_searches,
                "cached_results": cached_results,
                "success_rate_percent": round(success_rate, 2),
                "timestamp": datetime.now().isoformat(),
            }

            logger.info(
                f"Cache warming completed: {successful_searches}/{total_searches} successful ({success_rate:.1f}%)"
            )
            return results

        except Exception as e:
            logger.error(f"Cache warming failed: {e}")
            return {"status": "failed", "error": str(e)}

        finally:
            with self._lock:
                self._is_running = False

    def _convert_posting_age(self, posting_age: str) -> str:
        """Convert human-readable posting age to scraper format."""
        age_mapping = {"Last 24h": "1d", "Last 72h": "3d", "Past Week": "7d"}
        return age_mapping.get(posting_age, "1d")

    def get_warming_status(self) -> Dict[str, Any]:
        """Get current cache warming status and statistics."""
        try:
            # Check if warming is running
            is_running = self._is_running

            # Get configuration info
            config = self.config.get("cache_warming", {})
            enabled = config.get("enabled", False)

            # Get cache statistics
            cache_stats = self.cache_manager.get_cache_stats()

            # Get analytics statistics
            analytics_stats = self.analytics.get_storage_stats()

            return {
                "is_running": is_running,
                "enabled": enabled,
                "config": {
                    "job_titles_count": len(config.get("job_titles", [])),
                    "locations_count": len(config.get("locations", [])),
                    "posting_ages_count": len(config.get("posting_ages", [])),
                    "refresh_interval_hours": config.get("refresh_interval_hours", 168),
                },
                "cache_stats": cache_stats,
                "analytics_stats": analytics_stats,
                "last_updated": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to get warming status: {e}")
            return {"error": str(e)}

    def run_scheduled_warming(self) -> None:
        """
        Run scheduled cache warming with analytics-based configuration updates.

        This method is designed to be called by cron, systemd, or container
        orchestration systems.
        """
        try:
            logger.info("Starting scheduled cache warming")

            # Step 1: Update configuration from analytics
            self.update_config_from_analytics()

            # Step 2: Execute cache warming
            results = self.warm_cache()

            # Step 3: Log results
            if results.get("status") == "completed":
                logger.info(
                    f"Scheduled warming completed successfully: {results['successful_searches']} searches warmed"
                )
            else:
                logger.error(f"Scheduled warming failed: {results}")

        except Exception as e:
            logger.error(f"Scheduled warming failed: {e}")


# Convenience function for manual cache warming
def warm_cache_manual() -> Dict[str, Any]:
    """
    Manual cache warming function for testing and development.

    Returns:
        Dictionary with warming results
    """
    manager = CacheWarmingManager()
    return manager.warm_cache()


# Convenience function for scheduled warming
def run_scheduled_warming() -> None:
    """
    Run scheduled cache warming (for cron/systemd integration).
    """
    manager = CacheWarmingManager()
    manager.run_scheduled_warming()


if __name__ == "__main__":
    # Test cache warming
    print("Testing cache warming manager...")
    manager = CacheWarmingManager()

    # Get status
    status = manager.get_warming_status()
    print(f"Status: {status}")

    # Run warming
    results = manager.warm_cache()
    print(f"Results: {results}")
