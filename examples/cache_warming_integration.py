#!/usr/bin/env python3
"""
Example: Cache Warming Integration

This file shows how to integrate cache warming into your main application.
Add this to your dashboard.py or main application startup.
"""

import logging
import os
from typing import Optional

# Import cache warming functionality
from core.cache_warmer import start_cache_warming_service
from core.global_cache_manager import GlobalCacheManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_cache_warming() -> Optional[GlobalCacheManager]:
    """
    Initialize cache warming service during application startup

    This function should be called when your application starts to enable
    automatic cache warming in the background.

    Returns:
        GlobalCacheManager: Cache manager instance for manual control
    """
    try:
        # Check if cache warming is enabled via environment variable
        enable_cache_warming = os.getenv("ENABLE_CACHE_WARMING", "true").lower() == "true"

        if not enable_cache_warming:
            logger.info("🔄 Cache warming disabled via ENABLE_CACHE_WARMING environment variable")
            return None

        # Get warming interval from environment variable (default: 6 hours)
        warming_interval = int(os.getenv("CACHE_WARMING_INTERVAL_HOURS", "6"))

        logger.info(f"🚀 Initializing cache warming service (interval: {warming_interval} hours)")

        # Start the cache warming service
        cache_warmer = start_cache_warming_service(interval_hours=warming_interval)

        # Get the cache manager instance for manual control
        cache_manager = cache_warmer.cache_manager

        logger.info("✅ Cache warming service initialized successfully")
        return cache_manager

    except Exception as e:
        logger.error(f"❌ Failed to initialize cache warming service: {e}")
        return None


def manual_cache_warming_example() -> None:
    """
    Example of manual cache warming for specific searches
    """
    try:
        from core.cache_warmer import get_cache_warmer

        # Get cache warmer instance
        warmer = get_cache_warmer()

        # Manual warming for specific searches
        logger.info("🔥 Running manual cache warming...")

        # Warm cache for specific job titles and locations
        manual_searches = [
            ("software_engineer", "Global", False, "24h"),
            ("data_scientist", "San Francisco", False, "72h"),
            ("product_manager", "Remote", True, "7d"),
        ]

        for job_title, location, remote, posting_age in manual_searches:
            cache_key = warmer.cache_manager.generate_cache_key(
                scraper_name="indeed", job_title=job_title, location=location, remote=remote, posting_age=posting_age
            )

            # Check if already cached
            search_info = f"{job_title} in {location} ({posting_age})"
            if not warmer.cache_manager.exists(cache_key):
                logger.info(f"🔥 Warming cache for: {search_info}")
                search_params = {
                    "scraper": "indeed",
                    "job_title": job_title,
                    "location": location,
                    "remote": remote,
                    "posting_age": posting_age,
                }
                result = warmer._warm_single_search(search_params, max_results=20, force_refresh=False)
                if result["success"]:
                    logger.info(f"✅ Successfully warmed cache for: {search_info}")
                else:
                    logger.warning(f"❌ Failed to warm cache for: {search_info}")
            else:
                logger.info(f"⏭️  Cache already exists for: {search_info}")

    except Exception as e:
        logger.error(f"❌ Manual cache warming failed: {e}")


def get_cache_statistics() -> None:
    """
    Example of getting cache statistics and insights
    """
    try:
        from core.cache_warmer import get_cache_warmer

        warmer = get_cache_warmer()
        cache_manager = warmer.cache_manager

        # Get cache statistics
        stats = cache_manager.get_cache_stats()
        insights = cache_manager.get_cache_optimization_insights()

        print("\n📊 Cache Statistics:")
        print(f"  Cache hits: {stats['cache_hits']}")
        print(f"  Cache misses: {stats['cache_misses']}")
        print(f"  Hit rate: {stats['hit_rate_percent']}%")
        print(f"  Redis healthy: {stats['redis_healthy']}")
        print(f"  File cache size: {stats['cache_efficiency']['file_cache_size_mb']} MB")

        print("\n🎯 Cache Optimization Insights:")
        for recommendation in insights["recommendations"]:
            print(f"  • {recommendation}")

        print("\n📈 Search Analytics:")
        analytics = cache_manager.search_analytics.get_analytics_summary()
        print(f"  Total searches: {analytics['total_searches']}")
        print(f"  Today's searches: {analytics['today_searches']}")
        print(f"  Popular job titles: {analytics['popular_job_titles'][:3]}")

    except Exception as e:
        logger.error(f"❌ Failed to get cache statistics: {e}")


if __name__ == "__main__":
    """
    Example usage of cache warming integration
    """
    print("🚀 Cache Warming Integration Example")
    print("=" * 50)

    # Initialize cache warming service
    cache_manager = initialize_cache_warming()

    if cache_manager:
        print("✅ Cache warming service started successfully")

        # Show cache statistics
        get_cache_statistics()

        # Example of manual warming
        print("\n🔥 Manual Cache Warming Example:")
        manual_cache_warming_example()

    else:
        print("❌ Failed to start cache warming service")

    print("\n✅ Example completed!")
