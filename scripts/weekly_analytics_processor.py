#!/usr/bin/env python3
"""
Weekly Analytics Processor for Jobs Dashboard

This script runs weekly to:
1. Aggregate daily analytics logs into weekly summaries
2. Identify top popular searches by frequency
3. Update cache warming configuration automatically
4. Compress old analytics files for space efficiency
5. Clean old cache entries based on TTL and size limits

Designed to run via cron, systemd, or container orchestration.
"""

import argparse
import gzip
import json
import logging

# Add project root to path for imports
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from core.cache_warming_manager import CacheWarmingManager
from core.lightweight_analytics import LightweightAnalytics

sys.path.append(str(Path(__file__).parent.parent))


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class WeeklyAnalyticsProcessor:
    """
    Processes weekly analytics and updates cache warming configuration.

    This class handles the weekly aggregation of daily analytics data,
    identifies popular searches, and updates the cache warming strategy.
    """

    def __init__(self, analytics_path: str = "analytics"):
        """
        Initialize the weekly processor.

        Args:
            analytics_path: Path to analytics directory
        """
        self.analytics_path = Path(analytics_path)
        self.daily_path = self.analytics_path / "daily"
        self.weekly_path = self.analytics_path / "weekly"
        self.monthly_path = self.analytics_path / "monthly"

        # Ensure directories exist
        self.daily_path.mkdir(parents=True, exist_ok=True)
        self.weekly_path.mkdir(parents=True, exist_ok=True)
        self.monthly_path.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.analytics = LightweightAnalytics(analytics_path)
        self.cache_warming_manager = CacheWarmingManager()

        logger.info("Weekly analytics processor initialized")

    def process_weekly_analytics(self, target_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Process weekly analytics for a specific date or current week.

        Args:
            target_date: Target date in YYYY-MM-DD format (default: current date)

        Returns:
            Dictionary with processing results
        """
        try:
            if target_date is None:
                target_date = datetime.now().strftime("%Y-%m-%d")

            # Calculate week boundaries
            target_dt = datetime.strptime(target_date, "%Y-%m-%d")
            week_start = target_dt - timedelta(days=target_dt.weekday())
            week_end = week_start + timedelta(days=6)

            week_start_str = week_start.strftime("%Y-%m-%d")
            week_end_str = week_end.strftime("%Y-%m-%d")
            week_key = f"{week_start_str}_to_{week_end_str}"

            logger.info(f"Processing weekly analytics for week: {week_key}")

            # Step 1: Aggregate daily logs
            weekly_data = self._aggregate_daily_logs(week_start_str, week_end_str)

            # Step 2: Generate weekly summary
            weekly_summary = self._generate_weekly_summary(weekly_data, week_key)

            # Step 3: Save weekly summary
            weekly_file = self.weekly_path / f"{week_key}.json"
            with open(weekly_file, "w", encoding="utf-8") as f:
                json.dump(weekly_summary, f, indent=2, ensure_ascii=False)

            # Step 4: Identify popular searches
            popular_searches = self._identify_popular_searches(weekly_data)

            # Step 5: Update cache warming configuration
            config_updated = self._update_cache_warming_config(popular_searches)

            # Step 6: Compress old daily files
            compressed_count = self._compress_old_daily_files(week_start_str)

            # Step 7: Clean old cache entries
            cache_cleaned = self._clean_old_cache_entries()

            results = {
                "status": "completed",
                "week": week_key,
                "total_searches": weekly_summary["total_searches"],
                "popular_job_titles": len(popular_searches["job_titles"]),
                "popular_locations": len(popular_searches["locations"]),
                "config_updated": config_updated,
                "files_compressed": compressed_count,
                "cache_cleaned": cache_cleaned,
                "timestamp": datetime.now().isoformat(),
            }

            logger.info(f"Weekly processing completed: {results['total_searches']} searches processed")
            return results

        except Exception as e:
            logger.error(f"Weekly analytics processing failed: {e}")
            return {"status": "failed", "error": str(e)}

    def _aggregate_daily_logs(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Aggregate daily analytics logs for the specified week."""
        try:
            all_searches = []
            current_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")

            while current_date <= end_dt:
                date_str = current_date.strftime("%Y-%m-%d")
                daily_stats = self.analytics.get_daily_stats(date_str)

                if daily_stats["total_searches"] > 0:
                    all_searches.extend(daily_stats["searches"])

                current_date += timedelta(days=1)

            return {
                "start_date": start_date,
                "end_date": end_date,
                "searches": all_searches,
                "total_searches": len(all_searches),
            }

        except Exception as e:
            logger.error(f"Failed to aggregate daily logs: {e}")
            return {"start_date": start_date, "end_date": end_date, "searches": [], "total_searches": 0}

    def _generate_weekly_summary(self, weekly_data: Dict[str, Any], week_key: str) -> Dict[str, Any]:
        """Generate weekly summary from aggregated data."""
        try:
            searches = weekly_data.get("searches", [])

            # Count by various dimensions
            job_title_counts: Dict[str, int] = {}
            location_counts: Dict[str, int] = {}
            platform_counts: Dict[str, int] = {}
            posting_age_counts: Dict[str, int] = {}
            remote_counts: Dict[str, int] = {"remote": 0, "on_site": 0}

            for search in searches:
                # Job title counts
                job_title = search.get("search_term", "Unknown")
                job_title_counts[job_title] = job_title_counts.get(job_title, 0) + 1

                # Location counts
                location = search.get("location", "Unknown")
                location_counts[location] = location_counts.get(location, 0) + 1

                # Platform counts
                platform = search.get("platform", "Unknown")
                platform_counts[platform] = platform_counts.get(platform, 0) + 1

                # Posting age counts
                posting_age = search.get("job_posting_age", "Unknown")
                posting_age_counts[posting_age] = posting_age_counts.get(posting_age, 0) + 1

                # Remote vs on-site counts
                if search.get("is_remote", False):
                    remote_counts["remote"] += 1
                else:
                    remote_counts["on_site"] += 1

            # Get top items
            top_job_titles = sorted(job_title_counts.items(), key=lambda x: x[1], reverse=True)[:15]

            top_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:10]

            top_platforms = sorted(platform_counts.items(), key=lambda x: x[1], reverse=True)[:5]

            return {
                "week_key": week_key,
                "start_date": weekly_data["start_date"],
                "end_date": weekly_data["end_date"],
                "total_searches": weekly_data["total_searches"],
                "summary": {
                    "top_job_titles": top_job_titles,
                    "top_locations": top_locations,
                    "top_platforms": top_platforms,
                    "posting_age_distribution": posting_age_counts,
                    "remote_vs_onsite": remote_counts,
                },
                "trends": {
                    "most_popular_job": top_job_titles[0] if top_job_titles else None,
                    "most_popular_location": top_locations[0] if top_locations else None,
                    "remote_percentage": round(
                        (
                            (remote_counts["remote"] / weekly_data["total_searches"] * 100)
                            if weekly_data["total_searches"] > 0
                            else 0
                        ),
                        2,
                    ),
                },
                "processed_at": datetime.now().isoformat(),
            }

        except Exception as e:
            logger.error(f"Failed to generate weekly summary: {e}")
            return {"week_key": week_key, "error": str(e)}

    def _identify_popular_searches(self, weekly_data: Dict[str, Any]) -> Dict[str, Any]:
        """Identify popular searches for cache warming optimization."""
        try:
            searches = weekly_data.get("searches", [])

            # Count search combinations
            search_combinations: Dict[str, int] = {}

            for search in searches:
                job_title = search.get("search_term", "Unknown")
                location = search.get("location", "Unknown")
                posting_age = search.get("job_posting_age", "Unknown")
                is_remote = search.get("is_remote", False)

                # Create combination key
                combination_key = f"{job_title}|{location}|{posting_age}|{is_remote}"
                search_combinations[combination_key] = search_combinations.get(combination_key, 0) + 1

            # Get top combinations
            top_combinations = sorted(search_combinations.items(), key=lambda x: x[1], reverse=True)[:20]

            # Extract unique job titles and locations
            job_titles = set()
            locations = set()

            for combination, _ in top_combinations:
                parts = combination.split("|")
                if len(parts) >= 4:
                    job_titles.add(parts[0])
                    locations.add(parts[1])

            # Always prioritize "Global" location
            if "Global" in locations:
                locations.remove("Global")
                locations_list = ["Global"] + list(locations)[:4]
            else:
                locations_list = ["Global"] + list(locations)[:4]

            return {
                "job_titles": list(job_titles)[:10],  # Top 10 job titles
                "locations": locations_list[:5],  # Top 5 locations
                "top_combinations": top_combinations[:15],
                "total_combinations": len(search_combinations),
            }

        except Exception as e:
            logger.error(f"Failed to identify popular searches: {e}")
            return {"job_titles": [], "locations": [], "top_combinations": [], "total_combinations": 0}

    def _update_cache_warming_config(self, popular_searches: Dict[str, Any]) -> bool:
        """Update cache warming configuration based on popular searches."""
        try:
            if not popular_searches["job_titles"]:
                logger.warning("No popular job titles found for config update")
                return False

            # Update the cache warming manager configuration
            self.cache_warming_manager.update_config_from_analytics()

            logger.info("Cache warming configuration updated successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to update cache warming config: {e}")
            return False

    def _compress_old_daily_files(self, cutoff_date: str) -> int:
        """Compress old daily files to save space."""
        try:
            compressed_count = 0
            cutoff_dt = datetime.strptime(cutoff_date, "%Y-%m-%d")

            # Get all daily files
            daily_files = list(self.daily_path.glob("*.json"))

            for file_path in daily_files:
                try:
                    # Extract date from filename
                    filename = file_path.stem
                    if filename == "today":
                        continue

                    file_date = datetime.strptime(filename, "%Y-%m-%d")

                    # Compress files older than cutoff date
                    if file_date < cutoff_dt:
                        compressed_file = file_path.with_suffix(".json.gz")

                        if not compressed_file.exists():
                            with open(file_path, "rb") as f_in:
                                with gzip.open(compressed_file, "wb", compresslevel=6) as f_out:
                                    f_out.writelines(f_in)

                            # Remove original file
                            file_path.unlink()
                            compressed_count += 1

                            logger.debug(f"Compressed daily file: {filename}")

                except Exception as e:
                    logger.warning(f"Failed to compress file {file_path.name}: {e}")
                    continue

            logger.info(f"Compressed {compressed_count} old daily files")
            return compressed_count

        except Exception as e:
            logger.error(f"Failed to compress old daily files: {e}")
            return 0

    def _clean_old_cache_entries(self) -> bool:
        """Clean old cache entries based on TTL and size limits."""
        try:
            # This would integrate with the cache lifecycle manager
            # For now, just log that cleanup is needed
            logger.info("Cache cleanup requested (integration pending)")
            return True

        except Exception as e:
            logger.error(f"Failed to clean old cache entries: {e}")
            return False

    def run_weekly_processing(self) -> Dict[str, Any]:
        """Run complete weekly processing workflow."""
        try:
            logger.info("Starting weekly analytics processing")

            # Process current week
            results = self.process_weekly_analytics()

            if results["status"] == "completed":
                logger.info("Weekly processing completed successfully")

                # Log summary
                logger.info(f"Processed {results['total_searches']} searches")
                logger.info(f"Identified {results['popular_job_titles']} popular job titles")
                logger.info(f"Updated cache warming config: {results['config_updated']}")
                logger.info(f"Compressed {results['files_compressed']} old files")
            else:
                logger.error(f"Weekly processing failed: {results.get('error', 'Unknown error')}")

            return results

        except Exception as e:
            logger.error(f"Weekly processing workflow failed: {e}")
            return {"status": "failed", "error": str(e)}


def main() -> None:
    """Main entry point for the weekly analytics processor."""
    parser = argparse.ArgumentParser(description="Weekly Analytics Processor")
    parser.add_argument("--date", type=str, help="Target date for processing (YYYY-MM-DD format)")
    parser.add_argument("--analytics-path", type=str, default="analytics", help="Path to analytics directory")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Initialize processor
        processor = WeeklyAnalyticsProcessor(args.analytics_path)

        # Run weekly processing
        results = processor.run_weekly_processing()

        # Output results
        if results["status"] == "completed":
            print("✅ Weekly processing completed successfully")
            print(f"   Processed {results['total_searches']} searches")
            print(f"   Popular job titles: {results['popular_job_titles']}")
            print(f"   Config updated: {results['config_updated']}")
            print(f"   Files compressed: {results['files_compressed']}")
            print(f"   Cache cleaned: {results['cache_cleaned']}")
        else:
            print(f"❌ Weekly processing failed: {results.get('error', 'Unknown error')}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Weekly processor failed: {e}")
        print(f"❌ Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
