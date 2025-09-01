"""
Search Analytics Module

This module provides lightweight search logging and analytics to track
user search patterns and optimize cache warming strategies.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


class SearchAnalytics:
    """
    Lightweight search analytics for tracking user search patterns
    """

    def __init__(self, log_file: str = "logs/search_analytics.json", max_log_size_mb: int = 10):
        """
        Initialize search analytics

        Args:
            log_file: File to store search logs (defaults to logs/search_analytics.json)
            max_log_size_mb: Maximum log file size in MB before rotation
        """
        self.log_file = Path(log_file)
        self.max_log_size_bytes = max_log_size_mb * 1024 * 1024

        # Ensure logs directory exists
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self._search_counts: Dict[str, int] = defaultdict(int)
        self._daily_searches: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Load existing data
        self._load_existing_data()

    def _load_existing_data(self) -> None:
        """Load existing search analytics data from file"""
        try:
            if self.log_file.exists():
                with open(self.log_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self._search_counts = defaultdict(int, data.get("search_counts", {}))
                    self._daily_searches = defaultdict(lambda: defaultdict(int))

                    # Convert daily searches back to nested defaultdict
                    for date, searches in data.get("daily_searches", {}).items():
                        self._daily_searches[date] = defaultdict(int, searches)

                logger.info(f"Loaded {len(self._search_counts)} search records from analytics file")
        except Exception as e:
            logger.warning(f"Failed to load search analytics: {e}")

    def _save_data(self) -> None:
        """Save search analytics data to file"""
        try:
            # Check file size and rotate if needed
            if self.log_file.exists() and self.log_file.stat().st_size > self.max_log_size_bytes:
                self._rotate_log_file()

            # Convert defaultdict to regular dict for JSON serialization
            data = {
                "search_counts": dict(self._search_counts),
                "daily_searches": {date: dict(searches) for date, searches in self._daily_searches.items()},
                "last_updated": datetime.now().isoformat(),
            }

            with open(self.log_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

        except Exception as e:
            logger.error(f"Failed to save search analytics: {e}")

    def _rotate_log_file(self) -> None:
        """Rotate log file when it gets too large"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = self.log_file.with_suffix(f".{timestamp}.json")
            self.log_file.rename(backup_file)
            logger.info(f"Rotated search analytics log to {backup_file}")
        except Exception as e:
            logger.error(f"Failed to rotate log file: {e}")

    def log_search(
        self,
        job_title: str,
        location: str,
        remote: bool = False,
        scraper_name: str = "indeed",
        posting_age: str = "any",
    ) -> None:
        """
        Log a search query

        Args:
            job_title: Job title searched for
            location: Location searched for
            remote: Whether remote jobs were requested
            scraper_name: Name of the scraper used
            posting_age: Posting age filter used
        """
        # Create search key
        search_key = f"{job_title}|{location}|{remote}|{posting_age}|{scraper_name}"

        # Update counters
        self._search_counts[search_key] += 1

        # Update daily statistics
        today = datetime.now().strftime("%Y-%m-%d")
        self._daily_searches[today][search_key] += 1

        # Save data periodically (every 10 searches)
        if sum(self._search_counts.values()) % 10 == 0:
            self._save_data()

        logger.debug(f"Logged search: {job_title} in {location} (remote: {remote})")

    def get_popular_searches(self, days: int = 30, limit: int = 20) -> List[Tuple[str, int]]:
        """
        Get most popular searches in the last N days

        Args:
            days: Number of days to look back
            limit: Maximum number of results to return

        Returns:
            List of (search_key, count) tuples sorted by popularity
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        # Aggregate searches from the specified time period
        period_counts: Dict[str, int] = defaultdict(int)

        for date, searches in self._daily_searches.items():
            if date >= cutoff_str:
                for search_key, count in searches.items():
                    period_counts[search_key] += count

        # Sort by count and return top results
        sorted_searches = sorted(period_counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_searches[:limit]

    def get_popular_job_titles(self, days: int = 30, limit: int = 10) -> List[Tuple[str, int]]:
        """
        Get most popular job titles in the last N days

        Args:
            days: Number of days to look back
            limit: Maximum number of results to return

        Returns:
            List of (job_title, count) tuples sorted by popularity
        """
        job_title_counts: Dict[str, int] = defaultdict(int)

        for search_key, count in self.get_popular_searches(days, limit * 5):  # Get more to account for locations
            parts = search_key.split("|")
            if len(parts) >= 1:
                job_title = parts[0]
                job_title_counts[job_title] += count

        sorted_titles = sorted(job_title_counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_titles[:limit]

    def get_popular_locations(self, days: int = 30, limit: int = 10) -> List[Tuple[str, int]]:
        """
        Get most popular locations in the last N days

        Args:
            days: Number of days to look back
            limit: Maximum number of results to return

        Returns:
            List of (location, count) tuples sorted by popularity
        """
        location_counts: Dict[str, int] = defaultdict(int)

        for search_key, count in self.get_popular_searches(days, limit * 5):  # Get more to account for job titles
            parts = search_key.split("|")
            if len(parts) >= 2:
                location = parts[1]
                location_counts[location] += count

        sorted_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_locations[:limit]

    def get_popular_posting_ages(self, days: int = 30, limit: int = 10) -> List[Tuple[str, int]]:
        """
        Get most popular posting age filters in the last N days

        Args:
            days: Number of days to look back
            limit: Maximum number of results to return

        Returns:
            List of (posting_age, count) tuples sorted by popularity
        """
        posting_age_counts: Dict[str, int] = defaultdict(int)

        for search_key, count in self.get_popular_searches(days, limit * 5):
            parts = search_key.split("|")
            if len(parts) >= 4:
                posting_age = parts[3]
                posting_age_counts[posting_age] += count

        sorted_posting_ages = sorted(posting_age_counts.items(), key=lambda x: x[1], reverse=True)
        return sorted_posting_ages[:limit]

    def get_search_trends(self, days: int = 7) -> Dict[str, int]:
        """
        Get search trends over the last N days

        Args:
            days: Number of days to analyze

        Returns:
            Dictionary with daily search counts
        """
        trends = {}

        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            if date in self._daily_searches:
                daily_total = sum(self._daily_searches[date].values())
                trends[date] = daily_total
            else:
                trends[date] = 0

        return trends

    def get_analytics_summary(self) -> Dict[str, Any]:
        """
        Get a summary of search analytics

        Returns:
            Dictionary with analytics summary
        """
        total_searches = sum(self._search_counts.values())
        today = datetime.now().strftime("%Y-%m-%d")
        today_searches = sum(self._daily_searches.get(today, {}).values())

        popular_jobs = self.get_popular_job_titles(days=30, limit=5)
        popular_locations = self.get_popular_locations(days=30, limit=5)

        return {
            "total_searches": total_searches,
            "today_searches": today_searches,
            "unique_search_combinations": len(self._search_counts),
            "popular_job_titles": popular_jobs,
            "popular_locations": popular_locations,
            "search_trends_7d": self.get_search_trends(days=7),
            "log_file_size_mb": round(self.log_file.stat().st_size / (1024 * 1024), 2) if self.log_file.exists() else 0,
        }

    def cleanup_old_data(self, days_to_keep: int = 90) -> int:
        """
        Clean up old search data to keep the log file manageable

        Args:
            days_to_keep: Number of days of data to keep

        Returns:
            Number of old entries removed
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cutoff_str = cutoff_date.strftime("%Y-%m-%d")

        removed_count = 0
        dates_to_remove = []

        for date in self._daily_searches:
            if date < cutoff_str:
                removed_count += sum(self._daily_searches[date].values())
                dates_to_remove.append(date)

        for date in dates_to_remove:
            del self._daily_searches[date]

        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old search records")
            self._save_data()

        return removed_count

    def export_data(self, output_file: str = "search_analytics_export.json") -> bool:
        """
        Export search analytics data to a file

        Args:
            output_file: Output file path

        Returns:
            True if export was successful
        """
        try:
            data = {
                "search_counts": dict(self._search_counts),
                "daily_searches": {date: dict(searches) for date, searches in self._daily_searches.items()},
                "analytics_summary": self.get_analytics_summary(),
                "export_date": datetime.now().isoformat(),
            }

            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            logger.info(f"Exported search analytics to {output_file}")
            return True

        except Exception as e:
            logger.error(f"Failed to export search analytics: {e}")
            return False
