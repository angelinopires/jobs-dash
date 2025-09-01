"""
Lightweight Analytics System for Jobs Dashboard

This module provides a simple, file-based analytics system that tracks search patterns
without requiring a database. It's designed for free-tier deployment with minimal
storage requirements.

Features:
- Daily search logging with JSON files
- Automatic daily aggregation
- File compression for space efficiency
- No external dependencies beyond Python standard library
"""

import gzip
import json
import logging
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class SearchEvent:
    """Represents a single search event for analytics tracking."""

    search_term: str
    location: str
    is_remote: bool
    platform: str
    job_posting_age: str
    timestamp: str
    session_id: Optional[str] = None
    response_time_ms: Optional[int] = None
    results_count: Optional[int] = None


class LightweightAnalytics:
    """
    Lightweight analytics system using JSON files and compression.

    This system tracks search patterns without a database, making it perfect
    for free-tier deployments. It automatically compresses old data to save space.
    """

    def __init__(self, base_path: str = "analytics"):
        """
        Initialize the analytics system.

        Args:
            base_path: Base directory for analytics storage
        """
        self.base_path = Path(base_path)
        self.daily_path = self.base_path / "daily"
        self.weekly_path = self.base_path / "weekly"
        self.monthly_path = self.base_path / "monthly"

        # Create directories if they don't exist
        self.daily_path.mkdir(parents=True, exist_ok=True)
        self.weekly_path.mkdir(parents=True, exist_ok=True)
        self.monthly_path.mkdir(parents=True, exist_ok=True)

        # Thread safety
        self._lock = threading.Lock()

        logger.info(f"Analytics system initialized at {self.base_path}")

    def log_search(self, search_event: SearchEvent) -> None:
        """
        Log a search event to the daily analytics file.

        Args:
            search_event: SearchEvent object containing search details
        """
        try:
            with self._lock:
                today = datetime.now().strftime("%Y-%m-%d")
                daily_file = self.daily_path / f"{today}.json"

                # Load existing data or create new
                if daily_file.exists():
                    with open(daily_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                else:
                    data = {"date": today, "searches": []}

                # Add new search event
                data["searches"].append(asdict(search_event))

                # Write back to file
                with open(daily_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                logger.debug(f"Logged search: {search_event.search_term} in {search_event.location}")

        except Exception as e:
            logger.error(f"Failed to log search event: {e}")

    def get_daily_stats(self, date: str) -> Dict[str, Any]:
        """
        Get analytics statistics for a specific date.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            Dictionary with daily statistics
        """
        try:
            daily_file = self.daily_path / f"{date}.json"
            if not daily_file.exists():
                return {"date": date, "searches": [], "total_searches": 0}

            with open(daily_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Calculate basic stats
            searches = data.get("searches", [])
            total_searches = len(searches)

            # Count by job title
            job_title_counts: Dict[str, int] = {}
            location_counts: Dict[str, int] = {}
            platform_counts: Dict[str, int] = {}

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

            return {
                "date": date,
                "searches": searches,
                "total_searches": total_searches,
                "job_title_counts": job_title_counts,
                "location_counts": location_counts,
                "platform_counts": platform_counts,
            }

        except Exception as e:
            logger.error(f"Failed to get daily stats for {date}: {e}")
            return {"date": date, "searches": [], "total_searches": 0}

    def get_popular_searches(self, days: int = 7) -> Dict[str, Any]:
        """
        Get popular searches over a specified number of days.

        Args:
            days: Number of days to analyze (default: 7)

        Returns:
            Dictionary with popular searches and trends
        """
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            all_searches = []
            job_title_counts: Dict[str, int] = {}
            location_counts: Dict[str, int] = {}

            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                daily_stats = self.get_daily_stats(date_str)

                if daily_stats["total_searches"] > 0:
                    all_searches.extend(daily_stats["searches"])

                    # Aggregate counts
                    for job_title, count in daily_stats["job_title_counts"].items():
                        job_title_counts[job_title] = job_title_counts.get(job_title, 0) + count

                    for location, count in daily_stats["location_counts"].items():
                        location_counts[location] = location_counts.get(location, 0) + count

                current_date += timedelta(days=1)

            # Sort by popularity
            popular_job_titles = sorted(job_title_counts.items(), key=lambda x: x[1], reverse=True)[:10]

            popular_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)[:5]

            return {
                "period_days": days,
                "total_searches": len(all_searches),
                "popular_job_titles": popular_job_titles,
                "popular_locations": popular_locations,
                "search_trends": {
                    "remote_searches": sum(1 for s in all_searches if s.get("is_remote", False)),
                    "global_searches": sum(1 for s in all_searches if s.get("location") == "Global"),
                    "recent_searches": sum(1 for s in all_searches if s.get("job_posting_age") == "Last 24h"),
                },
            }

        except Exception as e:
            logger.error(f"Failed to get popular searches: {e}")
            return {"period_days": days, "total_searches": 0}

    def compress_daily_file(self, date: str) -> bool:
        """
        Compress a daily analytics file to save space.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            True if compression successful, False otherwise
        """
        try:
            daily_file = self.daily_path / f"{date}.json"
            compressed_file = self.daily_path / f"{date}.json.gz"

            if not daily_file.exists():
                return False

            # Read and compress
            with open(daily_file, "rb") as f_in:
                with gzip.open(compressed_file, "wb", compresslevel=6) as f_out:
                    f_out.writelines(f_in)

            # Remove original file
            daily_file.unlink()

            logger.info(f"Compressed daily file: {date}")
            return True

        except Exception as e:
            logger.error(f"Failed to compress daily file {date}: {e}")
            return False

    def cleanup_old_files(self, max_daily_files: int = 30) -> None:
        """
        Clean up old analytics files to manage storage.

        Args:
            max_daily_files: Maximum number of daily files to keep
        """
        try:
            # Get all daily files sorted by date
            daily_files = sorted(
                [f for f in self.daily_path.glob("*.json*")], key=lambda x: x.stat().st_mtime, reverse=True
            )

            # Remove old files
            if len(daily_files) > max_daily_files:
                files_to_remove = daily_files[max_daily_files:]
                for file_path in files_to_remove:
                    file_path.unlink()
                    logger.info(f"Removed old analytics file: {file_path.name}")

        except Exception as e:
            logger.error(f"Failed to cleanup old files: {e}")

    def get_storage_stats(self) -> Dict[str, Any]:
        """
        Get storage statistics for analytics data.

        Returns:
            Dictionary with storage information
        """
        try:
            total_size = 0
            file_counts = {"daily": 0, "weekly": 0, "monthly": 0}

            # Count files and calculate sizes
            for folder_name, folder_path in [
                ("daily", self.daily_path),
                ("weekly", self.weekly_path),
                ("monthly", self.monthly_path),
            ]:
                files = list(folder_path.glob("*"))
                file_counts[folder_name] = len(files)

                for file_path in files:
                    if file_path.is_file():
                        total_size += file_path.stat().st_size

            return {
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "file_counts": file_counts,
                "compression_ratio": self._estimate_compression_ratio(),
            }

        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {"total_size_bytes": 0, "total_size_mb": 0, "file_counts": {}}

    def _estimate_compression_ratio(self) -> float:
        """Estimate compression ratio based on file types."""
        try:
            total_files = 0
            compressed_files = 0

            for folder_path in [self.daily_path, self.weekly_path, self.monthly_path]:
                for file_path in folder_path.glob("*"):
                    if file_path.is_file():
                        total_files += 1
                        if file_path.suffix == ".gz":
                            compressed_files += 1

            if total_files == 0:
                return 0.0

            # Estimate 70% space savings for compressed files
            compression_ratio = (compressed_files / total_files) * 0.7
            return round(compression_ratio, 2)

        except Exception:
            return 0.0


# Convenience function for quick search logging
def log_search_quick(
    search_term: str,
    location: str,
    is_remote: bool = True,
    platform: str = "indeed",
    job_posting_age: str = "Last 24h",
    session_id: Optional[str] = None,
    response_time_ms: Optional[int] = None,
    results_count: Optional[int] = None,
) -> None:
    """
    Quick function to log a search event.

    Args:
        search_term: Job title or search term
        location: Search location
        is_remote: Whether remote jobs were requested
        platform: Job platform (indeed, linkedin, etc.)
        job_posting_age: Age filter for job postings
        session_id: Optional session identifier
        response_time_ms: Optional response time in milliseconds
        results_count: Optional number of results returned
    """
    analytics = LightweightAnalytics()
    search_event = SearchEvent(
        search_term=search_term,
        location=location,
        is_remote=is_remote,
        platform=platform,
        job_posting_age=job_posting_age,
        timestamp=datetime.now().strftime("%H:%M:%S"),
        session_id=session_id,
        response_time_ms=response_time_ms,
        results_count=results_count,
    )
    analytics.log_search(search_event)
