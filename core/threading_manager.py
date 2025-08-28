"""
Threading Manager for parallel job scraping.

This module provides:
- ThreadPoolExecutor for parallel country searches
- Real-time progress callbacks to Streamlit
- Error handling for failed country searches
- Performance monitoring for parallel operations
- Memory-efficient result aggregation
"""

import logging
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from queue import Queue
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

# Suppress Streamlit threading warnings - they're expected and harmless
warnings.filterwarnings("ignore", category=UserWarning, module="streamlit")
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
warnings.filterwarnings("ignore", message=".*Session state does not function.*")


@dataclass
class SearchTask:
    """Represents a single country search task."""

    country: str
    search_term: str
    include_remote: bool
    time_filter: Optional[str] = None
    task_id: Optional[str] = None


@dataclass
class SearchResult:
    """Represents the result of a country search."""

    country: str
    success: bool
    jobs: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    search_time: float = 0.0
    jobs_count: int = 0
    task_id: Optional[str] = None


class ThreadingManager:
    """
    Manages parallel processing for global job searches.

    Features:
    - ThreadPoolExecutor for concurrent country searches
    - Real-time progress updates via callbacks
    - Comprehensive error handling and recovery
    - Memory-efficient result aggregation
    - Performance monitoring and logging
    """

    def __init__(self, max_workers: int = 4, timeout_per_country: int = 30):
        """
        Initialize the threading manager.

        Args:
            max_workers: Maximum number of concurrent threads
            timeout_per_country: Timeout in seconds for each country search
        """
        self.max_workers = max_workers
        self.timeout_per_country = timeout_per_country
        self.logger = logging.getLogger("threading.manager")

        # Performance tracking
        self.total_searches = 0
        self.successful_searches = 0
        self.failed_searches = 0
        self.total_search_time = 0.0

        # Thread safety
        self._lock = threading.Lock()
        self._progress_queue = Queue()

    def search_countries_parallel(
        self,
        countries: List[str],
        search_func: Callable,
        search_term: str,
        include_remote: bool = True,
        time_filter: Optional[str] = None,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, Any]:
        """
        Perform parallel search across multiple countries.

        Args:
            countries: List of countries to search
            search_func: Function to call for each country search
            search_term: Job search term
            include_remote: Whether to include remote jobs
            time_filter: Time filter for job postings
            progress_callback: Callback for progress updates

        Returns:
            Dictionary with search results and metadata
        """
        if not countries:
            return self._empty_result("No countries provided")

        start_time = time.time()
        total_countries = len(countries)

        # Initialize progress tracking
        completed_countries = 0
        successful_countries = 0
        failed_countries = 0
        all_results = []

        # Create search tasks
        tasks = [
            SearchTask(
                country=country,
                search_term=search_term,
                include_remote=include_remote,
                time_filter=time_filter,
                task_id=f"task_{i}",
            )
            for i, country in enumerate(countries)
        ]

        # Update initial progress
        if progress_callback:
            progress_callback(f"🚀 Starting parallel search across {total_countries} countries...", 0.05)

        self.logger.info(f"🌍 Starting parallel search: {total_countries} countries, {self.max_workers} workers")

        # Execute searches in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_task = {
                executor.submit(self._search_single_country_threaded, task, search_func): task for task in tasks
            }

            # Process completed tasks
            for future in as_completed(future_to_task, timeout=self.timeout_per_country * total_countries):
                task = future_to_task[future]

                try:
                    result = future.result(timeout=5)  # 5s timeout for result processing

                    # Update counters
                    with self._lock:
                        completed_countries += 1
                        if result.success:
                            successful_countries += 1
                            if result.jobs is not None and not result.jobs.empty:
                                all_results.append(result)
                        else:
                            failed_countries += 1

                    # Update progress
                    progress_percent = 0.05 + (completed_countries / total_countries) * 0.9
                    if progress_callback:
                        status = f"✅ {result.country}" if result.success else f"❌ {result.country}"
                        progress_callback(
                            f"🌍 {completed_countries}/{total_countries} countries: {status} ({result.jobs_count} jobs)",
                            progress_percent,
                        )

                    # Log result
                    if result.success:
                        self.logger.info(f"✅ {result.country}: {result.jobs_count} jobs in {result.search_time:.2f}s")
                    else:
                        self.logger.warning(f"❌ {result.country}: {result.error}")

                except Exception as e:
                    # Handle task execution errors
                    with self._lock:
                        completed_countries += 1
                        failed_countries += 1

                    error_msg = f"Task execution failed for {task.country}: {str(e)}"
                    self.logger.error(error_msg)

                    if progress_callback:
                        progress_percent = 0.05 + (completed_countries / total_countries) * 0.9
                        progress_callback(
                            f"🌍 {completed_countries}/{total_countries} countries: ❌ {task.country} (error)",
                            progress_percent,
                        )

        # Process final results
        total_time = time.time() - start_time

        # Update performance stats
        with self._lock:
            self.total_searches += total_countries
            self.successful_searches += successful_countries
            self.failed_searches += failed_countries
            self.total_search_time += total_time

        # Combine results
        combined_jobs = self._combine_results(all_results)

        # Final progress update
        if progress_callback:
            progress_callback(
                f"🎉 Parallel search complete! {successful_countries}/{total_countries} countries successful, "
                f"{len(combined_jobs)} total jobs in {total_time:.2f}s",
                1.0,
            )

        return {
            "success": True,
            "jobs": combined_jobs,
            "count": len(combined_jobs),
            "search_time": total_time,
            "message": f"Found {len(combined_jobs)} jobs across {successful_countries}/{total_countries} countries",
            "metadata": {
                "search_type": "parallel_global",
                "countries_searched": successful_countries,
                "total_countries": total_countries,
                "failed_countries": failed_countries,
                "parallel_workers": self.max_workers,
                "performance": {
                    "avg_time_per_country": total_time / total_countries if total_countries > 0 else 0,
                    "success_rate": successful_countries / total_countries if total_countries > 0 else 0,
                    "speedup_factor": self._calculate_speedup(total_time, total_countries),
                },
            },
        }

    def _search_single_country_threaded(self, task: SearchTask, search_func: Callable) -> SearchResult:
        """
        Execute a single country search in a thread.

        Args:
            task: Search task to execute
            search_func: Function to call for the search

        Returns:
            SearchResult with the outcome
        """
        start_time = time.time()

        try:
            # Call the search function - handle both positional and keyword argument styles
            # Try keyword arguments first (for test mocks), then fallback to positional
            try:
                result = search_func(
                    search_term=task.search_term,
                    where=task.country,
                    include_remote=task.include_remote,
                    time_filter=task.time_filter,
                )
            except TypeError:
                # Fallback to positional arguments for BaseScraper methods
                # Method signature: _search_single_country_optimized(search_term, country, include_remote, **kwargs)
                result = search_func(
                    task.search_term,  # search_term (positional)
                    task.country,  # country (positional)
                    task.include_remote,  # include_remote (positional)
                    time_filter=task.time_filter,  # **kwargs
                )

            search_time = time.time() - start_time

            if result.get("success", False) and result.get("jobs") is not None:
                jobs_df = result["jobs"]
                jobs_count = len(jobs_df) if not jobs_df.empty else 0

                # Add country metadata
                if not jobs_df.empty:
                    jobs_df = jobs_df.copy()
                    jobs_df["source_country"] = task.country

                return SearchResult(
                    country=task.country,
                    success=True,
                    jobs=jobs_df,
                    search_time=search_time,
                    jobs_count=jobs_count,
                    task_id=task.task_id,
                )
            else:
                return SearchResult(
                    country=task.country,
                    success=False,
                    error=result.get("message", "Unknown error"),
                    search_time=search_time,
                    task_id=task.task_id,
                )

        except Exception as e:
            search_time = time.time() - start_time
            return SearchResult(
                country=task.country, success=False, error=str(e), search_time=search_time, task_id=task.task_id
            )

    def _combine_results(self, results: List[SearchResult]) -> pd.DataFrame:
        """
        Combine results from multiple country searches.

        Args:
            results: List of SearchResult objects

        Returns:
            Combined DataFrame with all jobs
        """
        if not results:
            return pd.DataFrame()

        # Filter successful results with jobs
        valid_results = [r for r in results if r.success and r.jobs is not None and not r.jobs.empty]

        if not valid_results:
            return pd.DataFrame()

        # Combine DataFrames
        if len(valid_results) == 1:
            return valid_results[0].jobs

        # Handle multiple DataFrames
        combined_jobs = []
        for result in valid_results:
            df = result.jobs.copy()
            # Ensure consistent dtypes to avoid concat issues
            for col in df.columns:
                if isinstance(df[col].dtype, pd.CategoricalDtype):
                    df[col] = df[col].astype("object")
            combined_jobs.append(df)

        # Concatenate with error handling
        try:
            import warnings

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning, message=".*DataFrame concatenation.*")
                combined_df = pd.concat(combined_jobs, ignore_index=True, sort=False)
        except Exception as e:
            self.logger.error(f"Error combining results: {e}")
            # Fallback: return first result
            return valid_results[0].jobs

        # Remove duplicates based on job_url
        if "job_url" in combined_df.columns:
            initial_count = len(combined_df)
            combined_df = combined_df.drop_duplicates(subset=["job_url"], keep="first")
            duplicates_removed = initial_count - len(combined_df)

            if duplicates_removed > 0:
                self.logger.info(f"🔧 Removed {duplicates_removed} duplicate jobs")

        return combined_df

    def _empty_result(self, message: str) -> Dict[str, Any]:
        """Return empty result structure."""
        return {
            "success": True,
            "jobs": pd.DataFrame(),
            "count": 0,
            "search_time": 0.0,
            "message": message,
            "metadata": {
                "search_type": "parallel_global",
                "countries_searched": 0,
                "total_countries": 0,
                "failed_countries": 0,
                "parallel_workers": self.max_workers,
            },
        }

    def _calculate_speedup(self, total_time: float, country_count: int) -> float:
        """
        Calculate speedup factor compared to sequential processing.

        Assumes average 3 seconds per country for sequential processing.
        """
        if country_count == 0:
            return 1.0

        estimated_sequential_time = country_count * 3.0  # 3s per country estimate
        if estimated_sequential_time > 0:
            return estimated_sequential_time / total_time
        return 1.0

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get threading performance statistics."""
        return {
            "total_searches": self.total_searches,
            "successful_searches": self.successful_searches,
            "failed_searches": self.failed_searches,
            "success_rate": self.successful_searches / self.total_searches if self.total_searches > 0 else 0,
            "total_search_time": self.total_search_time,
            "avg_search_time": self.total_search_time / self.total_searches if self.total_searches > 0 else 0,
            "max_workers": self.max_workers,
            "timeout_per_country": self.timeout_per_country,
        }

    def reset_stats(self):
        """Reset performance statistics."""
        with self._lock:
            self.total_searches = 0
            self.successful_searches = 0
            self.failed_searches = 0
            self.total_search_time = 0.0
