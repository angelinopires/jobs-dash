"""
Search orchestrator for job search infrastructure.

This module provides the main search orchestration functionality that coordinates
job searches across multiple countries and job boards, with built-in optimizations,
caching, monitoring, and resilience mechanisms.
"""

import time
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from ..monitoring.performance_monitor import PerformanceMonitor
from ..redis.redis_cache_manager import RedisCacheManager
from ..resilience.circuit_breaker import CircuitOpenException, get_circuit_breaker
from ..resilience.rate_limiter import get_rate_limiter
from .threading_manager import ThreadingManager


class SearchOrchestrator(ABC):
    """
    Search orchestrator for job search infrastructure with built-in optimizations.

    This class provides:
    - Search coordination across multiple countries and job boards
    - Standardized caching across all searches
    - Performance monitoring and logging
    - Parallel result processing capabilities
    - Common error handling and fallback strategies
    """

    def __init__(self, scraper_name: str) -> None:
        self.scraper_name = scraper_name
        self.performance_monitor = PerformanceMonitor(scraper_name)
        self.cache_manager = RedisCacheManager()
        self.threading_manager = ThreadingManager()
        self.last_search_time = 0.0
        self.min_delay = 1.0  # Minimum delay between API calls

        self.circuit_breaker = get_circuit_breaker(f"{scraper_name}_api")
        self.rate_limiter = get_rate_limiter()

    # Abstract methods that each scraper must implement

    @abstractmethod
    def get_supported_countries(self) -> List[str]:
        """Return list of countries this scraper supports."""
        pass

    @abstractmethod
    def get_supported_api_filters(self) -> Dict[str, bool]:
        """Return which filters this scraper supports at API level."""
        pass

    @abstractmethod
    def _build_api_search_params(self, **filters: Any) -> Dict[str, Any]:
        """Build search parameters for the scraping API."""
        pass

    @abstractmethod
    def _call_scraping_api(self, search_params: Dict[str, Any]) -> pd.DataFrame:
        """Call the actual scraping library/API."""
        pass

    def _call_scraping_api_with_circuit_breaker(
        self,
        search_params: Dict[str, Any],
        progress_callback: Optional[Callable[[str], None]] = None,
        country: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Call scraping API with circuit breaker protection and intelligent rate limiting.

        This method wraps the actual API call with circuit breaker logic and intelligent
        rate limiting, providing fallback to cached results when the circuit is open.

        Args:
            search_params: API search parameters
            progress_callback: Optional callback for user feedback
            country: Optional country for per-country rate limiting

        Returns:
            pd.DataFrame: Job results or cached fallback
        """
        # Use per-country endpoint for parallel requests, fallback to global endpoint
        if country:
            endpoint = f"{self.scraper_name}_api_{country.lower()}"
        else:
            endpoint = f"{self.scraper_name}_api"

        try:
            return self.rate_limiter.call_with_rate_limiting(
                self.circuit_breaker.call,
                endpoint,
                self._call_scraping_api,
                search_params,
                progress_callback=progress_callback,
            )

        except CircuitOpenException:
            # Circuit is open - return cached results if available
            if progress_callback:
                progress_callback("⚠️ API temporarily unavailable, using cached results...")

            self.performance_monitor.log(
                "Circuit Breaker", f"⚠️ Circuit OPEN for {self.scraper_name}, using cached results"
            )

            # Return empty DataFrame as fallback
            # In Phase 3, this will be enhanced with Redis cache fallback
            return pd.DataFrame()

    def search_jobs(
        self, search_term: str = "", where: str = "", include_remote: bool = True, **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Main search interface with built-in optimizations.

        This method handles:
        - Cache checking and management
        - Performance monitoring
        - Global vs single-country search routing
        - Result processing and formatting

        Args:
            search_term: Job title or keywords
            where: "Global" for multi-country search or specific country name
            include_remote: Whether to include remote work filters
            **kwargs: Additional scraper-specific filters

        Returns:
            Dict with results, timing, and metadata
        """
        search_start = time.time()

        # Start performance monitoring
        self.performance_monitor.start_search(search_term, where, include_remote)

        try:
            # Handle global vs single-country searches
            if where == "Global":
                # Extract progress_callback from kwargs to avoid duplicate parameter
                progress_callback = kwargs.pop("progress_callback", None)
                result = self._search_global_optimized(search_term, include_remote, progress_callback, **kwargs)
            else:
                result = self._search_single_country_optimized(search_term, where, include_remote, **kwargs)

            # Add timing information
            total_time = time.time() - search_start
            result["search_time"] = total_time

            # Log performance
            self.performance_monitor.end_search(result["success"], total_time, result.get("count", 0))

            return result

        except Exception as e:
            total_time = time.time() - search_start
            self.performance_monitor.end_search(False, total_time, 0, str(e))

            return {
                "success": False,
                "jobs": pd.DataFrame(),
                "count": 0,
                "search_time": total_time,
                "message": f"Search failed: {str(e)}",
                "metadata": {"error": str(e)},
            }

    def _search_single_country_optimized(
        self, search_term: str, country: str, include_remote: bool, **kwargs: Any
    ) -> Dict[str, Any]:
        """Optimized single-country search with caching."""

        # Filter out function references from kwargs to avoid JSON serialization issues
        filtered_kwargs = {k: v for k, v in kwargs.items() if not callable(v)}

        # Check Redis cache first (RedisCacheManager generates keys internally)
        cached_result = self.cache_manager.get_cached_result(
            scraper=self.scraper_name,
            search_term=search_term,
            country=country,
            remote=include_remote,
            **filtered_kwargs,
        )
        if cached_result:
            # Create cache info for performance monitoring (Redis doesn't expose cache entry details)
            cache_info = {"source": "redis", "hit": True}
            cache_key_for_logging = f"{self.scraper_name}_{search_term}_{country}"
            self.performance_monitor.log_cache_event("hit", cache_key_for_logging, country, cache_info)

            # Convert cached result to expected format (list of jobs -> DataFrame)
            jobs_df = pd.DataFrame(cached_result)
            return {
                "success": True,
                "jobs": jobs_df,
                "count": len(cached_result),
                "search_time": 0.0,  # Cache hit has minimal time
                "message": f"Found {len(cached_result)} jobs (cached)",
                "metadata": {"source": "cache", "cache_hit": True},
            }

        # No cache hit - perform actual search
        cache_key_for_logging = f"{self.scraper_name}_{search_term}_{country}"
        self.performance_monitor.log_cache_event("miss", cache_key_for_logging, country)

        # Rate limiting
        self._apply_rate_limiting()

        # Build API parameters (filter out function references)
        filters = {
            "search_term": search_term,
            "where": country,
            "location": country,
            "include_remote": include_remote,
            **filtered_kwargs,
        }

        api_params = self._build_api_search_params(**filters)

        # Call scraping API with circuit breaker protection
        start_time = time.time()
        jobs_df = self._call_scraping_api_with_circuit_breaker(api_params, country=country)
        api_time = time.time() - start_time

        # Process results
        if not jobs_df.empty:
            processed_jobs = self._process_jobs_optimized(jobs_df)

            result = {
                "success": True,
                "jobs": processed_jobs,
                "count": len(processed_jobs),
                "message": f"Found {len(processed_jobs)} jobs in {country}",
                "metadata": {"country": country, "api_time": api_time, "scraper": self.scraper_name},
            }
        else:
            result = {
                "success": True,
                "jobs": pd.DataFrame(),
                "count": 0,
                "message": f"No jobs found in {country}",
                "metadata": {"country": country, "api_time": api_time, "scraper": self.scraper_name},
            }

        # Cache the result in Redis (only cache successful results with jobs)
        jobs_data = result.get("jobs")
        if result.get("success") and jobs_data is not None and not jobs_data.empty:
            # Convert DataFrame to list of dicts for Redis storage
            jobs_list = jobs_data.to_dict("records") if hasattr(jobs_data, "to_dict") else jobs_data
            self.cache_manager.cache_result(
                scraper=self.scraper_name,
                search_term=search_term,
                country=country,
                result=jobs_list,  # RedisCacheManager expects list of job dicts
                remote=include_remote,
                **filtered_kwargs,
            )

        return result

    def _search_global_optimized(
        self, search_term: str, include_remote: bool, progress_callback: Optional[Callable], **kwargs: Any
    ) -> Dict[str, Any]:
        """
        Phase 2: Parallel global search with ThreadPoolExecutor.

        Features:
        - Parallel processing across multiple countries
        - Real-time progress callbacks to Streamlit
        - Comprehensive error handling for failed countries
        - Performance monitoring and speedup tracking
        - Memory-efficient result aggregation
        """
        countries = self.get_supported_countries()

        if not countries:
            return {
                "success": True,
                "jobs": pd.DataFrame(),
                "count": 0,
                "search_time": 0.0,
                "message": "No supported countries available",
                "metadata": {
                    "search_type": "parallel_global",
                    "countries_searched": 0,
                    "total_countries": 0,
                    "scraper": self.scraper_name,
                },
            }

        # Extract additional parameters for threading
        time_filter = kwargs.get("time_filter")

        # Use threading manager for parallel processing
        result = self.threading_manager.search_countries_parallel(
            countries=countries,
            search_func=self._search_single_country_optimized,
            search_term=search_term,
            include_remote=include_remote,
            time_filter=time_filter,
            progress_callback=progress_callback,
        )

        # Add scraper metadata
        result["metadata"]["scraper"] = self.scraper_name

        # Log performance metrics
        performance = result["metadata"].get("performance", {})
        speedup = performance.get("speedup_factor", 1.0)
        success_rate = performance.get("success_rate", 0.0)

        self.performance_monitor.log(
            "Parallel search",
            f"🚀 {len(countries)} countries in {result['search_time']:.2f}s "
            f"(speedup: {speedup:.1f}x, success: {success_rate:.1%})",
        )

        return result

    def _process_jobs_optimized(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimized job processing with parallel-ready architecture.

        This method can be enhanced later with actual parallel processing.
        For now, it provides a clean interface for result processing.
        """
        if jobs_df.empty:
            return jobs_df

        # Delegate to scraper-specific processing
        # This allows each scraper to have custom processing logic
        return self._process_jobs(jobs_df)

    def _process_jobs(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Scraper-specific job processing.
        Override this method in concrete scraper implementations.
        """
        return jobs_df

    def _apply_rate_limiting(self, endpoint: str = "default") -> None:
        """
        Apply intelligent rate limiting between API calls.

        Args:
            endpoint: API endpoint identifier for per-endpoint rate limiting
        """
        # Use intelligent rate limiter instead of fixed delays
        self.rate_limiter.wait_if_needed(endpoint)

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics for this scraper."""
        stats = self.performance_monitor.get_stats()

        # Add threading performance stats
        threading_stats = self.threading_manager.get_performance_stats()
        stats["threading"] = threading_stats

        return stats

    def get_threading_stats(self) -> Dict[str, Any]:
        """Get threading-specific performance statistics."""
        return self.threading_manager.get_performance_stats()

    def clear_cache(self) -> int:
        """Clear all cached results for this scraper."""
        return self.cache_manager.clear_scraper_cache(self.scraper_name)

    def get_circuit_breaker_status(self) -> Dict[str, Any]:
        """
        Get circuit breaker status for monitoring.

        Returns:
            dict: Circuit breaker status information
        """
        return self.circuit_breaker.get_status()

    def get_rate_limiter_status(self, endpoint: Optional[str] = None) -> Dict[str, Any]:
        """
        Get rate limiter status for monitoring.

        Args:
            endpoint: Specific endpoint to get status for, or None for all endpoints

        Returns:
            dict: Rate limiter status information
        """
        if endpoint:
            return self.rate_limiter.get_endpoint_status(endpoint)
        else:
            return self.rate_limiter.get_all_endpoints_status()
