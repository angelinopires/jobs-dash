"""
Abstract base scraper class with standardized optimization interface.

This replaces the old scrapers/base_scraper.py with a cleaner architecture
that separates core functionality from scraper-specific implementations.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Callable
import pandas as pd
import time
from datetime import datetime

from .performance_monitor import PerformanceMonitor
from .cache_manager import CacheManager
from .threading_manager import ThreadingManager


class BaseScraper(ABC):
    """
    Enhanced base class for all job scrapers with built-in optimizations.
    
    This class provides:
    - Standardized caching across all scrapers
    - Performance monitoring and logging
    - Parallel result processing capabilities
    - Common error handling and fallback strategies
    """
    
    def __init__(self, scraper_name: str):
        self.scraper_name = scraper_name
        self.performance_monitor = PerformanceMonitor(scraper_name)
        self.cache_manager = CacheManager()
        self.threading_manager = ThreadingManager(max_workers=4)  # Phase 2: Parallel processing
        self.last_search_time = 0
        self.min_delay = 1.0  # Minimum delay between API calls
        
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
    def _build_api_search_params(self, **filters) -> Dict[str, Any]:
        """Build search parameters for the scraping API."""
        pass
    
    @abstractmethod
    def _call_scraping_api(self, search_params: Dict[str, Any]) -> pd.DataFrame:
        """Call the actual scraping library/API."""
        pass
    
    def search_jobs(
        self,
        search_term: str = "",
        where: str = "",
        include_remote: bool = True,
        **kwargs
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
                progress_callback = kwargs.pop('progress_callback', None)
                result = self._search_global_optimized(
                    search_term, include_remote, progress_callback, **kwargs
                )
            else:
                result = self._search_single_country_optimized(
                    search_term, where, include_remote, **kwargs
                )
            
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
                "metadata": {"error": str(e)}
            }
    
    def _search_single_country_optimized(
        self, search_term: str, country: str, include_remote: bool, **kwargs
    ) -> Dict[str, Any]:
        """Optimized single-country search with caching."""
        
        # Filter out function references from kwargs to avoid JSON serialization issues
        filtered_kwargs = {k: v for k, v in kwargs.items() if not callable(v)}
        
        # Check cache first
        cache_key = self.cache_manager.generate_cache_key(
            scraper=self.scraper_name,
            search_term=search_term,
            country=country,
            include_remote=include_remote,
            **filtered_kwargs
        )
        
        cached_result = self.cache_manager.get_cached_result(cache_key)
        if cached_result:
            # Get cache entry for expiration info
            cache_entry = self.cache_manager.get_cache_entry_info(cache_key)
            self.performance_monitor.log_cache_event("hit", cache_key, country, cache_entry)
            return cached_result
        
        # No cache hit - perform actual search
        self.performance_monitor.log_cache_event("miss", cache_key, country)
        
        # Rate limiting
        self._apply_rate_limiting()
        
        # Build API parameters (filter out function references)
        filters = {
            'search_term': search_term,
            'where': country,
            'location': country,
            'include_remote': include_remote,
            **filtered_kwargs
        }
        
        api_params = self._build_api_search_params(**filters)
        
        # Call scraping API
        start_time = time.time()
        jobs_df = self._call_scraping_api(api_params)
        api_time = time.time() - start_time
        
        # Process results
        if not jobs_df.empty:
            processed_jobs = self._process_jobs_optimized(jobs_df)
            
            result = {
                "success": True,
                "jobs": processed_jobs,
                "count": len(processed_jobs),
                "message": f"Found {len(processed_jobs)} jobs in {country}",
                "metadata": {
                    "country": country,
                    "api_time": api_time,
                    "scraper": self.scraper_name
                }
            }
        else:
            result = {
                "success": True,
                "jobs": pd.DataFrame(),
                "count": 0,
                "message": f"No jobs found in {country}",
                "metadata": {
                    "country": country,
                    "api_time": api_time,
                    "scraper": self.scraper_name
                }
            }
        
        # Cache the result
        self.cache_manager.cache_result(cache_key, result)
        
        return result
    
    def _search_global_optimized(
        self, search_term: str, include_remote: bool, progress_callback: Optional[Callable], **kwargs
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
                    "scraper": self.scraper_name
                }
            }
        
        # Extract additional parameters for threading
        time_filter = kwargs.get('time_filter')
        
        # Use threading manager for parallel processing
        result = self.threading_manager.search_countries_parallel(
            countries=countries,
            search_func=self._search_single_country_optimized,
            search_term=search_term,
            include_remote=include_remote,
            time_filter=time_filter,
            progress_callback=progress_callback
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
            f"(speedup: {speedup:.1f}x, success: {success_rate:.1%})"
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
    
    def _apply_rate_limiting(self):
        """Apply rate limiting between API calls."""
        current_time = time.time()
        time_since_last = current_time - self.last_search_time
        
        if time_since_last < self.min_delay:
            sleep_time = self.min_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_search_time = time.time()
    
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
    
    def clear_cache(self):
        """Clear all cached results for this scraper."""
        self.cache_manager.clear_scraper_cache(self.scraper_name)
