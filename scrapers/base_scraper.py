"""
Base scraper class with standardized filtering architecture.

This module defines the interface between API-level filters (what scraping libraries support)
and post-processing filters (what we apply after getting raw results).

Architecture:
- API Filters: Passed directly to the scraping library (faster, fewer results)
- Post-Processing Filters: Applied to scraped data (more flexible, universal)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Tuple
import pandas as pd
import time


class BaseJobScraper(ABC):
    """
    Abstract base class for all job scrapers.
    
    Defines clear separation between API-level and post-processing filters.
    Each scraper implementation specifies what filters it supports at API level.
    """
    
    def __init__(self):
        self.last_search_time = 0
        self.min_delay = 1.0  # Minimum delay between searches
        
    # Abstract methods that each scraper must implement
    
    @abstractmethod
    def get_supported_api_filters(self) -> Dict[str, bool]:
        """
        Return which filters this scraper supports at API level.
        
        Returns:
            Dict mapping filter names to support status:
            {
                'job_type': True,
                'remote_level': True, 
                'time_filter': True,
                'location': True,
                'salary_min': False,  # Not supported by API
                'salary_currency': False,  # Not supported by API
                'company_size': False,  # Not supported by API
            }
        """
        pass
    
    @abstractmethod
    def _build_api_search_params(self, **filters) -> Dict[str, Any]:
        """
        Build search parameters for the scraping API.
        Only includes filters supported at API level.
        
        Args:
            **filters: All user-requested filters
            
        Returns:
            Dict of parameters to pass to the scraping library
        """
        pass
    
    @abstractmethod
    def _call_scraping_api(self, search_params: Dict[str, Any]) -> pd.DataFrame:
        """
        Call the actual scraping library/API.
        
        Args:
            search_params: Parameters to pass to scraping library
            
        Returns:
            Raw DataFrame from scraping library
        """
        pass
    
    # Universal post-processing filter methods
    
    def apply_post_processing_filters(self, jobs_df: pd.DataFrame, **filters) -> pd.DataFrame:
        """
        Apply filters that aren't supported at API level.
        These work across all scrapers universally.
        
        Args:
            jobs_df: Raw scraped jobs DataFrame
            **filters: All user-requested filters
            
        Returns:
            Filtered DataFrame
        """
        if jobs_df.empty:
            return jobs_df
        
        filtered_df = jobs_df.copy()
        supported_api_filters = self.get_supported_api_filters()
        
        # Apply each post-processing filter if not handled by API
        if not supported_api_filters.get('salary_currency', False):
            salary_currency = filters.get('salary_currency', 'Any')
            if salary_currency != 'Any':
                filtered_df = self._filter_by_salary_currency(filtered_df, salary_currency)
        
        if not supported_api_filters.get('company_size', False):
            company_size = filters.get('company_size')
            if company_size:
                filtered_df = self._filter_by_company_size(filtered_df, company_size)
        
        if not supported_api_filters.get('salary_range', False):
            salary_min = filters.get('salary_min')
            salary_max = filters.get('salary_max')
            if salary_min or salary_max:
                filtered_df = self._filter_by_salary_range(filtered_df, salary_min, salary_max)
        
        return filtered_df
    
    def _filter_by_salary_currency(self, jobs_df: pd.DataFrame, target_currency: str) -> pd.DataFrame:
        """
        Universal salary currency filter (works across all scrapers).
        
        Args:
            jobs_df: Jobs DataFrame
            target_currency: Target currency code (USD, EUR, etc.)
            
        Returns:
            Filtered DataFrame
        """
        if target_currency == "Any" or jobs_df.empty:
            return jobs_df
        
        # Create currency patterns for different formats
        currency_patterns = {
            'USD': ['$', 'USD', 'US$', 'dollar'],
            'EUR': ['€', 'EUR', 'euro'],
            'GBP': ['£', 'GBP', 'pound', 'sterling'],
            'CAD': ['CAD', 'C$', 'canadian'],
            'AUD': ['AUD', 'A$', 'australian'],
            'BRL': ['R$', 'BRL', 'real', 'reais']
        }
        
        if target_currency.upper() not in currency_patterns:
            return jobs_df
        
        patterns = currency_patterns[target_currency.upper()]
        currency_mask = pd.Series([False] * len(jobs_df), index=jobs_df.index)
        
        # Check multiple columns for currency information
        currency_columns = ['currency', 'salary_currency', 'salary_formatted', 'compensation']
        
        for col in currency_columns:
            if col in jobs_df.columns:
                for pattern in patterns:
                    matches = jobs_df[col].fillna('').astype(str).str.contains(
                        pattern, case=False, na=False, regex=False  # Disable regex for exact matching
                    )
                    currency_mask |= matches
        
        return jobs_df[currency_mask]
    
    def _filter_by_company_size(self, jobs_df: pd.DataFrame, company_size: str) -> pd.DataFrame:
        """
        Universal company size filter.
        
        Args:
            jobs_df: Jobs DataFrame
            company_size: Company size category
            
        Returns:
            Filtered DataFrame
        """
        # Implementation for company size filtering
        # This would check company_num_employees or similar columns
        return jobs_df  # Placeholder
    
    def _filter_by_salary_range(self, jobs_df: pd.DataFrame, min_salary: Optional[int], max_salary: Optional[int]) -> pd.DataFrame:
        """
        Universal salary range filter.
        
        Args:
            jobs_df: Jobs DataFrame
            min_salary: Minimum salary
            max_salary: Maximum salary
            
        Returns:
            Filtered DataFrame
        """
        # Implementation for salary range filtering
        # This would parse min_amount, max_amount columns
        return jobs_df  # Placeholder
    
    # Core search method template
    
    def search_jobs(self, **filters) -> Dict[str, Any]:
        """
        Main search method that combines API and post-processing filters.
        
        Args:
            **filters: All user-requested filters
            
        Returns:
            Dict with search results and metadata including search_time
        """
        start_time = time.time()
        
        try:
            # Rate limiting
            self._enforce_rate_limit()
            
            # Build API search parameters (only supported filters)
            api_params = self._build_api_search_params(**filters)
            
            # Call scraping API
            raw_jobs = self._call_scraping_api(api_params)
            
            # Apply post-processing filters (unsupported by API)
            filtered_jobs = self.apply_post_processing_filters(raw_jobs, **filters)
            
            # Format and clean data
            processed_jobs = self._process_jobs(filtered_jobs)
            
            search_time = time.time() - start_time
            
            return {
                "success": True,
                "jobs": processed_jobs,
                "count": len(processed_jobs),
                "search_time": search_time,  # Dashboard expects this field
                "message": f"Found {len(processed_jobs)} jobs",
                "metadata": {
                    "api_filters_used": list(api_params.keys()),
                    "post_processing_applied": self._get_post_processing_filters_used(**filters)
                }
            }
            
        except Exception as e:
            search_time = time.time() - start_time
            return {
                "success": False,
                "jobs": None,
                "count": 0,
                "search_time": search_time,  # Include timing even for errors
                "message": f"Search failed: {str(e)}",
                "metadata": {"error": str(e)}
            }
    
    def _get_post_processing_filters_used(self, **filters) -> List[str]:
        """Get list of post-processing filters that were applied."""
        used_filters = []
        supported_api = self.get_supported_api_filters()
        
        if not supported_api.get('salary_currency', False) and filters.get('salary_currency', 'Any') != 'Any':
            used_filters.append('salary_currency')
        
        if not supported_api.get('company_size', False) and filters.get('company_size'):
            used_filters.append('company_size')
        
        return used_filters
    
    # Utility methods
    
    def _enforce_rate_limit(self):
        """Enforce minimum delay between searches."""
        current_time = time.time()
        time_since_last = current_time - self.last_search_time
        
        if time_since_last < self.min_delay:
            time.sleep(self.min_delay - time_since_last)
        
        self.last_search_time = time.time()
    
    def _process_jobs(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and format job data for display.
        Default implementation - can be overridden by specific scrapers.
        """
        if jobs_df.empty:
            return jobs_df
        
        processed = jobs_df.copy()
        
        # Ensure basic columns exist
        required_columns = ['title', 'company', 'location', 'date_posted', 'job_url']
        for col in required_columns:
            if col not in processed.columns:
                processed[col] = None
        
        return processed


class FilterCapabilities:
    """
    Helper class to define filter capabilities across different scrapers.
    """
    
    # Standard filter categories
    API_FILTERS = {
        'search_term': 'Job title/keywords search',
        'location': 'Geographic location',
        'job_type': 'Employment type (full-time, contract, etc.)',
        'remote_level': 'Remote work level',
        'time_filter': 'Job posting age',
        'results_wanted': 'Number of results to fetch'
    }
    
    POST_PROCESSING_FILTERS = {
        'salary_currency': 'Filter by salary currency',
        'salary_range': 'Filter by salary amount range', 
        'company_size': 'Filter by company size',
        'keywords_exclude': 'Exclude jobs with certain keywords',
        'experience_level': 'Filter by experience level required'
    }
    
    @classmethod
    def get_all_filters(cls) -> Dict[str, str]:
        """Get all available filters with descriptions."""
        return {**cls.API_FILTERS, **cls.POST_PROCESSING_FILTERS}
    
    @classmethod
    def is_api_filter(cls, filter_name: str) -> bool:
        """Check if a filter is typically handled at API level."""
        return filter_name in cls.API_FILTERS
    
    @classmethod
    def is_post_processing_filter(cls, filter_name: str) -> bool:
        """Check if a filter is typically handled via post-processing."""
        return filter_name in cls.POST_PROCESSING_FILTERS
