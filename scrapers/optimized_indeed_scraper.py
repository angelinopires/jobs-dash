"""
Optimized Indeed scraper using the new core architecture.

This replaces the old indeed_scraper.py with a version that leverages:
- Smart caching system
- Performance monitoring
- Optimization framework
- Multi-scraper preparation
"""

import time
import pandas as pd
import streamlit as st
import logging
import urllib.parse
from typing import Optional, List, Dict, Any
from jobspy import scrape_jobs

# New core architecture imports
from core.base_scraper import BaseScraper
from core.base_optimizer import SearchOptimizer
from utils.constants import INVALID_VALUES

# Import display functions for cleaning
from utils.display_utils import clean_display_value

# Existing config imports
from config.countries import get_indeed_country_name
from config.remote_filters import get_global_countries
from utils.time_filters import get_hours_from_filter


class OptimizedIndeedScraper(BaseScraper):
    """
    Indeed scraper with full optimization framework.
    
    Features:
    - Automatic caching (15min TTL)
    - Performance monitoring and logging
    - Memory-optimized result processing
    - Preparation for parallel processing
    - Clean separation of API and post-processing filters
    """
    
    def __init__(self):
        super().__init__("indeed")  # Initialize base scraper
        self.min_delay = 2  # Indeed-specific rate limiting
        self.optimizer = SearchOptimizer("indeed")
        
        # Setup Indeed-specific logging
        self.logger = logging.getLogger("scraper.indeed")
    
    def get_supported_countries(self) -> List[str]:
        """Return list of countries Indeed supports for global searches."""
        return [country_name for country_name, _ in get_global_countries()]
    
    def get_supported_api_filters(self) -> Dict[str, bool]:
        """
        Return which filters Indeed supports at API level.
        
        Indeed via JobSpy supports these filters directly:
        - search_term: Job title/keywords ✓
        - location: Geographic location ✓  
        - time_filter: Job posting age ✓ (via hours_old)
        - results_wanted: Number of results ✓
        
        These require post-processing:
        - job_type: Employment type ✗
        - salary_currency: Not supported by JobSpy API ✗
        - company_size: Not supported by JobSpy API ✗
        """
        return {
            'search_term': True,
            'location': True,
            'job_type': False,        # Post-processing only
            'time_filter': True,
            'results_wanted': True,
            'salary_currency': False,  # Post-processing only
            'salary_min': False,      # Post-processing only
            'salary_max': False,      # Post-processing only
            'company_size': False,    # Post-processing only
        }
    
    def _build_api_search_params(self, **filters) -> Dict[str, Any]:
        """
        Build JobSpy API parameters from user filters.
        Only includes filters that Indeed supports natively.
        """
        # Apply search optimizations first
        optimized_filters = self.optimizer.optimize_search_params(**filters)
        
        # Base parameters
        search_params = {
            "site_name": ["indeed"],
            "results_wanted": optimized_filters.get('results_wanted', 500),  # Optimized default
        }
        
        # Add search term (always supported)
        if optimized_filters.get('search_term'):
            search_params["search_term"] = optimized_filters['search_term']
        
        # Add location/country (convert to Indeed format)
        where = optimized_filters.get('where', optimized_filters.get('location', ''))
        if where and where != "Global":
            search_params["country_indeed"] = get_indeed_country_name(where)
        
        # Handle remote checkbox (set location="remote" when checked)
        if optimized_filters.get('include_remote', False):
            search_params["location"] = "remote"
        
        # Add time filter if supported
        time_filter = optimized_filters.get('time_filter', 'Any')
        if time_filter and time_filter != 'Any':
            hours_old = get_hours_from_filter(time_filter)
            if hours_old is not None:
                search_params["hours_old"] = hours_old
        
        return search_params
    
    def _call_scraping_api(self, search_params: Dict[str, Any]) -> pd.DataFrame:
        """
        Call JobSpy's scrape_jobs function with Indeed-specific parameters.
        Enhanced with performance monitoring and error handling.
        """
        try:
            # Log the API call for monitoring
            self._log_indeed_api_call(search_params)
            
            # Record API call start time
            api_start = time.time()
            
            # Make the API call
            jobs_df = scrape_jobs(**search_params)
            
            # Record performance metrics
            api_time = time.time() - api_start
            
            # Log API call completion
            site = "indeed"
            search_term = search_params.get('search_term', 'Unknown')
            approx_url = self._construct_indeed_url_preview(search_params)
            
            self.performance_monitor.log_api_call(
                site=site,
                search_term=search_term, 
                url=approx_url,
                response_time=api_time
            )
            
            if not jobs_df.empty:
                self.performance_monitor.log("API success", f"✅ {len(jobs_df)} jobs found")
            else:
                self.performance_monitor.log("API warning", "⚠️ No jobs found")
            
            return jobs_df if not jobs_df.empty else pd.DataFrame()
            
        except Exception as e:
            # Enhanced error handling with specific error types
            error_str = str(e)
            
            if "Read timed out" in error_str or "timeout" in error_str.lower():
                error_msg = "Indeed API request timed out"
                self.performance_monitor.log("API timeout", error_msg)
            elif "HTTPSConnectionPool" in error_str:
                error_msg = "Network connection issue with Indeed API"
                self.performance_monitor.log("API network", error_msg)
            elif "429" in error_str or "rate limit" in error_str.lower():
                error_msg = "Indeed API rate limit exceeded"
                self.performance_monitor.log("API rate limit", error_msg)
            else:
                error_msg = f"Indeed API error: {error_str}"
                self.performance_monitor.log("API error", error_msg)
            
            return pd.DataFrame()
    
    def _process_jobs(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process and clean jobs DataFrame with optimizations.
        
        This method applies Indeed-specific processing and then
        runs optimization passes for better performance.
        """
        if jobs_df.empty:
            return jobs_df
        
        # Apply Indeed-specific processing first
        processed_jobs = self._apply_indeed_processing(jobs_df)
        
        # Apply general optimizations
        optimized_jobs = self.optimizer.optimize_result_processing(processed_jobs)
        
        return optimized_jobs
    
    def _apply_indeed_processing(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """Apply Indeed-specific job processing logic."""
        # Ensure all required columns exist (JobSpy uses 'company', not 'company_name')
        required_columns = [
            'title', 'company', 'location', 'date_posted', 
            'site', 'job_url', 'description', 'is_remote'
        ]
        
        processed_jobs = jobs_df.copy()
        
        for col in required_columns:
            if col not in processed_jobs.columns:
                processed_jobs[col] = None
        
        # Add job_type column if not present
        if 'job_type' not in processed_jobs.columns:
            processed_jobs['job_type'] = self._derive_job_type(processed_jobs)
        
        # Clean company names
        if 'company' in processed_jobs.columns:
            processed_jobs['company'] = processed_jobs['company'].apply(
                lambda x: clean_display_value(x, "Not specified")
            )
            # Create company_name alias for dashboard compatibility
            processed_jobs['company_name'] = processed_jobs['company']
        
        # Format location
        if 'location' in processed_jobs.columns:
            processed_jobs['location_formatted'] = processed_jobs['location'].apply(
                lambda x: clean_display_value(self._format_location(x))
            )
        
        # Format salary information
        if 'min_amount' in processed_jobs.columns or 'max_amount' in processed_jobs.columns:
            processed_jobs['salary_formatted'] = processed_jobs.apply(
                lambda row: clean_display_value(self._format_salary_from_columns(row)), axis=1
            )
        else:
            processed_jobs['salary_formatted'] = "Not specified"
        
        # Format posted date
        if 'date_posted' in processed_jobs.columns:
            processed_jobs['date_posted_formatted'] = processed_jobs['date_posted'].apply(
                lambda x: clean_display_value(self._format_posted_date(x))
            )
        
        # Format company information
        processed_jobs['company_info'] = processed_jobs.apply(
            lambda row: clean_display_value(self._format_company_info(row)), axis=1
        )
        
        return processed_jobs
    
    def _derive_job_type(self, jobs_df: pd.DataFrame) -> pd.Series:
        """Derive job type from job title and description."""
        if jobs_df.empty:
            return pd.Series([], dtype=str)
        
        job_types = []
        
        # Job type keywords mapping
        type_keywords = {
            'Full-time': ['full time', 'full-time', 'permanent', 'salaried'],
            'Part-time': ['part time', 'part-time', 'hourly'],
            'Contract': ['contract', 'contractor', 'freelance', 'consulting', 'temporary', 'temp'],
            'Internship': ['intern', 'internship', 'co-op', 'trainee']
        }
        
        for idx, row in jobs_df.iterrows():
            title = str(row.get('title', '')).lower()
            description = str(row.get('description', '')).lower() 
            combined_text = f"{title} {description}"
            
            # Check for job type keywords
            detected_type = 'Full-time'  # Default
            
            for job_type, keywords in type_keywords.items():
                for keyword in keywords:
                    if keyword in combined_text:
                        detected_type = job_type
                        break
                if detected_type != 'Full-time':
                    break
            
            job_types.append(detected_type)
        
        return pd.Series(job_types, index=jobs_df.index)
    
    def _format_location(self, location):
        """Format location for display."""
        if not location:
            return "N/A"
        
        try:
            if hasattr(location, 'city') and hasattr(location, 'state'):
                city = location.city or ""
                state = location.state or ""
                country = getattr(location, 'country', None) or ""
                
                parts = [city, state, country]
                return ", ".join([p for p in parts if p])
            else:
                return str(location)
        except:
            return str(location)
    
    def _format_salary_from_columns(self, row):
        """Format salary from JobSpy columns (min_amount, max_amount, etc.)."""
        try:
            min_amount = row.get('min_amount')
            max_amount = row.get('max_amount')
            currency = row.get('currency', 'USD')
            interval = row.get('interval')
            
            # Clean up None/NaN values
            if pd.isna(min_amount):
                min_amount = None
            if pd.isna(max_amount):
                max_amount = None
            if pd.isna(currency) or currency == 'nan':
                currency = 'USD'
            
            # Format interval for display
            interval_str = ""
            if interval and not pd.isna(interval):
                interval_str = f" ({interval})"
            
            if min_amount and max_amount:
                return f"{currency} {int(min_amount):,} - {int(max_amount):,}{interval_str}"
            elif min_amount:
                return f"{currency} {int(min_amount):,}+{interval_str}"
            elif max_amount:
                return f"Up to {currency} {int(max_amount):,}{interval_str}"
            else:
                return "Not specified"
                
        except Exception:
            return "Not specified"
    
    def _format_company_info(self, row):
        """Format company information for display."""
        info_parts = []
        
        # Helper function to check if value is valid
        def is_valid_value(value):
            if value is None or pd.isna(value):
                return False
            str_value = str(value).strip().lower()
            return str_value not in [v.lower() for v in INVALID_VALUES]
        
        industry = row.get('company_industry')
        if not pd.isna(industry) and industry and is_valid_value(industry):
            info_parts.append(f"Industry: {industry}")
        
        size = row.get('company_num_employees')
        if not pd.isna(size) and size and is_valid_value(size):
            info_parts.append(f"Size: {size}")
        
        revenue = row.get('company_revenue')
        if not pd.isna(revenue) and revenue and is_valid_value(revenue):
            info_parts.append(f"Revenue: {revenue}")
        
        return " | ".join(info_parts) if info_parts else "N/A"
    
    def _format_salary(self, compensation):
        """Format salary information from JobSpy compensation field."""
        if not compensation or pd.isna(compensation):
            return "N/A"
        
        try:
            # Handle different compensation formats
            if isinstance(compensation, str):
                return compensation.strip()
            elif isinstance(compensation, (int, float)):
                return f"${compensation:,.0f}"
            else:
                return str(compensation)
        except:
            return "N/A"
    
    def _format_posted_date(self, date_posted):
        """Format posted date for display."""
        if not date_posted:
            return "N/A"
        
        try:
            import datetime as dt
            
            # Handle different input formats
            if isinstance(date_posted, str):
                if date_posted.isdigit():
                    timestamp = int(date_posted)
                    date_obj = dt.datetime.fromtimestamp(timestamp)
                else:
                    parsed_date = pd.to_datetime(date_posted)
                    return parsed_date.strftime("%b %d, %Y %I:%M %p")
            elif isinstance(date_posted, (int, float)):
                timestamp = int(date_posted)
                if timestamp > 1e10:  # Likely milliseconds
                    timestamp = timestamp // 1000
                date_obj = dt.datetime.fromtimestamp(timestamp)
            elif hasattr(date_posted, 'strftime'):
                date_obj = date_posted
            else:
                return str(date_posted)
            
            # Format relative time
            now = dt.datetime.now()
            diff = now - date_obj
            
            if diff.days == 0:
                hours = diff.seconds // 3600
                if hours < 1:
                    minutes = diff.seconds // 60
                    return f"{minutes} min ago" if minutes > 0 else "Just now"
                else:
                    return f"{hours}h ago"
            elif diff.days == 1:
                return "1 day ago"
            elif diff.days < 7:
                return f"{diff.days} days ago"
            else:
                return date_obj.strftime("%b %d, %Y %H:%M")
                
        except Exception:
            return str(date_posted) if date_posted else "N/A"
    
    def _log_indeed_api_call(self, search_params: Dict[str, Any]) -> None:
        """Log Indeed API call details for monitoring."""
        # Create readable summary
        log_parts = []
        
        if 'search_term' in search_params:
            log_parts.append(f"Term: {search_params['search_term']}")
        
        if 'country_indeed' in search_params:
            log_parts.append(f"Country: {search_params['country_indeed']}")
        elif 'location' in search_params:
            log_parts.append(f"Location: {search_params['location']}")
            
        if 'hours_old' in search_params:
            log_parts.append(f"Age: {search_params['hours_old']}h")
            
        if 'results_wanted' in search_params:
            log_parts.append(f"Results: {search_params['results_wanted']}")
        
        log_message = " | ".join(log_parts)
        self.performance_monitor.log("API call prep", log_message)
    
    def _construct_indeed_url_preview(self, search_params: Dict[str, Any]) -> str:
        """Construct approximate Indeed URL for logging."""
        base_url = "https://www.indeed.com/jobs"
        params = []
        
        if 'search_term' in search_params:
            encoded_term = urllib.parse.quote_plus(search_params['search_term'])
            params.append(f"q={encoded_term}")
        
        if 'location' in search_params:
            encoded_location = urllib.parse.quote_plus(search_params['location'])
            params.append(f"l={encoded_location}")
        elif 'country_indeed' in search_params:
            encoded_country = urllib.parse.quote_plus(search_params['country_indeed'])
            params.append(f"l={encoded_country}")
        
        if 'hours_old' in search_params:
            hours = search_params['hours_old']
            if hours <= 24:
                params.append("fromage=1")
            elif hours <= 72:
                params.append("fromage=3")
            elif hours <= 168:
                params.append("fromage=7")
            else:
                params.append("fromage=14")
        
        return f"{base_url}?{'&'.join(params)}" if params else base_url

# Create a function to get the scraper instance (maintains compatibility)
def get_indeed_scraper():
    """Get an optimized Indeed scraper instance."""
    return OptimizedIndeedScraper()
