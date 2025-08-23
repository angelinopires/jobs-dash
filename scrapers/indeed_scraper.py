"""
Enhanced Indeed scraper for the job dashboard.
Handles all Indeed-specific parameters and provides better error handling.
"""

import time
import pandas as pd
import streamlit as st
from typing import Optional, List, Dict, Any
from jobspy import scrape_jobs
from jobspy.model import JobType

from .base_scraper import BaseJobScraper
from config.countries import get_indeed_country_name
from config.remote_filters import (
    get_global_countries, 
    enhance_search_term_with_remote_keywords,
    get_country_flag_and_name,
    get_remote_level_code
)
from utils.time_filters import get_hours_from_filter

class IndeedScraper(BaseJobScraper):
    """Enhanced Indeed scraper with better parameter handling and error management."""
    
    def __init__(self):
        super().__init__()
        self.min_delay = 2  # Override base class delay for Indeed-specific rate limiting
    
    def get_supported_api_filters(self) -> Dict[str, bool]:
        """
        Return which filters Indeed supports at API level.
        
        Indeed via JobSpy supports these filters directly in the API call:
        - search_term: Job title/keywords ✓
        - location: Geographic location ✓  
        - remote_level: Remote work level ✓ (via location="remote")
        - time_filter: Job posting age ✓ (via hours_old)
        
        These require post-processing:
        - job_type: Employment type ✗ (handled entirely via post-processing)
        - salary_currency: Not supported by JobSpy API ✗
        - salary_range: Not supported by JobSpy API ✗
        - company_size: Not supported by JobSpy API ✗
        """
        return {
            'search_term': True,
            'location': True,
            'job_type': False,        # Handled entirely via post-processing
            'remote_level': True, 
            'time_filter': True,
            'results_wanted': True,
            'salary_currency': False,  # Need post-processing
            'salary_min': False,      # Need post-processing
            'salary_max': False,      # Need post-processing
            'company_size': False,    # Need post-processing
        }
    
    def _build_api_search_params(self, **filters) -> Dict[str, Any]:
        """
        Build JobSpy API parameters from user filters.
        Only includes filters that Indeed/JobSpy supports natively.
        """
        supported_filters = self.get_supported_api_filters()
        
        # Base parameters
        search_params = {
            "site_name": ["indeed"],
            "results_wanted": filters.get('results_wanted', 1000),
        }
        
        # Add search term (always supported)
        if filters.get('search_term'):
            # Enhance with remote keywords for better remote job targeting
            search_params["search_term"] = enhance_search_term_with_remote_keywords(
                filters['search_term']
            )
        
        # Add location/country (convert to Indeed format)
        where = filters.get('where', filters.get('location', ''))
        if where and where != "Global":
            search_params["country_indeed"] = get_indeed_country_name(where)
        
        # Handle remote level (JobSpy uses location="remote" for remote jobs)
        if supported_filters.get('remote_level', False):
            remote_level = filters.get('remote_level', 'Any')
            remote_code = get_remote_level_code(remote_level)
            if remote_code == "FULLY_REMOTE":
                search_params["location"] = "remote"
        
        # Add time filter if supported
        if supported_filters.get('time_filter', False):
            time_filter = filters.get('time_filter', 'Any')
            if time_filter and time_filter != 'Any':
                hours_old = get_hours_from_filter(time_filter)
                if hours_old is not None:
                    search_params["hours_old"] = hours_old
        
        # Add proxies if provided
        if filters.get('proxies'):
            search_params["proxies"] = filters['proxies']
        
        return search_params
    
    def _call_scraping_api(self, search_params: Dict[str, Any]) -> pd.DataFrame:
        """
        Call JobSpy's scrape_jobs function with Indeed-specific parameters.
        """
        try:
            jobs_df = scrape_jobs(**search_params)
            return jobs_df if not jobs_df.empty else pd.DataFrame()
        except Exception as e:
            # Log the error but return empty DataFrame so base class can handle it
            print(f"JobSpy API call failed: {str(e)}")
            return pd.DataFrame()
        
    def search_jobs(
        self,
        search_term: str = "",
        where: str = "",
        remote_level: str = "Fully Remote",
        salary_currency: str = "Any",
        time_filter: str = "1 week or more",
        results_wanted: int = 1000,
        proxies: Optional[List[str]] = None,
        progress_callback=None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Search for jobs using Indeed. Supports both single-country and global searches.
        
        This method extends the base class to handle Indeed's special global search capability.
        For single countries, it uses the standard base class flow.
        For global searches, it implements custom multi-country logic.
        
        Args:
            search_term: Job title or keywords
            where: "Global" for multi-country search or specific country name
            remote_level: Remote level 
            salary_currency: Preferred salary currency
            time_filter: Job posting age filter
            results_wanted: Number of jobs to fetch (default: 1000 for maximum results)
            proxies: List of proxy servers
            progress_callback: Function to update progress
            **kwargs: Additional filters
            
        Returns:
            Dict with results and metadata
        """
        # Package all filters for base class
        all_filters = {
            'search_term': search_term,
            'where': where,
            'location': where,  # Alias for compatibility
            'remote_level': remote_level,
            'salary_currency': salary_currency,
            'time_filter': time_filter,
            'results_wanted': results_wanted,
            'proxies': proxies,
            **kwargs
        }
        
        # Handle global searches (special Indeed feature)
        if where == "Global":
            return self._search_global_remote_jobs(
                all_filters, progress_callback
            )
        else:
            # Use base class for single-country searches
            return super().search_jobs(**all_filters)
    
    def _search_global_remote_jobs(
        self, filters: Dict[str, Any], progress_callback=None
    ) -> Dict[str, Any]:
        """Search for remote jobs across multiple countries using base class architecture."""
        global_countries = get_global_countries()
        all_jobs = []
        total_start_time = time.time()
        
        if progress_callback:
            progress_callback("Starting global remote job search...", 0.1)
        
        for i, (flag, country_name, country_code) in enumerate(global_countries):
            try:
                # Update progress - show 100% when searching the last country
                if i == len(global_countries) - 1:
                    # Last country - show 100%
                    progress_percent = 1.0
                else:
                    # For other countries, scale from 10% to 95%
                    progress_percent = 0.1 + ((i + 0.5) / len(global_countries)) * 0.85  # 10% to 95%
                
                if progress_callback:
                    progress_callback(f"Searching {flag} {country_name} ({i+1}/{len(global_countries)})...", progress_percent)
                
                # Create country-specific filters
                country_filters = filters.copy()
                country_filters['where'] = country_name
                country_filters['location'] = country_name
                
                # Use base class search for this country
                country_result = super().search_jobs(**country_filters)
                
                if country_result["success"] and country_result["jobs"] is not None:
                    # Add country metadata to jobs
                    country_jobs = country_result["jobs"].copy()
                    country_jobs["country_flag"] = flag
                    country_jobs["country_name"] = country_name 
                    country_jobs["country_code"] = country_code
                    all_jobs.append(country_jobs)
                
                # Rate limiting between countries
                time.sleep(1)
                
            except Exception as e:
                if progress_callback:
                    progress_callback(f"⚠️ Failed to search {country_name}: {str(e)}", progress_percent)
                continue
        
        # Combine results
        if not all_jobs:
            total_time = time.time() - total_start_time
            return {
                "success": True,
                "jobs": pd.DataFrame(),
                "count": 0,
                "search_time": total_time,  # ✅ Add missing search_time
                "message": f"No remote jobs found globally for '{filters.get('search_term', '')}'",
                "metadata": {"search_type": "global", "countries_searched": 0}
            }
        
        # Concatenate all job DataFrames
        combined_jobs = pd.concat(all_jobs, ignore_index=True)
        
        # Remove duplicates based on job_url
        if 'job_url' in combined_jobs.columns:
            combined_jobs = combined_jobs.drop_duplicates(subset=['job_url'], keep='first')
        
        total_time = time.time() - total_start_time
        countries_searched = len(all_jobs)
        
        if progress_callback:
            progress_callback(f"✅ Global search complete! Found {len(combined_jobs)} remote jobs", 1.0)
        
        return {
            "success": True,
            "jobs": combined_jobs,
            "count": len(combined_jobs),
            "search_time": total_time,  # ✅ Add missing search_time
            "message": f"Found {len(combined_jobs)} remote jobs across {countries_searched} countries",
            "metadata": {
                "search_type": "global",
                "countries_searched": countries_searched,
                "total_countries": len(global_countries),
                "api_filters_used": list(self._build_api_search_params(**filters).keys()),
                "post_processing_applied": self._get_post_processing_filters_used(**filters)
            }
        }
    
    # Override base class _process_jobs to add Indeed-specific formatting
    
    def _process_jobs(self, jobs_df):
        """Process and clean the jobs DataFrame for display."""
        if jobs_df.empty:
            return jobs_df
        
        # Ensure all required columns exist (Note: JobSpy uses 'company', not 'company_name')
        required_columns = [
            'title', 'company', 'location', 'date_posted', 
            'site', 'job_url', 'description', 'is_remote'
        ]
        
        for col in required_columns:
            if col not in jobs_df.columns:
                jobs_df[col] = None
        
        # Add job_type column if not present (derive from title/description)
        if 'job_type' not in jobs_df.columns:
            jobs_df['job_type'] = self._derive_job_type(jobs_df)
        
        # Clean and format data
        processed_jobs = jobs_df.copy()
        
        # Clean company names (replace empty/None with "Not specified")
        if 'company' in processed_jobs.columns:
            processed_jobs['company'] = processed_jobs['company'].fillna("Not specified")
            processed_jobs['company'] = processed_jobs['company'].replace('', "Not specified")
            # Also create company_name alias for dashboard compatibility
            processed_jobs['company_name'] = processed_jobs['company']
        
        # Format location
        if 'location' in processed_jobs.columns:
            processed_jobs['location_formatted'] = processed_jobs['location'].apply(
                self._format_location
            )
        
        # Format salary information (JobSpy provides individual columns)
        if 'min_amount' in processed_jobs.columns or 'max_amount' in processed_jobs.columns:
            processed_jobs['salary_formatted'] = processed_jobs.apply(
                self._format_salary_from_columns, axis=1
            )
        elif 'compensation' in processed_jobs.columns:
            processed_jobs['salary_formatted'] = processed_jobs['compensation'].apply(
                self._format_salary
            )
        else:
            # If no salary columns, create empty salary column
            processed_jobs['salary_formatted'] = "Not specified"
        
        # Add company info
        if 'company_industry' in processed_jobs.columns:
            processed_jobs['company_info'] = processed_jobs.apply(
                self._format_company_info, axis=1
            )
        
        # Format posted date
        if 'date_posted' in processed_jobs.columns:
            processed_jobs['date_posted_formatted'] = processed_jobs['date_posted'].apply(
                self._format_posted_date
            )
        
        return processed_jobs
    
    def _derive_job_type(self, jobs_df):
        """
        Derive job type from job title and description.
        
        Args:
            jobs_df: Jobs DataFrame
            
        Returns:
            Series with job types
        """
        import pandas as pd
        
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
        """Format location object for display."""
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
    
    def _format_salary(self, compensation):
        """Format compensation object for display."""
        if not compensation:
            return "Not specified"
        
        try:
            # Handle different compensation formats
            if hasattr(compensation, 'min_amount') or hasattr(compensation, 'max_amount'):
                min_amount = getattr(compensation, 'min_amount', None)
                max_amount = getattr(compensation, 'max_amount', None)
                currency = getattr(compensation, 'currency', 'USD')
                interval = getattr(compensation, 'interval', None)
                
                # Format interval for display
                interval_str = ""
                if interval:
                    if hasattr(interval, 'value'):
                        interval_str = f" ({interval.value})"
                    else:
                        interval_str = f" ({str(interval)})"
                
                if min_amount and max_amount:
                    return f"{currency} {min_amount:,} - {max_amount:,}{interval_str}"
                elif min_amount:
                    return f"{currency} {min_amount:,}+{interval_str}"
                elif max_amount:
                    return f"Up to {currency} {max_amount:,}{interval_str}"
                else:
                    return "Not specified"
            elif isinstance(compensation, dict):
                # Handle dict format
                min_amt = compensation.get('min_amount')
                max_amt = compensation.get('max_amount')
                currency = compensation.get('currency', 'USD')
                
                if min_amt and max_amt:
                    return f"{currency} {min_amt:,} - {max_amt:,}"
                elif min_amt:
                    return f"{currency} {min_amt:,}+"
                elif max_amt:
                    return f"Up to {currency} {max_amt:,}"
                else:
                    return "Not specified"
            else:
                # Fallback to string representation
                comp_str = str(compensation)
                return comp_str if comp_str and comp_str != "None" else "Not specified"
        except Exception as e:
            # Debug: return the error info
            return f"Parse error: {type(compensation).__name__}"
    
    def _format_salary_from_columns(self, row):
        """Format salary from individual JobSpy columns (min_amount, max_amount, etc.)."""
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
                
        except Exception as e:
            return "Not specified"
    
    def _format_company_info(self, row):
        """Format company information for display."""
        info_parts = []
        
        if row.get('company_industry'):
            info_parts.append(f"Industry: {row['company_industry']}")
        
        if row.get('company_num_employees'):
            info_parts.append(f"Size: {row['company_num_employees']}")
        
        if row.get('company_revenue'):
            info_parts.append(f"Revenue: {row['company_revenue']}")
        
        return " | ".join(info_parts) if info_parts else "N/A"
    
    def _format_posted_date(self, date_posted):
        """
        Format posted date from timestamp to readable format.
        
        Args:
            date_posted: Could be timestamp, date string, or date object
            
        Returns:
            Formatted date string like "Dec 15, 2023 2:30 PM" or "2 days ago"
        """
        if not date_posted:
            return "N/A"
        
        try:
            import datetime as dt
            
            # Handle different input formats
            if isinstance(date_posted, str):
                # Try to parse ISO format or other common formats
                try:
                    if date_posted.isdigit():
                        # Timestamp as string
                        timestamp = int(date_posted)
                    else:
                        # Parse date string
                        parsed_date = pd.to_datetime(date_posted)
                        return parsed_date.strftime("%b %d, %Y %I:%M %p")
                except:
                    return date_posted  # Return as-is if can't parse
            
            elif isinstance(date_posted, (int, float)):
                # Timestamp (could be in seconds or milliseconds)
                timestamp = int(date_posted)
                
                # Check if timestamp is in milliseconds (common in web APIs)
                if timestamp > 1e10:  # Likely milliseconds
                    timestamp = timestamp // 1000
                
                date_obj = dt.datetime.fromtimestamp(timestamp)
                
            elif hasattr(date_posted, 'strftime'):
                # Already a datetime object
                date_obj = date_posted
            else:
                return str(date_posted)
            
            # Format the date
            now = dt.datetime.now()
            diff = now - date_obj
            
            # Show relative time for recent posts
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
                # Full date for older posts - match search history format
                return date_obj.strftime("%b %d, %Y %H:%M")
                
        except Exception as e:
            # Fallback to string representation
            return str(date_posted) if date_posted else "N/A"
    
    def get_supported_parameters(self) -> Dict[str, Any]:
        """Get information about supported parameters."""
        return {
            "search_term": "Job title or keywords",
            "location": "Geographic location (city, state, country)",
            "country": "Country for Indeed search (60+ supported)",
            "distance": "Search radius in miles (default: 50)",
            "is_remote": "Filter for remote jobs",
            "time_filter": "Filter by posting age (24h, 72h, 1 week, or no filter)",
            "results_wanted": "Number of jobs to fetch (default: 1000 for maximum results)",
            "proxies": "List of proxy servers for rotation"
        }
    
    def get_limitations(self) -> List[str]:
        """Get list of Indeed scraper limitations."""
        return [
            "Maximum of 1000 jobs per search",
            "Rate limiting may apply without proxies",
            "Some job details may be incomplete",
            "Job type filtering handled via post-processing (more accurate)"
        ]
