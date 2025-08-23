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

from config.countries import get_indeed_country_name
from config.remote_filters import (
    get_global_countries, 
    enhance_search_term_with_remote_keywords,
    get_country_flag_and_name,
    get_job_type_code,
    get_remote_level_code
)
from utils.time_filters import get_hours_from_filter

class IndeedScraper:
    """Enhanced Indeed scraper with better parameter handling and error management."""
    
    def __init__(self):
        self.last_search_time = 0
        self.min_delay = 2  # Minimum delay between searches in seconds
        
    def search_jobs(
        self,
        search_term: str,
        where: str,
        job_type: str = "Full-time",
        remote_level: str = "Fully Remote",
        salary_currency: str = "Any",
        time_filter: str = "1 week or more",
        results_wanted: int = 20,
        proxies: Optional[List[str]] = None,
        progress_callback=None
    ) -> Dict[str, Any]:
        """
        Search for remote jobs using Indeed with enhanced parameters.
        
        Args:
            search_term: Job title or keywords (will be enhanced with remote keywords)
            where: "Global" for multi-country search or specific country name
            job_type: Job type filter ("Any", "Full-time", "Contract", "Part-time")
            remote_level: Remote level ("Fully Remote", "Hybrid", "Any")
            salary_currency: Preferred salary currency ("Any", "USD", "EUR", etc.)
            time_filter: User-friendly time filter option
            results_wanted: Number of jobs to fetch per country
            proxies: List of proxy servers
            progress_callback: Function to update progress
            
        Returns:
            Dict with results and metadata
        """
        try:
            # Rate limiting
            self._enforce_rate_limit()
            
            # Enhance search term with remote keywords for better remote job results
            enhanced_search_term = enhance_search_term_with_remote_keywords(search_term)
            
            # Convert filter parameters
            hours_old = get_hours_from_filter(time_filter)
            job_type_code = get_job_type_code(job_type)
            remote_level_code = get_remote_level_code(remote_level)
            
            # Determine search strategy
            if where == "Global":
                return self._search_global_remote_jobs(
                    enhanced_search_term, job_type_code, remote_level_code, 
                    salary_currency, hours_old, results_wanted, proxies, progress_callback
                )
            else:
                return self._search_country_remote_jobs(
                    enhanced_search_term, where, job_type_code, remote_level_code,
                    salary_currency, hours_old, results_wanted, proxies, progress_callback
                )
            
        except Exception as e:
            error_msg = f"Error during Indeed remote search: {str(e)}"
            return {
                "success": False,
                "jobs": None,
                "count": 0,
                "search_time": 0,
                "message": error_msg,
                "error": str(e)
            }
    
    def _search_global_remote_jobs(
        self, search_term: str, job_type_code: str, remote_level_code: str,
        salary_currency: str, hours_old: Optional[int], results_wanted: int,
        proxies: Optional[List[str]], progress_callback=None
    ) -> Dict[str, Any]:
        """Search for remote jobs across multiple countries."""
        global_countries = get_global_countries()
        all_jobs = []
        search_times = []
        total_start_time = time.time()
        
        if progress_callback:
            progress_callback("Starting global remote job search...", 0.1)
        
        for i, (flag, country_name, country_code) in enumerate(global_countries):
            try:
                # Update progress with percentage
                progress_percent = 0.2 + (i / len(global_countries)) * 0.6  # 20% to 80%
                if progress_callback:
                    progress_callback(f"Searching {flag} {country_name} ({i+1}/{len(global_countries)})...", progress_percent)
                
                # Search this country
                country_result = self._search_single_country(
                    search_term, country_code, job_type_code, remote_level_code,
                    hours_old, results_wanted, proxies
                )
                
                if country_result["success"] and country_result["jobs"] is not None:
                    # Add country information to each job
                    country_jobs = country_result["jobs"].copy()
                    country_jobs["country_flag"] = flag
                    country_jobs["country_name"] = country_name 
                    country_jobs["country_code"] = country_code
                    all_jobs.append(country_jobs)
                    search_times.append(country_result["search_time"])
                
                # Rate limiting between countries
                time.sleep(1)
                
            except Exception as e:
                if progress_callback:
                    progress_callback(f"⚠️ Failed to search {country_name}: {str(e)}", progress_percent)
                continue
        
        # Combine all results
        if not all_jobs:
            total_time = time.time() - total_start_time
            return {
                "success": True,
                "jobs": None,
                "count": 0,
                "search_time": total_time,
                "message": f"No remote jobs found globally for '{search_term}'"
            }
        
        # Concatenate all job DataFrames
        import pandas as pd
        combined_jobs = pd.concat(all_jobs, ignore_index=True)
        
        # Remove duplicates based on job_url
        if 'job_url' in combined_jobs.columns:
            combined_jobs = combined_jobs.drop_duplicates(subset=['job_url'], keep='first')
        
        # Process and clean the combined data
        processed_jobs = self._process_jobs(combined_jobs)
        
        # Apply salary currency filter if specified
        if salary_currency != "Any":
            processed_jobs = self._filter_by_salary_currency(processed_jobs, salary_currency)
        
        total_time = time.time() - total_start_time
        countries_searched = len([c for c in global_countries if any(jobs["country_code"].iloc[0] == c[2] for jobs in all_jobs if not jobs.empty)])
        
        if progress_callback:
            progress_callback(f"✅ Global search complete! Found {len(processed_jobs)} remote jobs", 1.0)
        
        return {
            "success": True,
            "jobs": processed_jobs,
            "count": len(processed_jobs),
            "search_time": total_time,
            "message": f"Found {len(processed_jobs)} remote jobs across {countries_searched} countries",
            "metadata": {
                "search_type": "global",
                "countries_searched": countries_searched,
                "total_countries": len(global_countries),
                "enhanced_search_term": search_term
            }
        }
    
    def _search_country_remote_jobs(
        self, search_term: str, country: str, job_type_code: str, remote_level_code: str,
        salary_currency: str, hours_old: Optional[int], results_wanted: int,
        proxies: Optional[List[str]], progress_callback=None
    ) -> Dict[str, Any]:
        """Search for remote jobs in a specific country."""
        indeed_country = get_indeed_country_name(country)
        
        if progress_callback:
            progress_callback(f"Searching remote jobs in {country}...", 0.3)
        
        start_time = time.time()
        result = self._search_single_country(
            search_term, indeed_country, job_type_code, remote_level_code,
            hours_old, results_wanted, proxies
        )
        
        if progress_callback:
            progress_callback(f"Processing {country} results...", 0.8)
        search_time = time.time() - start_time
        
        if result["success"] and result["jobs"] is not None:
            # Add country information
            jobs_with_country = result["jobs"].copy()
            flag, country_display = get_country_flag_and_name(indeed_country)
            jobs_with_country["country_flag"] = flag
            jobs_with_country["country_name"] = country_display
            jobs_with_country["country_code"] = indeed_country
            
            # Process and clean the data
            processed_jobs = self._process_jobs(jobs_with_country)
            
            # Apply salary currency filter if specified
            if salary_currency != "Any":
                processed_jobs = self._filter_by_salary_currency(processed_jobs, salary_currency)
            
            return {
                "success": True,
                "jobs": processed_jobs,
                "count": len(processed_jobs),
                "search_time": search_time,
                "message": f"Found {len(processed_jobs)} remote jobs in {country}",
                "metadata": {
                    "search_type": "country",
                    "country": country,
                    "country_code": indeed_country,
                    "enhanced_search_term": search_term
                }
            }
        else:
            return {
                "success": True,
                "jobs": None,
                "count": 0,
                "search_time": search_time,
                "message": f"No remote jobs found in {country}",
                "metadata": {
                    "search_type": "country",
                    "country": country,
                    "country_code": indeed_country
                }
            }
    
    def _search_single_country(
        self, search_term: str, country_code: str, job_type_code: str, 
        remote_level_code: str, hours_old: Optional[int], results_wanted: int,
        proxies: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Perform a single country search with remote-first parameters."""
        
        # Prepare base search parameters (less restrictive for debugging)
        search_params = {
            "search_term": search_term,
            "country_indeed": country_code,
            "results_wanted": results_wanted,
            "site_name": ["indeed"]
        }
        
        # Add location for remote search
        if remote_level_code == "FULLY_REMOTE":
            search_params["location"] = "remote"
            # search_params["is_remote"] = True  # Commenting out to test
        else:
            search_params["location"] = ""
        
        # Add job type filter if not "Any" (using string values as expected by JobSpy)
        if job_type_code != "ANY":
            search_params["job_type"] = job_type_code  # Use the lowercase string directly
        
        # Add time filter if specified (avoiding conflicts)
        if hours_old is not None:
            search_params["hours_old"] = hours_old
            # Note: Cannot use job_type with hours_old in JobSpy
            if "job_type" in search_params:
                del search_params["job_type"]
        
        # Add proxies if provided
        if proxies:
            search_params["proxies"] = proxies
        
        # Perform the search
        start_time = time.time()
        jobs = scrape_jobs(**search_params)
        search_time = time.time() - start_time
        
        return {
            "success": True,
            "jobs": jobs if not jobs.empty else None,
            "search_time": search_time,
            "search_params": search_params
        }
    
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
    
    def _filter_by_salary_currency(self, jobs_df: pd.DataFrame, target_currency: str) -> pd.DataFrame:
        """
        Filter jobs by salary currency after scraping.
        
        Since JobSpy doesn't support salary currency filtering at API level,
        we filter the results post-processing.
        """
        if target_currency == "Any" or jobs_df.empty:
            return jobs_df
        
        # Create a boolean mask for jobs that match the currency
        currency_mask = pd.Series([True] * len(jobs_df), index=jobs_df.index)
        
        # Check different currency columns
        currency_columns = ['currency', 'salary_currency']
        
        for col in currency_columns:
            if col in jobs_df.columns:
                # Filter by exact currency match (case insensitive)
                currency_mask &= (
                    jobs_df[col].fillna('').astype(str).str.upper() == target_currency.upper()
                )
        
        # If no currency columns exist, check salary_formatted for currency symbols
        if 'salary_formatted' in jobs_df.columns and not any(col in jobs_df.columns for col in currency_columns):
            currency_patterns = {
                'USD': ['$', 'USD', 'US$'],
                'EUR': ['€', 'EUR', 'euro'],
                'GBP': ['£', 'GBP', 'pound'],
                'CAD': ['CAD', 'C$', 'canadian'],
                'AUD': ['AUD', 'A$', 'australian'],
                'BRL': ['R$', 'BRL', 'real', 'reais']
            }
            
            if target_currency.upper() in currency_patterns:
                patterns = currency_patterns[target_currency.upper()]
                # Check if any of the patterns exist in salary_formatted
                pattern_mask = pd.Series([False] * len(jobs_df), index=jobs_df.index)
                
                for pattern in patterns:
                    pattern_mask |= jobs_df['salary_formatted'].fillna('').str.contains(
                        pattern, case=False, na=False
                    )
                
                currency_mask &= pattern_mask
        
        return jobs_df[currency_mask]
    
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
                # Full date for older posts
                return date_obj.strftime("%b %d, %Y")
                
        except Exception as e:
            # Fallback to string representation
            return str(date_posted) if date_posted else "N/A"
    
    def _enforce_rate_limit(self):
        """Enforce minimum delay between searches."""
        current_time = time.time()
        time_since_last = current_time - self.last_search_time
        
        if time_since_last < self.min_delay:
            sleep_time = self.min_delay - time_since_last
            time.sleep(sleep_time)
        
        self.last_search_time = time.time()
    
    def get_supported_parameters(self) -> Dict[str, Any]:
        """Get information about supported parameters."""
        return {
            "search_term": "Job title or keywords",
            "location": "Geographic location (city, state, country)",
            "country": "Country for Indeed search (60+ supported)",
            "distance": "Search radius in miles (default: 50)",
            "is_remote": "Filter for remote jobs",
            "time_filter": "Filter by posting age (24h, 72h, 1 week, or no filter)",
            "results_wanted": "Number of jobs to fetch (5-100)",
            "proxies": "List of proxy servers for rotation"
        }
    
    def get_limitations(self) -> List[str]:
        """Get list of Indeed scraper limitations."""
        return [
            "Cannot use 'hours_old' with 'job_type' or 'easy_apply' simultaneously",
            "Maximum of 1000 jobs per search",
            "Rate limiting may apply without proxies",
            "Some job details may be incomplete"
        ]
