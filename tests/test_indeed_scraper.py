"""
Unit tests for IndeedScraper implementation.

Tests the inheritance from BaseJobScraper and Indeed-specific functionality.
"""

import unittest
import pandas as pd
import time
from unittest.mock import Mock, patch, MagicMock
from typing import Dict, Any

# Import the scrapers
from scrapers.base_scraper import BaseScraper
from scrapers.optimized_indeed_scraper import get_indeed_scraper, OptimizedIndeedScraper


class TestIndeedScraperInheritance(unittest.TestCase):
    """Test that IndeedScraper properly inherits from BaseJobScraper."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    def test_inheritance(self):
        """Test that IndeedScraper inherits from BaseScraper."""
        self.assertIsInstance(self.scraper, BaseScraper)
        self.assertIsInstance(self.scraper, OptimizedIndeedScraper)
    
    def test_abstract_methods_implemented(self):
        """Test that all abstract methods are implemented."""
        # Should not raise TypeError (which would happen if abstract methods weren't implemented)
        scraper = get_indeed_scraper()
        
        # Check that required methods exist and are callable
        self.assertTrue(hasattr(scraper, 'get_supported_api_filters'))
        self.assertTrue(callable(scraper.get_supported_api_filters))
        
        self.assertTrue(hasattr(scraper, '_build_api_search_params'))
        self.assertTrue(callable(scraper._build_api_search_params))
        
        self.assertTrue(hasattr(scraper, '_call_scraping_api'))
        self.assertTrue(callable(scraper._call_scraping_api))
    
    def test_get_supported_api_filters(self):
        """Test that supported API filters are properly defined."""
        supported = self.scraper.get_supported_api_filters()
        
        # Should return a dict
        self.assertIsInstance(supported, dict)
        
        # Indeed via JobSpy should support these filters
        expected_supported = [
            'search_term', 'location', 'time_filter', 'results_wanted'
        ]
        for filter_name in expected_supported:
            self.assertIn(filter_name, supported)
            self.assertTrue(supported[filter_name], f"{filter_name} should be supported")
        
        # These should require post-processing
        expected_post_processing = [
            'job_type', 'salary_currency', 'salary_min', 'salary_max', 'company_size'
        ]
        for filter_name in expected_post_processing:
            self.assertIn(filter_name, supported)
            self.assertFalse(supported[filter_name], f"{filter_name} should require post-processing")


class TestIndeedScraperAPI(unittest.TestCase):
    """Test IndeedScraper API integration and parameter building."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    def test_build_api_search_params_basic(self):
        """Test building basic API search parameters."""
        filters = {
            'search_term': 'Python Developer',
            'where': 'United States',
            'include_remote': True,
            'time_filter': 'Past Week',
            'results_wanted': 1000
        }
        
        params = self.scraper._build_api_search_params(**filters)
        
        # Should include basic parameters
        self.assertIn('site_name', params)
        self.assertEqual(params['site_name'], ['indeed'])
        self.assertIn('results_wanted', params)
        self.assertEqual(params['results_wanted'], 1000)
        
        # Should include enhanced search term
        self.assertIn('search_term', params)
        self.assertIn('python', params['search_term'].lower())
        
        # Should include country mapping
        self.assertIn('country_indeed', params)
    
    def test_build_api_search_params_remote(self):
        """Test remote-specific parameter building."""
        filters = {
            'search_term': 'Developer',
            'include_remote': True
        }
        
        params = self.scraper._build_api_search_params(**filters)
        
        # Should set location to "remote" for fully remote jobs
        self.assertIn('location', params)
        self.assertEqual(params['location'], 'remote')
    
    def test_build_api_search_params_time_filter_conflicts(self):
        """Test that time filter handles JobSpy limitations correctly."""
        filters = {
            'search_term': 'Developer',
            'time_filter': 'Last 24h'  # Use correct time filter option
        }
        
        params = self.scraper._build_api_search_params(**filters)
        
        # Should include hours_old
        self.assertIn('hours_old', params)
        
        # Should NOT include job_type (JobSpy limitation)
        self.assertNotIn('job_type', params)
    
    @patch('scrapers.indeed_scraper.scrape_jobs')
    def test_call_scraping_api_success(self, mock_scrape_jobs):
        """Test successful API call."""
        # Mock successful response
        mock_jobs = pd.DataFrame({
            'title': ['Python Developer'],
            'company': ['TechCorp'],
            'location': ['Remote'],
            'job_url': ['http://example.com/1']
        })
        mock_scrape_jobs.return_value = mock_jobs
        
        search_params = {
            'search_term': 'Python',
            'site_name': ['indeed'],
            'results_wanted': 1000
        }
        
        result = self.scraper._call_scraping_api(search_params)
        
        # Should call scrape_jobs with correct params
        mock_scrape_jobs.assert_called_once_with(**search_params)
        
        # Should return the DataFrame
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['title'], 'Python Developer')
    
    @patch('scrapers.optimized_indeed_scraper.scrape_jobs')
    def test_call_scraping_api_failure(self, mock_scrape_jobs):
        """Test API call failure handling."""
        # Mock API failure
        mock_scrape_jobs.side_effect = Exception("API Error")
        
        search_params = {'search_term': 'Python'}
        result = self.scraper._call_scraping_api(search_params)
        
        # Should return empty DataFrame on error
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)


class TestIndeedScraperSearchTime(unittest.TestCase):
    """Test that search_time field is always present in results."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    @patch('scrapers.optimized_indeed_scraper.scrape_jobs')
    def test_single_country_search_includes_search_time(self, mock_scrape_jobs):
        """Test that single country search includes search_time."""
        # Mock empty response
        mock_scrape_jobs.return_value = pd.DataFrame()
        
        result = self.scraper.search_jobs(
            search_term="Python",
            where="United States"
        )
        
        # Should include search_time
        self.assertIn('search_time', result)
        self.assertIsInstance(result['search_time'], (int, float))
        self.assertGreater(result['search_time'], 0)
    
    @patch.object(get_indeed_scraper().__class__, '_search_global_optimized')
    def test_global_search_includes_search_time(self, mock_global_search):
        """Test that global search includes search_time."""
        # Mock global search response
        mock_global_search.return_value = {
            "success": True,
            "jobs": pd.DataFrame(),
            "count": 0,
            "search_time": 1.5,  # ✅ This is what we fixed!
            "message": "No jobs found",
            "metadata": {"search_type": "global"}
        }
        
        result = self.scraper.search_jobs(
            search_term="Python",
            where="Global"
        )
        
        # Should include search_time
        self.assertIn('search_time', result)
        self.assertEqual(result['search_time'], 1.5)
    
    @patch('scrapers.optimized_indeed_scraper.get_global_countries')
    @patch.object(BaseScraper, 'search_jobs')
    def test_global_search_empty_results_includes_search_time(self, mock_base_search, mock_global_countries):
        """Test that global search with no results still includes search_time."""
        # Mock no global countries
        mock_global_countries.return_value = []
        
        # Call the actual _search_global_optimized method
        result = self.scraper._search_global_optimized('Python', True, None)
        
        # Should include search_time even with no results
        self.assertIn('search_time', result)
        self.assertIsInstance(result['search_time'], (int, float))
        self.assertGreater(result['search_time'], 0)
    
    @patch('scrapers.optimized_indeed_scraper.get_global_countries')
    @patch.object(BaseScraper, 'search_jobs')
    def test_global_search_with_results_includes_search_time(self, mock_base_search, mock_global_countries):
        """Test that global search with results includes search_time."""
        # Mock some global countries
        mock_global_countries.return_value = [
            ('🇺🇸', 'United States', 'US'),
            ('🇨🇦', 'Canada', 'CA')
        ]
        
        # Mock base search returning some jobs
        mock_jobs = pd.DataFrame({
            'title': ['Developer'],
            'company': ['TechCorp'],
            'job_url': ['http://example.com/1']
        })
        mock_base_search.return_value = {
            "success": True,
            "jobs": mock_jobs,
            "count": 1,
            "search_time": 0.5,
            "message": "Found 1 job"
        }
        
        # Call the actual _search_global_optimized method
        result = self.scraper._search_global_optimized('Python', True, None)
        
        # Should include search_time
        self.assertIn('search_time', result)
        self.assertIsInstance(result['search_time'], (int, float))
        self.assertGreater(result['search_time'], 0)


class TestIndeedScraperJobProcessing(unittest.TestCase):
    """Test Indeed-specific job processing and formatting."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
        
        # Sample raw JobSpy data (what comes from the API)
        self.raw_jobs = pd.DataFrame({
            'title': ['Python Developer', 'JavaScript Engineer'],
            'company': ['TechCorp', ''],  # Empty company name
            'location': ['Remote', 'New York'],
            'min_amount': [80000, None],
            'max_amount': [120000, 90000],
            'currency': ['USD', 'USD'],
            'interval': ['yearly', 'yearly'],
            'date_posted': [1701360000, '2023-12-02'],  # Mix of timestamp and string
            'job_url': ['http://example.com/1', 'http://example.com/2'],
            'description': ['Great Python job', 'Frontend role'],
            'is_remote': [True, False]
        })
    
    def test_process_jobs_handles_empty_dataframe(self):
        """Test that _process_jobs handles empty DataFrames."""
        empty_df = pd.DataFrame()
        result = self.scraper._process_jobs(empty_df)
        
        # Should return empty DataFrame without errors
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)
    
    def test_process_jobs_adds_required_columns(self):
        """Test that _process_jobs adds all required columns."""
        result = self.scraper._process_jobs(self.raw_jobs)
        
        required_columns = [
            'title', 'company', 'location', 'date_posted', 
            'site', 'job_url', 'description', 'is_remote'
        ]
        
        for col in required_columns:
            self.assertIn(col, result.columns, f"Missing required column: {col}")
    
    def test_process_jobs_cleans_company_names(self):
        """Test that empty company names are handled."""
        result = self.scraper._process_jobs(self.raw_jobs)
        
        # Empty company should be replaced with "Not specified"
        companies = result['company'].tolist()
        self.assertNotIn('', companies)
        self.assertIn('Not specified', companies)
        
        # Should also create company_name alias
        self.assertIn('company_name', result.columns)
        self.assertEqual(result['company'].tolist(), result['company_name'].tolist())
    
    def test_process_jobs_formats_salary(self):
        """Test salary formatting from JobSpy columns."""
        result = self.scraper._process_jobs(self.raw_jobs)
        
        # Should have salary_formatted column
        self.assertIn('salary_formatted', result.columns)
        
        # Find jobs by their titles since sorting may reorder them
        python_job = result[result['title'] == 'Python Developer'].iloc[0]
        js_job = result[result['title'] == 'JavaScript Engineer'].iloc[0]
        
        # Python job has min and max
        python_salary = python_job['salary_formatted']
        self.assertIn('80,000', python_salary)
        self.assertIn('120,000', python_salary)
        self.assertIn('USD', python_salary)
        
        # JS job has only max
        js_salary = js_job['salary_formatted']
        self.assertIn('90,000', js_salary)
        self.assertIn('Up to', js_salary)
    
    def test_process_jobs_formats_dates(self):
        """Test date formatting from various formats."""
        result = self.scraper._process_jobs(self.raw_jobs)
        
        # Should have date_posted_formatted column
        self.assertIn('date_posted_formatted', result.columns)
        
        # Both dates should be formatted (one from timestamp, one from string)
        date1 = result.iloc[0]['date_posted_formatted']
        date2 = result.iloc[1]['date_posted_formatted']
        
        self.assertIsInstance(date1, str)
        self.assertIsInstance(date2, str)
        self.assertNotEqual(date1, 'N/A')
        self.assertNotEqual(date2, 'N/A')


class TestIndeedScraperIntegration(unittest.TestCase):
    """Integration tests for IndeedScraper end-to-end functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    @patch('scrapers.optimized_indeed_scraper.scrape_jobs')
    def test_end_to_end_search_result_format(self, mock_scrape_jobs):
        """Test that the complete search result has the expected format."""
        # Mock JobSpy response
        mock_jobs = pd.DataFrame({
            'title': ['Python Developer'],
            'company': ['TechCorp'],
            'location': ['Remote'],
            'min_amount': [80000],
            'max_amount': [120000],
            'currency': ['USD'],
            'date_posted': [1701360000],
            'job_url': ['http://example.com/1']
        })
        mock_scrape_jobs.return_value = mock_jobs
        
        # Perform search
        result = self.scraper.search_jobs(
            search_term="Python Developer",
            where="United States",
            include_remote=True,
            salary_currency="USD",
            time_filter="Past Week",
            results_wanted=1000
        )
        
        # Validate complete result format
        self.assertIsInstance(result, dict)
        
        # Required fields from BaseJobScraper
        required_fields = ['success', 'jobs', 'count', 'search_time', 'message', 'metadata']
        for field in required_fields:
            self.assertIn(field, result)
        
        # Should be successful
        self.assertTrue(result['success'])
        
        # Should have timing information
        self.assertIsInstance(result['search_time'], (int, float))
        self.assertGreater(result['search_time'], 0)
        
        # Should have processed jobs
        self.assertIsInstance(result['jobs'], pd.DataFrame)
        self.assertEqual(result['count'], 1)
        
        # Jobs should have required columns
        jobs_df = result['jobs']
        self.assertIn('salary_formatted', jobs_df.columns)
        self.assertIn('company_name', jobs_df.columns)
        self.assertIn('date_posted_formatted', jobs_df.columns)


if __name__ == '__main__':
    # Run with verbose output to see test names
    unittest.main(verbosity=2)
