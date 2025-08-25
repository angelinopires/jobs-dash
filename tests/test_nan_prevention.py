"""
Comprehensive test suite to prevent "nan" value issues across the entire application.
This is a meta-test file that ensures all display and formatting functions 
properly handle various forms of invalid/missing data.
"""

import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add the parent directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.display_utils import clean_display_value, clean_company_info, format_posted_date_enhanced
from scrapers.optimized_indeed_scraper import get_indeed_scraper


class TestNanPreventionSuite(unittest.TestCase):
    """Comprehensive test suite to prevent nan values from appearing in the UI."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for the class."""
        from scrapers.optimized_indeed_scraper import get_indeed_scraper
        cls.scraper = get_indeed_scraper()
        
        # Define all possible "invalid" values that should be cleaned
        cls.invalid_values = [
            None,
            np.nan,
            pd.NA,
            'nan',
            'NaN', 
            'NAN',
            'none',
            'None',
            'NONE',
            'null',
            'NULL',
            'Null',
            '',
            '   ',
            '\t',
            '\n',
            'n/a',
            'N/A',
            'n.a.',
            'N.A.',
            'not available',
            'Not Available',
            'NOT AVAILABLE',
            'undefined',
            'Undefined',
            'UNDEFINED'
        ]
        
        # Valid values that should NOT be cleaned
        cls.valid_values = [
            'Software Engineer',
            'Google Inc.',
            'Remote, US',
            '$100,000',
            'Technology',
            '100-500 employees',
            'banana',  # Contains 'nan' but is valid
            'finance',  # Contains 'nan' but is valid
            'management',  # Contains 'nan' but is valid
            'Financial Services',
            'nano technology',
            'Annual revenue',
            '0',  # Zero as string
            0,    # Zero as number
            'False',  # String false
            False,    # Boolean false
        ]
    
    def test_clean_display_value_never_shows_nan(self):
        """Test that clean_display_value never returns any form of 'nan'."""
        for invalid_value in self.invalid_values:
            with self.subTest(value=invalid_value):
                result = clean_display_value(invalid_value)
                self.assertNotIn('nan', str(result).lower())
                self.assertNotIn('none', str(result).lower())
                self.assertNotIn('null', str(result).lower())
                # Should return the default
                self.assertEqual(result, "Not available")
    
    def test_clean_display_value_preserves_valid_data(self):
        """Test that clean_display_value preserves valid data."""
        for valid_value in self.valid_values:
            with self.subTest(value=valid_value):
                result = clean_display_value(valid_value)
                # Should not be cleaned to "Not available"
                self.assertNotEqual(result, "Not available")
                # Should preserve the original value (as string)
                self.assertEqual(result, str(valid_value))
    
    def test_clean_company_info_never_shows_nan(self):
        """Test that clean_company_info never returns any form of 'nan'."""
        # Test with various combinations of invalid company data
        for invalid_value in self.invalid_values:
            test_strings = [
                f"Industry: {invalid_value}",
                f"Industry: {invalid_value} | Size: {invalid_value}",
                f"Industry: {invalid_value} | Size: {invalid_value} | Revenue: {invalid_value}",
                f"Industry: Technology | Size: {invalid_value} | Revenue: {invalid_value}",
                str(invalid_value)
            ]
            
            for test_string in test_strings:
                with self.subTest(value=test_string):
                    result = clean_company_info(test_string)
                    self.assertNotIn('nan', str(result).lower())
                    self.assertNotIn('none', str(result).lower()) 
                    self.assertNotIn('null', str(result).lower())
    
    def test_scraper_format_company_info_never_shows_nan(self):
        """Test that scraper _format_company_info never returns any form of 'nan'."""
        for invalid_value in self.invalid_values:
            test_rows = [
                {'company_industry': invalid_value},
                {'company_industry': invalid_value, 'company_num_employees': invalid_value},
                {'company_industry': invalid_value, 'company_num_employees': invalid_value, 'company_revenue': invalid_value},
                {'company_industry': 'Technology', 'company_num_employees': invalid_value, 'company_revenue': invalid_value},
                {}  # Empty row
            ]
            
            for row in test_rows:
                with self.subTest(row=row):
                    result = self.scraper._format_company_info(row)
                    self.assertNotIn('nan', str(result).lower())
                    self.assertNotIn('none', str(result).lower())
                    self.assertNotIn('null', str(result).lower())
    
    def test_date_formatting_never_shows_nan(self):
        """Test that date formatting never returns any form of 'nan'."""
        for invalid_value in self.invalid_values:
            with self.subTest(value=invalid_value):
                result = format_posted_date_enhanced(invalid_value)
                self.assertNotIn('nan', str(result).lower())
                self.assertNotIn('none', str(result).lower())
                self.assertNotIn('null', str(result).lower())
    
    def test_end_to_end_job_processing(self):
        """Test end-to-end job processing doesn't produce nan values."""
        # Create a job DataFrame with various invalid values
        jobs_data = pd.DataFrame([
            {
                'title': 'Software Engineer',
                'company': 'Tech Corp',
                'location': 'Remote',
                'date_posted': '2025-01-15',
                'site': 'indeed',
                'job_url': 'https://example.com/job1',
                'description': 'Great opportunity',
                'is_remote': True,
                'company_industry': 'nan',
                'company_num_employees': np.nan,
                'company_revenue': None
            },
            {
                'title': None,
                'company': 'null',
                'location': '',
                'date_posted': np.nan,
                'site': 'indeed',
                'job_url': 'n/a',
                'description': '   ',
                'is_remote': False,
                'company_industry': 'none',
                'company_num_employees': 'undefined',
                'company_revenue': 'not available'
            }
        ])
        
        # Process through scraper
        processed_jobs = self.scraper._process_jobs(jobs_data)
        
        # Check all text fields for nan values
        text_columns = ['company_name', 'location_formatted', 'salary_formatted', 
                       'company_info', 'date_posted_formatted']
        
        for column in text_columns:
            if column in processed_jobs.columns:
                for value in processed_jobs[column].fillna(''):
                    self.assertNotIn('nan', str(value).lower(), 
                                   f"Found 'nan' in {column}: {value}")
                    self.assertNotIn('none', str(value).lower(),
                                   f"Found 'none' in {column}: {value}")
                    self.assertNotIn('null', str(value).lower(),
                                   f"Found 'null' in {column}: {value}")
    
    def test_dashboard_display_functions_consistency(self):
        """Test that all dashboard display functions handle the same invalid values consistently."""
        for invalid_value in self.invalid_values:
            # All these functions should handle invalid values gracefully
            display_result = clean_display_value(invalid_value)
            company_result = clean_company_info(invalid_value)
            date_result = format_posted_date_enhanced(invalid_value)
            
            # None should contain nan/none/null
            results = [display_result, company_result, date_result]
            for result in results:
                self.assertNotIn('nan', str(result).lower())
                self.assertNotIn('none', str(result).lower()) 
                self.assertNotIn('null', str(result).lower())
    
    def test_realistic_job_data_scenarios(self):
        """Test with realistic job data that might contain nan values."""
        realistic_scenarios = [
            # Scenario 1: Indeed job with partial company data
            {
                'title': 'Web Developer',
                'company_name': 'StartupCorp',
                'location_formatted': 'Remote, Worldwide',
                'salary_formatted': 'Not specified',
                'company_info': 'Industry: Technology | Size: nan | Revenue: Confidential',
                'job_type': 'Contract',
                'description': 'Exciting opportunity to join our team...'
            },
            # Scenario 2: Job with minimal data
            {
                'title': 'Data Analyst',
                'company_name': 'nan',
                'location_formatted': None,
                'salary_formatted': 'N/A',
                'company_info': 'Industry: none | Size: null | Revenue: undefined',
                'job_type': '',
                'description': None
            }
        ]
        
        for i, scenario in enumerate(realistic_scenarios):
            with self.subTest(scenario=i):
                # Apply display functions as they would be used in the dashboard
                cleaned_data = {}
                for field, value in scenario.items():
                    if field == 'company_info':
                        cleaned_data[field] = clean_company_info(value)
                    else:
                        cleaned_data[field] = clean_display_value(value)
                
                # Verify no nan values in cleaned data
                for field, cleaned_value in cleaned_data.items():
                    self.assertNotIn('nan', str(cleaned_value).lower(), 
                                   f"Found 'nan' in cleaned {field}: {cleaned_value}")
                    self.assertNotIn('none', str(cleaned_value).lower(),
                                   f"Found 'none' in cleaned {field}: {cleaned_value}")
                    self.assertNotIn('null', str(cleaned_value).lower(),
                                   f"Found 'null' in cleaned {field}: {cleaned_value}")


class TestNanPreventionRegression(unittest.TestCase):
    """Regression tests for specific nan issues that were reported."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    def test_company_info_nan_regression(self):
        """Regression test for the specific company info nan issue."""
        # This is the exact problem that was reported
        problematic_row = {
            'company_industry': 'nan',
            'company_num_employees': 'nan',
            'company_revenue': 'nan'
        }
        
        result = self.scraper._format_company_info(problematic_row)
        
        # Should NOT be the problematic output
        self.assertNotEqual(result, "Industry: nan | Size: nan | Revenue: nan")
        
        # Should be clean
        self.assertEqual(result, "N/A")
    
    def test_dashboard_job_details_nan_regression(self):
        """Regression test for nan values appearing in job details."""
        # Simulate the job data that would cause nan in job details
        job_data = {
            'title': 'Website Developer',
            'company_name': 'Kraken Athletics',
            'location_formatted': 'Remote, US',
            'salary_formatted': 'Not specified',
            'company_info': 'Industry: nan | Size: nan | Revenue: nan',
            'job_type': 'Contract'
        }
        
        # Apply the same cleaning that the dashboard would do
        cleaned_company_info = clean_company_info(job_data['company_info'])
        
        # Should not contain any nan values
        self.assertNotIn('nan', cleaned_company_info.lower())
        self.assertEqual(cleaned_company_info, "Not available")


if __name__ == '__main__':
    # Run all nan prevention tests
    unittest.main(verbosity=2)
