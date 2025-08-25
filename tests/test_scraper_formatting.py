"""
Unit tests for Indeed scraper formatting functions to prevent "nan" value issues.
Tests the _format_company_info and other formatting functions in IndeedScraper.
"""

import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add the parent directory to sys.path to import scraper classes
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.optimized_indeed_scraper import get_indeed_scraper


class TestIndeedScraperFormatting(unittest.TestCase):
    """Test Indeed scraper formatting functions for proper nan handling."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    def test_format_company_info_valid_data(self):
        """Test _format_company_info with valid company data."""
        # Test with all valid fields
        row_all_valid = {
            'company_industry': 'Technology',
            'company_num_employees': '100-500',
            'company_revenue': '$10M-50M'
        }
        result = self.scraper._format_company_info(row_all_valid)
        expected = "Industry: Technology | Size: 100-500 | Revenue: $10M-50M"
        self.assertEqual(result, expected)
        
        # Test with single valid field
        row_single = {'company_industry': 'Healthcare'}
        result = self.scraper._format_company_info(row_single)
        self.assertEqual(result, "Industry: Healthcare")
        
        # Test with two valid fields
        row_two = {
            'company_industry': 'Finance',
            'company_num_employees': '1000+'
        }
        result = self.scraper._format_company_info(row_two)
        self.assertEqual(result, "Industry: Finance | Size: 1000+")
    
    def test_format_company_info_invalid_data(self):
        """Test _format_company_info with invalid/nan data."""
        # Test with all nan values
        row_all_nan = {
            'company_industry': np.nan,
            'company_num_employees': np.nan,
            'company_revenue': np.nan
        }
        result = self.scraper._format_company_info(row_all_nan)
        self.assertEqual(result, "N/A")
        
        # Test with string "nan" values
        row_string_nan = {
            'company_industry': 'nan',
            'company_num_employees': 'none',
            'company_revenue': 'null'
        }
        result = self.scraper._format_company_info(row_string_nan)
        self.assertEqual(result, "N/A")
        
        # Test with None values
        row_none = {
            'company_industry': None,
            'company_num_employees': None,
            'company_revenue': None
        }
        result = self.scraper._format_company_info(row_none)
        self.assertEqual(result, "N/A")
        
        # Test with empty strings
        row_empty = {
            'company_industry': '',
            'company_num_employees': '   ',
            'company_revenue': 'n/a'
        }
        result = self.scraper._format_company_info(row_empty)
        self.assertEqual(result, "N/A")
    
    def test_format_company_info_mixed_data(self):
        """Test _format_company_info with mixed valid/invalid data."""
        # Test with some valid, some invalid
        row_mixed1 = {
            'company_industry': 'Technology',
            'company_num_employees': 'nan',
            'company_revenue': '$100M+'
        }
        result = self.scraper._format_company_info(row_mixed1)
        self.assertEqual(result, "Industry: Technology | Revenue: $100M+")
        
        row_mixed2 = {
            'company_industry': None,
            'company_num_employees': '50-100',
            'company_revenue': 'null'
        }
        result = self.scraper._format_company_info(row_mixed2)
        self.assertEqual(result, "Size: 50-100")
        
        row_mixed3 = {
            'company_industry': 'Healthcare',
            'company_num_employees': '',
            'company_revenue': None
        }
        result = self.scraper._format_company_info(row_mixed3)
        self.assertEqual(result, "Industry: Healthcare")
    
    def test_format_company_info_edge_cases(self):
        """Test _format_company_info with edge cases."""
        # Test with missing keys
        row_missing = {}
        result = self.scraper._format_company_info(row_missing)
        self.assertEqual(result, "N/A")
        
        # Test with pandas NA
        row_pandas_na = {
            'company_industry': pd.NA,
            'company_num_employees': pd.NA,
            'company_revenue': pd.NA
        }
        result = self.scraper._format_company_info(row_pandas_na)
        self.assertEqual(result, "N/A")
        
        # Test with values that contain "nan" but are valid
        row_contain_nan = {
            'company_industry': 'Financial Services',
            'company_num_employees': 'Management team of 5',
            'company_revenue': 'Annual revenue varies'
        }
        result = self.scraper._format_company_info(row_contain_nan)
        expected = "Industry: Financial Services | Size: Management team of 5 | Revenue: Annual revenue varies"
        self.assertEqual(result, expected)
    
    def test_format_company_info_case_insensitive(self):
        """Test _format_company_info handles case-insensitive invalid values."""
        # Test various case combinations of invalid values
        test_cases = [
            {'company_industry': 'NaN'},
            {'company_industry': 'NONE'},
            {'company_industry': 'NULL'},
            {'company_industry': 'N/A'},
            {'company_industry': 'Null'},
            {'company_industry': 'None'},
        ]
        
        for row in test_cases:
            result = self.scraper._format_company_info(row)
            self.assertEqual(result, "N/A", f"Failed for case: {row}")


class TestIndeedScraperFormattingIntegration(unittest.TestCase):
    """Integration tests for scraper formatting functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    def test_process_jobs_with_nan_data(self):
        """Test that _process_jobs handles nan company data properly."""
        # Create a mock DataFrame with nan company data
        jobs_data = pd.DataFrame([
            {
                'title': 'Software Engineer',
                'company': 'Tech Corp',
                'location': 'Remote',
                'date_posted': '2025-01-15',
                'site': 'indeed',
                'job_url': 'https://example.com/job1',
                'description': 'Great job opportunity',
                'is_remote': True,
                'company_industry': 'Technology',
                'company_num_employees': np.nan,
                'company_revenue': 'nan'
            },
            {
                'title': 'Data Scientist',
                'company': 'Data Inc',
                'location': 'New York',
                'date_posted': '2025-01-14',
                'site': 'indeed',
                'job_url': 'https://example.com/job2',
                'description': 'Exciting data role',
                'is_remote': False,
                'company_industry': None,
                'company_num_employees': None,
                'company_revenue': None
            }
        ])
        
        # Process the jobs
        processed_jobs = self.scraper._process_jobs(jobs_data)
        
        # Check that company_info is properly formatted
        self.assertIn('company_info', processed_jobs.columns)
        
        # First job should have only Industry (other fields are nan/invalid)
        first_job_info = processed_jobs.iloc[0]['company_info']
        self.assertEqual(first_job_info, "Industry: Technology")
        
        # Second job should have no valid company info
        second_job_info = processed_jobs.iloc[1]['company_info']
        self.assertEqual(second_job_info, "Not available")
    
    def test_salary_formatting_with_nan(self):
        """Test salary formatting handles nan values properly."""
        # Test _format_salary_from_columns with nan values
        row_with_nan = {
            'min_amount': np.nan,
            'max_amount': 120000,
            'currency': 'USD',
            'interval': 'yearly'
        }
        result = self.scraper._format_salary_from_columns(row_with_nan)
        self.assertEqual(result, "Up to USD 120,000 (yearly)")
        
        # Test with all nan salary data
        row_all_nan = {
            'min_amount': np.nan,
            'max_amount': np.nan,
            'currency': 'nan',
            'interval': np.nan
        }
        result = self.scraper._format_salary_from_columns(row_all_nan)
        self.assertEqual(result, "Not specified")


class TestFormattingPreventionSuite(unittest.TestCase):
    """Comprehensive test suite to prevent nan formatting issues."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()
    
    def test_all_formatting_functions_handle_nan(self):
        """Test that all formatting functions properly handle nan values."""
        # Test data with various nan-like values
        nan_values = [None, np.nan, pd.NA, 'nan', 'NaN', 'none', 'None', 'null', 'NULL', '', '   ', 'n/a', 'N/A']
        
        for nan_value in nan_values:
            # Test company info formatting
            row = {
                'company_industry': nan_value,
                'company_num_employees': nan_value,
                'company_revenue': nan_value
            }
            result = self.scraper._format_company_info(row)
            self.assertNotIn('nan', result.lower(), f"Found 'nan' in result for value: {nan_value}")
            self.assertNotIn('none', result.lower(), f"Found 'none' in result for value: {nan_value}")
            self.assertNotIn('null', result.lower(), f"Found 'null' in result for value: {nan_value}")
            
            # Test salary formatting
            # Handle pandas NA values properly
            if pd.isna(nan_value):
                min_amount = None
                max_amount = None
            elif str(nan_value).lower() in ['nan', 'none', 'null', 'n/a']:
                min_amount = None
                max_amount = None
            else:
                min_amount = nan_value
                max_amount = nan_value
                
            salary_row = {
                'min_amount': min_amount,
                'max_amount': max_amount,
                'currency': nan_value,
                'interval': nan_value
            }
            salary_result = self.scraper._format_salary_from_columns(salary_row)
            self.assertNotIn('nan', salary_result.lower(), f"Found 'nan' in salary result for value: {nan_value}")
    
    def test_regression_prevention(self):
        """Regression test to ensure the specific reported issue doesn't reoccur."""
        # This tests the exact scenario that was reported
        problematic_row = {
            'company_industry': 'nan',
            'company_num_employees': 'nan', 
            'company_revenue': 'nan'
        }
        
        result = self.scraper._format_company_info(problematic_row)
        
        # Should NOT contain the problematic output
        self.assertNotEqual(result, "Industry: nan | Size: nan | Revenue: nan")
        
        # Should return clean output
        self.assertEqual(result, "N/A")


if __name__ == '__main__':
    unittest.main()
