#!/usr/bin/env python3
"""
Unit tests for the new formatting functions added to the dashboard.
"""

import unittest
import pandas as pd
import sys
import os

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from dashboard import apply_display_formatting, _extract_salary_for_sorting
from utils.display_utils import format_posted_date_enhanced


class TestDisplayFormatting(unittest.TestCase):
    """Test the apply_display_formatting function."""
    
    def test_job_type_formatting(self):
        """Test that job types are properly formatted."""
        test_data = {
            'title': ['Developer', 'Engineer'],
            'job_type': ['fulltime', 'parttime'],
            'salary_formatted': ['$100k', '$50k'],
            'date_posted': ['2024-01-15', '2024-01-20'],
        }
        df = pd.DataFrame(test_data)
        
        result = apply_display_formatting(df)
        
        self.assertEqual(result['job_type'].tolist(), ['Full-time', 'Part-time'])
    
    def test_empty_dataframe_handling(self):
        """Test that empty DataFrames are handled gracefully."""
        empty_df = pd.DataFrame()
        result = apply_display_formatting(empty_df)
        self.assertTrue(result.empty)
    
    def test_missing_columns_handling(self):
        """Test that missing columns are handled gracefully."""
        test_data = {'title': ['Developer']}
        df = pd.DataFrame(test_data)
        
        # Should not raise an error
        result = apply_display_formatting(df)
        self.assertIsNotNone(result)
    
    def test_sorting_applied(self):
        """Test that sorting is applied correctly."""
        test_data = {
            'title': ['Z Developer', 'A Developer', 'B Developer'],
            'job_type': ['fulltime', 'parttime', 'contract'],
            'salary_formatted': ['$50,000', '$100,000', '$75,000'],
            'date_posted': ['2024-01-10', '2024-01-15', '2024-01-20'],
        }
        df = pd.DataFrame(test_data)
        
        result = apply_display_formatting(df)
        
        # Should be sorted by salary (descending), then title (ascending)
        titles = result['title'].tolist()
        # Highest salary first ($100k = A Developer), then $75k (B Developer), then $50k (Z Developer)
        self.assertEqual(titles[0], 'A Developer')  # $100k
        self.assertEqual(titles[1], 'B Developer')  # $75k  
        self.assertEqual(titles[2], 'Z Developer')  # $50k


class TestDateFormatting(unittest.TestCase):
    """Test the _format_posted_date_enhanced function."""
    
    def test_iso_date_formatting(self):
        """Test formatting of ISO date strings."""
        result = format_posted_date_enhanced('2024-01-15')
        self.assertEqual(result, 'Jan 15, 2024')
    
    def test_na_values(self):
        """Test handling of N/A values."""
        test_cases = ['N/A', 'n/a', '', None]
        for test_case in test_cases:
            result = format_posted_date_enhanced(test_case)
            self.assertEqual(result, 'N/A')
    
    def test_timestamp_formatting(self):
        """Test formatting of timestamps."""
        # Unix timestamp for Jan 15, 2024
        timestamp = 1705276800
        result = format_posted_date_enhanced(timestamp)
        self.assertIn('2024', result)
        self.assertIn('Jan', result)
    
    def test_invalid_date_handling(self):
        """Test handling of invalid date strings."""
        result = format_posted_date_enhanced('invalid_date')
        self.assertEqual(result, 'invalid_date')


class TestSalarySorting(unittest.TestCase):
    """Test the _extract_salary_for_sorting function."""
    
    def test_salary_range_extraction(self):
        """Test extraction of salary values from ranges."""
        result = _extract_salary_for_sorting('$80,000 - $120,000')
        self.assertEqual(result, 120000)  # Should return max value
    
    def test_hourly_rate_conversion(self):
        """Test conversion of hourly rates to annual."""
        result = _extract_salary_for_sorting('$25/hour')
        self.assertEqual(result, 25 * 40 * 52)  # Hourly to annual conversion
    
    def test_no_salary_specified(self):
        """Test handling of jobs with no salary specified."""
        test_cases = ['Not specified', 'N/A', '', None]
        for test_case in test_cases:
            result = _extract_salary_for_sorting(test_case)
            self.assertEqual(result, 0)
    
    def test_single_salary_value(self):
        """Test extraction of single salary values."""
        result = _extract_salary_for_sorting('$95,000')
        self.assertEqual(result, 95000)
    
    def test_currency_symbols_handling(self):
        """Test handling of different currency symbols."""
        result = _extract_salary_for_sorting('€80,000')
        self.assertEqual(result, 80000)


class TestIntegrationFormatting(unittest.TestCase):
    """Test integration of all formatting functions."""
    
    def test_complete_formatting_workflow(self):
        """Test the complete formatting workflow."""
        test_data = {
            'title': ['Senior Developer', 'Junior Developer'],
            'job_type': ['fulltime', 'parttime'],
            'salary_formatted': ['$120,000 - $150,000', '$25/hour'],
            'date_posted': ['2024-01-15', '2024-01-20'],
            'company_name': ['TechCorp', 'StartupInc']
        }
        df = pd.DataFrame(test_data)
        
        result = apply_display_formatting(df)
        
        # Check job type formatting
        self.assertIn('Full-time', result['job_type'].values)
        self.assertIn('Part-time', result['job_type'].values)
        
        # Check date formatting
        self.assertIn('date_posted_formatted', result.columns)
        formatted_dates = result['date_posted_formatted'].tolist()
        self.assertTrue(all('2024' in date for date in formatted_dates))
        
        # Check that DataFrame is not empty
        self.assertFalse(result.empty)
        
        # Check that all original columns are preserved
        for col in df.columns:
            self.assertIn(col, result.columns)


if __name__ == '__main__':
    unittest.main()
