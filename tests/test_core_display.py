"""
Core display and formatting tests.

Consolidated tests for display functions, formatting, and data cleaning.
Covers ~70% of critical functionality.
"""

import unittest
import pandas as pd
import numpy as np
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.display_utils import clean_display_value, clean_company_info, format_posted_date_enhanced
from dashboard import apply_display_formatting, filter_by_salary_range, _extract_salary_for_sorting


class TestCoreDisplay(unittest.TestCase):
    """Core display and formatting functionality tests."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_jobs_df = pd.DataFrame({
            'title': ['Senior Python Developer', 'Data Scientist'],
            'company_name': ['TechCorp', 'DataLab'],
            'location': ['Remote', 'New York'],
            'salary_formatted': ['$80,000 - $120,000', '$90,000 - $130,000'],
            'job_url': ['https://example.com/job/1', 'https://example.com/job/2'],
            'date_posted': ['2024-01-15', '2024-01-20'],
            'job_type': ['fulltime', 'contract']
        })

    def test_display_formatting_core_functionality(self):
        """Test core display formatting works."""
        result = apply_display_formatting(self.sample_jobs_df)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertIn('job_type', result.columns)

    def test_job_type_formatting(self):
        """Test job type formatting from raw to display values."""
        result = apply_display_formatting(self.sample_jobs_df)

        # Check that raw values are converted to display values
        job_types = result['job_type'].tolist()
        self.assertIn('Full-time', job_types)
        self.assertIn('Contract', job_types)

    def test_date_formatting(self):
        """Test date formatting from ISO to readable format."""
        result = apply_display_formatting(self.sample_jobs_df)

        self.assertIn('date_posted_formatted', result.columns)
        formatted_dates = result['date_posted_formatted'].tolist()

        # Should contain readable date format
        for date in formatted_dates:
            self.assertIn('2024', date)

    def test_salary_sorting_extraction(self):
        """Test salary extraction for sorting purposes."""
        test_cases = [
            ("$80,000 - $120,000", 120000),
            ("$25/hour", 52000),  # 25 * 40 * 52 = 52,000
            ("Not specified", 0),
            (None, 0)
        ]

        for salary_str, expected in test_cases:
            with self.subTest(salary=salary_str):
                result = _extract_salary_for_sorting(salary_str)
                self.assertEqual(result, expected)


class TestDataCleaning(unittest.TestCase):
    """Test data cleaning and NaN prevention."""

    def test_clean_display_value_with_nan_values(self):
        """Test cleaning of various NaN/invalid values."""
        invalid_values = [None, np.nan, pd.NA, 'nan', 'None', '', '   ']

        for invalid_value in invalid_values:
            with self.subTest(value=invalid_value):
                result = clean_display_value(invalid_value)
                self.assertEqual(result, "Not available")

    def test_clean_display_value_preserves_valid_data(self):
        """Test that valid data is preserved."""
        valid_values = ['Software Engineer', 'Google Inc.', '$100,000']

        for valid_value in valid_values:
            with self.subTest(value=valid_value):
                result = clean_display_value(valid_value)
                self.assertEqual(result, valid_value)

    def test_company_info_formatting(self):
        """Test company info formatting with mixed valid/invalid data."""
        test_cases = [
            ("Industry: Technology | Size: 100-500", "Industry: Technology | Size: 100-500"),
            ("Industry: nan | Size: 100-500", "Size: 100-500"),
            ("Industry: nan | Size: none", "Not available")
        ]

        for input_str, expected in test_cases:
            with self.subTest(company_info=input_str):
                result = clean_company_info(input_str)
                self.assertEqual(result, expected)

    def test_date_formatting_edge_cases(self):
        """Test date formatting with edge cases."""
        test_cases = [
            ('2024-01-15', 'Jan 15, 2024'),
            ('N/A', 'N/A'),
            (None, 'N/A'),
            ('invalid-date', 'invalid-date')
        ]

        for input_date, expected in test_cases:
            with self.subTest(date=input_date):
                result = format_posted_date_enhanced(input_date)
                self.assertEqual(result, expected)


class TestSalaryFiltering(unittest.TestCase):
    """Test salary-based filtering functionality."""

    def setUp(self):
        """Set up test data with various salary formats."""
        self.jobs_with_salaries = pd.DataFrame({
            'title': ['Job1', 'Job2', 'Job3', 'Job4'],
            'salary_formatted': [
                '$50,000 - $80,000',  # Should appear in 50k-100k range
                '$120,000 - $150,000',  # Should appear in 100k+ range
                '$25/hour',  # ~$52,000 annually, should appear in 50k-100k
                'Not specified'  # Should appear in Any range
            ]
        })

    def test_salary_range_filtering(self):
        """Test filtering by salary ranges."""
        # Test 50k-100k range (should include first and third jobs)
        result_50_100k = filter_by_salary_range(self.jobs_with_salaries, '$50k-100k')
        self.assertEqual(len(result_50_100k), 2)

        # Test 100k+ range (should include second job)
        result_100k_plus = filter_by_salary_range(self.jobs_with_salaries, '$150k+')
        self.assertEqual(len(result_100k_plus), 1)

        # Test any range (should include all)
        result_any = filter_by_salary_range(self.jobs_with_salaries, 'Any')
        self.assertEqual(len(result_any), 4)


if __name__ == '__main__':
    unittest.main()
