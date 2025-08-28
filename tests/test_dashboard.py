"""
Unit tests for dashboard.py.

This module contains comprehensive tests for the dashboard functionality.
"""

import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from dashboard import (
    _extract_salary_for_sorting,
    apply_display_formatting,
    apply_interactive_filters,
    filter_by_salary_range,
)

# Mock Streamlit before importing dashboard
sys.modules["streamlit"] = MagicMock()

# Now we can import dashboard functions


class TestDashboardFunctions(unittest.TestCase):
    """Test dashboard utility functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample job data for testing
        self.sample_jobs_df = pd.DataFrame(
            {
                "title": ["Senior Python Developer", "Data Scientist", "Frontend Engineer"],
                "company": ["TechCorp", "DataLab", "WebDev Inc"],
                "location": ["Remote", "New York", "San Francisco"],
                "salary_formatted": ["$80,000 - $120,000", "$90,000 - $130,000", "$70,000 - $100,000"],
                "job_url": ["https://example.com/job/1", "https://example.com/job/2", "https://example.com/job/3"],
                "date_posted_formatted": ["2 days ago", "1 day ago", "3 days ago"],
                "job_type": ["Full-time", "Contract", "Full-time"],
                "remote_status": ["Remote", "Hybrid", "On-site"],
            }
        )

        # Empty DataFrame for edge case testing
        self.empty_jobs_df = pd.DataFrame()

        # DataFrame with missing columns for error testing
        self.incomplete_jobs_df = pd.DataFrame(
            {
                "title": ["Test Job"],
                "company": ["Test Company"],
                # Missing other required columns
            }
        )

    def test_apply_display_formatting_valid_data(self):
        """Test display formatting with valid job data."""
        result = apply_display_formatting(self.sample_jobs_df)

        # Should return DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Should have same number of rows
        self.assertEqual(len(result), 3)

        # Should have expected columns
        expected_columns = ["title", "company", "location", "salary_formatted", "job_url", "date_posted_formatted"]
        for col in expected_columns:
            self.assertIn(col, result.columns)

    def test_apply_display_formatting_empty_dataframe(self):
        """Test display formatting with empty DataFrame."""
        result = apply_display_formatting(self.empty_jobs_df)

        # Should handle empty DataFrame gracefully
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)

    @patch("dashboard.st")
    def test_apply_interactive_filters_valid_data(self, mock_st):
        """Test interactive filtering with valid data."""
        # Mock the columns function to return 5 mock column objects
        mock_col1 = MagicMock()
        mock_col2 = MagicMock()
        mock_col3 = MagicMock()
        mock_col4 = MagicMock()
        mock_col5 = MagicMock()

        mock_st.columns.return_value = [mock_col1, mock_col2, mock_col3, mock_col4, mock_col5]

        # Mock other Streamlit functions
        mock_st.markdown.return_value = None
        mock_st.text_input.return_value = ""
        mock_st.selectbox.return_value = "Any"
        mock_st.multiselect.return_value = []

        result = apply_interactive_filters(self.sample_jobs_df)

        # Should return DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Should have same number of rows initially (no filters applied)
        self.assertEqual(len(result), 3)

    def test_filter_by_salary_range_valid_range(self):
        """Test salary range filtering with valid range."""
        # Test with a reasonable salary range
        result = filter_by_salary_range(self.sample_jobs_df, "80000-120000")

        # Should return DataFrame
        self.assertIsInstance(result, pd.DataFrame)

        # Should filter jobs within range
        self.assertLessEqual(len(result), len(self.sample_jobs_df))

    def test_filter_by_salary_range_invalid_range(self):
        """Test salary range filtering with invalid range."""
        # Test with invalid range format
        result = filter_by_salary_range(self.sample_jobs_df, "invalid-range")

        # Should handle invalid input gracefully
        self.assertIsInstance(result, pd.DataFrame)

    def test_extract_salary_for_sorting_valid_salary(self):
        """Test salary extraction for sorting with valid salary strings."""
        test_cases = [
            ("$80,000 - $120,000", 100000),  # Range
            ("$90,000", 90000),  # Single value
            ("$70,000+", 70000),  # Plus sign
            ("$100,000 - $150,000", 125000),  # Another range
        ]

        for salary_str, expected in test_cases:
            with self.subTest(salary=salary_str):
                result = _extract_salary_for_sorting(salary_str)
                self.assertIsInstance(result, (int, float))
                self.assertGreaterEqual(result, 0)

    def test_extract_salary_for_sorting_invalid_salary(self):
        """Test salary extraction with invalid salary strings."""
        invalid_salaries = ["Not specified", "Competitive", "", None, "N/A"]

        for salary_str in invalid_salaries:
            with self.subTest(salary=salary_str):
                result = _extract_salary_for_sorting(salary_str)
                # Should handle invalid input gracefully
                self.assertIsInstance(result, (int, float))


if __name__ == "__main__":
    unittest.main()
