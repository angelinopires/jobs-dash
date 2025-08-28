"""
Unit tests for dashboard display functions to prevent "nan" value issues.
Tests the clean_display_value and clean_company_info functions.
"""

import os
import sys
import unittest

import numpy as np
import pandas as pd

from utils.display_utils import clean_company_info, clean_display_value

# Add the parent directory to sys.path to import dashboard functions
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDisplayFunctions(unittest.TestCase):
    """Test dashboard display functions for proper nan handling."""

    def test_clean_display_value_valid_strings(self) -> None:
        """Test clean_display_value with valid string values."""
        self.assertEqual(clean_display_value("Software Engineer"), "Software Engineer")
        self.assertEqual(clean_display_value("Google Inc."), "Google Inc.")
        self.assertEqual(clean_display_value("Remote, US"), "Remote, US")
        self.assertEqual(clean_display_value("$100,000"), "$100,000")

    def test_clean_display_value_invalid_values(self) -> None:
        """Test clean_display_value with invalid/nan values."""
        # Test various forms of None/nan
        self.assertEqual(clean_display_value(str(None)), "Not available")
        self.assertEqual(clean_display_value(str(pd.NA)), "Not available")
        self.assertEqual(clean_display_value(str(np.nan)), "Not available")

        # Test string representations of invalid values
        self.assertEqual(clean_display_value("nan"), "Not available")
        self.assertEqual(clean_display_value("NaN"), "Not available")
        self.assertEqual(clean_display_value("none"), "Not available")
        self.assertEqual(clean_display_value("None"), "Not available")
        self.assertEqual(clean_display_value("NONE"), "Not available")
        self.assertEqual(clean_display_value("null"), "Not available")
        self.assertEqual(clean_display_value("NULL"), "Not available")
        self.assertEqual(clean_display_value("n/a"), "Not available")
        self.assertEqual(clean_display_value("N/A"), "Not available")
        self.assertEqual(clean_display_value(""), "Not available")
        self.assertEqual(clean_display_value("   "), "Not available")

    def test_clean_display_value_custom_default(self) -> None:
        """Test clean_display_value with custom default value."""
        self.assertEqual(clean_display_value(str(None), "Custom Default"), "Custom Default")
        self.assertEqual(clean_display_value("nan", "Missing Data"), "Missing Data")
        self.assertEqual(clean_display_value("", "Empty"), "Empty")

    def test_clean_display_value_edge_cases(self) -> None:
        """Test clean_display_value with edge cases."""
        # Test numbers that should be preserved
        self.assertEqual(clean_display_value(str(0)), "0")
        self.assertEqual(clean_display_value(str(42)), "42")
        self.assertEqual(clean_display_value(str(3.14)), "3.14")

        # Test strings that contain nan but are valid
        self.assertEqual(clean_display_value("banana"), "banana")
        self.assertEqual(clean_display_value("finance"), "finance")
        self.assertEqual(clean_display_value("nano technology"), "nano technology")

    def test_clean_company_info_valid_data(self) -> None:
        """Test clean_company_info with valid company information."""
        # Test with all valid data
        valid_info = "Industry: Technology | Size: 100-500 | Revenue: $10M-50M"
        self.assertEqual(clean_company_info(valid_info), valid_info)

        # Test with single valid field
        self.assertEqual(clean_company_info("Industry: Healthcare"), "Industry: Healthcare")

        # Test with mixed valid data
        mixed_info = "Industry: Finance | Size: 1000+ employees"
        self.assertEqual(clean_company_info(mixed_info), mixed_info)

    def test_clean_company_info_invalid_data(self) -> None:
        """Test clean_company_info with invalid/nan company information."""
        # Test completely invalid data
        self.assertEqual(clean_company_info("Industry: nan | Size: nan | Revenue: nan"), "Not available")
        self.assertEqual(clean_company_info("Industry: none | Size: null | Revenue: n/a"), "Not available")

        # Test None/nan values
        self.assertEqual(clean_company_info(str(None)), "Not available")
        self.assertEqual(clean_company_info(str(pd.NA)), "Not available")
        self.assertEqual(clean_company_info(str(np.nan)), "Not available")
        self.assertEqual(clean_company_info("nan"), "Not available")
        self.assertEqual(clean_company_info(""), "Not available")

    def test_clean_company_info_mixed_data(self) -> None:
        """Test clean_company_info with mix of valid and invalid data."""
        # Test with some valid, some invalid
        mixed_invalid = "Industry: Technology | Size: nan | Revenue: null"
        self.assertEqual(clean_company_info(mixed_invalid), "Industry: Technology")

        mixed_invalid2 = "Industry: nan | Size: 100-500 | Revenue: $10M+"
        self.assertEqual(clean_company_info(mixed_invalid2), "Size: 100-500 | Revenue: $10M+")

        mixed_invalid3 = "Industry: Healthcare | Size: none | Revenue: $1B+"
        self.assertEqual(clean_company_info(mixed_invalid3), "Industry: Healthcare | Revenue: $1B+")

    def test_clean_company_info_edge_cases(self) -> None:
        """Test clean_company_info with edge cases."""
        # Test malformed strings
        self.assertEqual(clean_company_info("InvalidFormat"), "Not available")
        self.assertEqual(clean_company_info("Industry"), "Not available")
        self.assertEqual(clean_company_info("Industry:"), "Not available")

        # Test strings that contain nan but are valid values
        valid_with_nan_substring = "Industry: Financial Services | Size: Management"
        self.assertEqual(clean_company_info(valid_with_nan_substring), valid_with_nan_substring)

    def test_clean_company_info_whitespace_handling(self) -> None:
        """Test clean_company_info handles whitespace properly."""
        # Test with extra whitespace
        whitespace_info = "  Industry: Technology  |  Size: 100-500  |  Revenue: $10M+  "
        expected = "Industry: Technology | Size: 100-500 | Revenue: $10M+"
        self.assertEqual(clean_company_info(whitespace_info), expected)

        # Test with whitespace around invalid values
        whitespace_invalid = "Industry:   nan   | Size:  none  | Revenue:   null   "
        self.assertEqual(clean_company_info(whitespace_invalid), "Not available")


class TestDisplayFunctionsIntegration(unittest.TestCase):
    """Integration tests for display functions with real-world data scenarios."""

    def test_job_details_scenario(self) -> None:
        """Test realistic job details scenario."""
        # Simulate a job record with mixed data quality
        job_data = {
            "title": "Software Engineer",
            "company_name": "Tech Corp",
            "location_formatted": "Remote, US",
            "salary_formatted": "$80,000 - $120,000",
            "company_info": "Industry: Technology | Size: nan | Revenue: $50M+",
            "job_type": "Full-time",
            "date_posted": "2025-01-15",
        }

        # Test each field
        self.assertEqual(clean_display_value(job_data["title"]), "Software Engineer")
        self.assertEqual(clean_display_value(job_data["company_name"]), "Tech Corp")
        self.assertEqual(clean_display_value(job_data["location_formatted"]), "Remote, US")
        self.assertEqual(clean_display_value(job_data["salary_formatted"]), "$80,000 - $120,000")
        self.assertEqual(clean_company_info(job_data["company_info"]), "Industry: Technology | Revenue: $50M+")
        self.assertEqual(clean_display_value(job_data["job_type"]), "Full-time")

    def test_empty_job_scenario(self) -> None:
        """Test scenario with mostly empty/invalid job data."""
        empty_job = {
            "title": None,
            "company_name": "nan",
            "location_formatted": "",
            "salary_formatted": "n/a",
            "company_info": "Industry: nan | Size: none | Revenue: null",
            "job_type": pd.NA,
            "description": "   ",
        }

        # All should return "Not available"
        for field, value in empty_job.items():
            if field == "company_info":
                self.assertEqual(clean_company_info(value), "Not available")
            else:
                self.assertEqual(clean_display_value(value), "Not available")


if __name__ == "__main__":
    unittest.main()
