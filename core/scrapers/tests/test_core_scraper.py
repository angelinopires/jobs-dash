"""
Core scraper functionality tests.

Tests for the main scraping pipeline and core functionality.
Focuses on essential paths rather than exhaustive edge cases.
"""

import os
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from core.scrapers.indeed_scraper import get_indeed_scraper

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestScraperCore(unittest.TestCase):
    """Test core scraper functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()

    def test_scraper_initialization(self) -> None:
        """Test that scraper initializes properly."""
        self.assertIsNotNone(self.scraper)
        # Should have required methods
        self.assertTrue(hasattr(self.scraper, "search_jobs"))
        self.assertTrue(hasattr(self.scraper, "_process_jobs"))

    @patch("core.scrapers.indeed_scraper.IndeedScraper.search_jobs")
    def test_search_jobs_interface(self, mock_search: MagicMock) -> None:
        """Test the main search_jobs interface."""
        # Mock successful search
        mock_search.return_value = {
            "success": True,
            "jobs": pd.DataFrame({"title": ["Test Job"]}),
            "count": 1,
            "search_time": 1.5,
            "message": "Success",
        }

        result = self.scraper.search_jobs(
            search_term="Python Developer", where="United States", include_remote=True, time_filter="24h"
        )

        # Verify result structure
        self.assertIn("success", result)
        self.assertIn("jobs", result)
        self.assertIn("count", result)
        self.assertIn("search_time", result)

    def test_job_processing_adds_required_columns(self) -> None:
        """Test that job processing adds all required columns."""
        raw_jobs = pd.DataFrame(
            {
                "title": ["Software Engineer"],
                "company": ["Tech Corp"],
                "location": ["Remote"],
                "date_posted": ["2024-01-15"],
                "site": ["indeed"],
                "job_url": ["https://example.com/job1"],
            }
        )

        processed = self.scraper._process_jobs(raw_jobs)

        # Should have all required columns
        required_columns = [
            "title",
            "company_name",
            "location_formatted",
            "salary_formatted",
            "date_posted_formatted",
            "company_info",
            "job_url",
        ]

        for col in required_columns:
            self.assertIn(col, processed.columns)

    def test_company_info_formatting(self) -> None:
        """Test company info formatting from raw data."""
        raw_row = {"company_industry": "Technology", "company_num_employees": "100-500", "company_revenue": "$10M-50M"}

        result = self.scraper._format_company_info(raw_row)

        # Should format properly
        self.assertIn("Industry: Technology", result)
        self.assertIn("Size: 100-500", result)
        self.assertIn("Revenue: $10M-50M", result)

    def test_company_info_handles_missing_data(self) -> None:
        """Test company info formatting with missing data."""
        incomplete_row = {"company_industry": "Technology", "company_num_employees": None, "company_revenue": None}

        result = self.scraper._format_company_info(incomplete_row)

        # Should only include valid data
        self.assertEqual(result, "Industry: Technology")


class TestCountrySupport(unittest.TestCase):
    """Test country and location support."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        from data.countries import get_country_options, get_indeed_country_name

        self.get_country_options = get_country_options
        self.get_indeed_country_name = get_indeed_country_name

    def test_country_options_structure(self) -> None:
        """Test that country options are properly structured."""
        options = self.get_country_options()

        self.assertIsInstance(options, list)
        self.assertGreater(len(options), 0)
        self.assertIn("United States", options)
        self.assertIn("Global", options)

    def test_indeed_country_mapping(self) -> None:
        """Test mapping of countries to Indeed country codes."""
        # Test common countries
        test_cases = [("United States", "usa"), ("United Kingdom", "uk"), ("Canada", "canada")]

        for country, expected_code in test_cases:
            with self.subTest(country=country):
                result = self.get_indeed_country_name(country)
                self.assertEqual(result, expected_code)


if __name__ == "__main__":
    unittest.main()
