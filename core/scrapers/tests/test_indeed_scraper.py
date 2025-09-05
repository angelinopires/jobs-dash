"""
Unit tests for IndeedScraper.

Focused on core functionality without external dependencies.
All heavy components are mocked to ensure fast, reliable test execution.
"""

import unittest
from typing import Any
from unittest.mock import Mock, patch

import pandas as pd


class TestIndeedScraperCore(unittest.TestCase):
    """Test core IndeedScraper functionality with minimal dependencies."""

    @patch("core.search.search_orchestrator.RedisCacheManager")
    @patch("core.search.search_orchestrator.get_circuit_breaker")
    @patch("core.search.search_orchestrator.get_rate_limiter")
    @patch("core.search.search_orchestrator.PerformanceMonitor")
    @patch("core.search.search_orchestrator.ThreadingManager")
    def setUp(self, mock_threading: Any, mock_perf: Any, mock_rate: Any, mock_circuit: Any, mock_redis: Any) -> None:
        """Set up with all external dependencies mocked."""
        # Mock all heavy dependencies
        mock_redis.return_value = Mock()
        mock_circuit.return_value = Mock()
        mock_rate.return_value = Mock()
        mock_perf.return_value = Mock()
        mock_threading.return_value = Mock()

        # Import here to avoid initialization overhead
        from core.scrapers.indeed_scraper import IndeedScraper

        self.scraper_class = IndeedScraper

    def test_get_supported_api_filters(self) -> None:
        """Test that supported API filters are properly defined."""
        scraper = self.scraper_class()
        supported = scraper.get_supported_api_filters()

        self.assertIsInstance(supported, dict)

        # Indeed should support these basic filters
        expected = ["search_term", "location", "time_filter", "results_wanted"]
        for filter_name in expected:
            self.assertIn(filter_name, supported)
            self.assertTrue(supported[filter_name], f"{filter_name} should be supported")

    def test_build_api_search_params(self) -> None:
        """Test building API search parameters."""
        scraper = self.scraper_class()

        # Call with **kwargs syntax
        params = scraper._build_api_search_params(
            search_term="Python Developer", where="United States", results_wanted=1000
        )

        # Should include required parameters
        self.assertIn("search_term", params)
        self.assertIn("site_name", params)
        self.assertEqual(params["site_name"], ["indeed"])
        self.assertEqual(params["search_term"], "Python Developer")

    @patch("core.scrapers.indeed_scraper.scrape_jobs")
    def test_call_scraping_api_success(self, mock_scrape_jobs: Any) -> None:
        """Test successful API call with mocked scrape_jobs."""
        scraper = self.scraper_class()

        # Mock successful response
        mock_jobs = pd.DataFrame(
            {
                "title": ["Python Developer"],
                "company": ["TechCorp"],
                "location": ["Remote"],
            }
        )
        mock_scrape_jobs.return_value = mock_jobs

        search_params = {"search_term": "Python", "site_name": ["indeed"]}

        result = scraper._call_scraping_api(search_params)

        # Should return the DataFrame
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["title"], "Python Developer")

    @patch("core.scrapers.indeed_scraper.scrape_jobs")
    def test_call_scraping_api_failure(self, mock_scrape_jobs: Any) -> None:
        """Test API call failure handling."""
        scraper = self.scraper_class()

        # Mock API failure
        mock_scrape_jobs.side_effect = Exception("API Error")

        search_params = {"search_term": "Python"}

        result = scraper._call_scraping_api(search_params)

        # Should return empty DataFrame on error
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)

    def test_process_jobs_empty_dataframe(self) -> None:
        """Test processing empty DataFrame."""
        scraper = self.scraper_class()
        empty_df = pd.DataFrame()

        result = scraper._process_jobs(empty_df)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)

    def test_process_jobs_adds_columns(self) -> None:
        """Test that processing adds required columns."""
        scraper = self.scraper_class()

        # Sample raw job data
        raw_jobs = pd.DataFrame(
            {
                "title": ["Software Engineer"],
                "company": ["TechCorp"],
                "location": ["San Francisco, CA"],
            }
        )

        result = scraper._process_jobs(raw_jobs)

        # Should add required columns (check what's actually added)
        # Based on the actual processing, it adds columns like date_posted, site, etc.
        expected_columns = ["site", "date_posted", "job_url"]
        for col in expected_columns:
            self.assertIn(col, result.columns)

        # Should preserve original data
        self.assertEqual(result.iloc[0]["title"], "Software Engineer")


if __name__ == "__main__":
    unittest.main()
