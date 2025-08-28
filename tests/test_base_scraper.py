"""
Unit tests for BaseJobScraper abstract class.

Tests the core architecture and contracts that all scrapers must implement.
"""

import time
import unittest
from typing import Any, Dict

import pandas as pd

# Import the base scraper
from scrapers.base_scraper import BaseJobScraper, FilterCapabilities


class TestFilterCapabilities(unittest.TestCase):
    """Test the FilterCapabilities helper class."""

    def test_api_filters_defined(self) -> None:
        """Test that API filters are properly defined."""
        api_filters = FilterCapabilities.API_FILTERS

        # Should include common API-level filters
        self.assertIn("search_term", api_filters)
        self.assertIn("location", api_filters)
        # job_type is now handled via post-processing only
        self.assertIn("time_filter", api_filters)

        # Each should have a description
        for filter_name, description in api_filters.items():
            self.assertIsInstance(description, str)
            self.assertTrue(len(description) > 0)

    def test_post_processing_filters_defined(self) -> None:
        """Test that post-processing filters are properly defined."""
        post_filters = FilterCapabilities.POST_PROCESSING_FILTERS

        # Should include filters that typically need post-processing
        self.assertIn("salary_currency", post_filters)
        self.assertIn("salary_range", post_filters)
        self.assertIn("company_size", post_filters)

        # Each should have a description
        for filter_name, description in post_filters.items():
            self.assertIsInstance(description, str)
            self.assertTrue(len(description) > 0)

    def test_filter_classification(self) -> None:
        """Test filter classification methods."""
        # API filters should be identified correctly
        self.assertTrue(FilterCapabilities.is_api_filter("search_term"))
        self.assertFalse(FilterCapabilities.is_api_filter("job_type"))  # Now post-processing only
        self.assertFalse(FilterCapabilities.is_api_filter("salary_currency"))

        # Post-processing filters should be identified correctly
        self.assertTrue(FilterCapabilities.is_post_processing_filter("salary_currency"))
        self.assertTrue(FilterCapabilities.is_post_processing_filter("company_size"))
        self.assertFalse(FilterCapabilities.is_post_processing_filter("search_term"))

    def test_get_all_filters(self) -> None:
        """Test getting all available filters."""
        all_filters = FilterCapabilities.get_all_filters()

        # Should combine both API and post-processing filters
        api_count = len(FilterCapabilities.API_FILTERS)
        post_count = len(FilterCapabilities.POST_PROCESSING_FILTERS)
        self.assertEqual(len(all_filters), api_count + post_count)


class ConcreteTestScraper(BaseJobScraper):
    """
    Concrete implementation of BaseJobScraper for testing.

    This is like creating a mock implementation that follows the interface.
    """

    def __init__(self) -> None:
        super().__init__()
        self.api_call_count: int = 0
        self.mock_jobs_data: pd.DataFrame = pd.DataFrame()

    def get_supported_api_filters(self) -> Dict[str, bool]:
        """Test implementation supporting common filters."""
        return {
            "search_term": True,
            "location": True,
            "job_type": False,  # Handled via post-processing
            "time_filter": True,
            "salary_currency": False,  # Not supported - needs post-processing
            "company_size": False,  # Not supported - needs post-processing
        }

    def _build_api_search_params(self, **filters: Any) -> Dict[str, Any]:
        """Test implementation that builds search params."""
        supported = self.get_supported_api_filters()
        params = {}

        # Only include supported filters
        if supported.get("search_term") and filters.get("search_term"):
            params["query"] = filters["search_term"]

        if supported.get("location") and filters.get("location"):
            params["location"] = filters["location"]

        # job_type is handled via post-processing only

        if supported.get("time_filter") and filters.get("time_filter"):
            params["hours_old"] = 24  # Mock conversion

        return params

    def _call_scraping_api(self, search_params: Dict[str, Any]) -> pd.DataFrame:
        """Mock API call that returns test data."""
        self.api_call_count += 1

        # Simulate API delay
        time.sleep(0.01)

        # Return mock data or empty DataFrame
        return self.mock_jobs_data.copy() if not self.mock_jobs_data.empty else pd.DataFrame()

    def set_mock_data(self, jobs_df: pd.DataFrame) -> None:
        """Helper to set mock data for testing."""
        self.mock_jobs_data = jobs_df


class TestBaseJobScraper(unittest.TestCase):
    """Test the BaseJobScraper abstract class behavior."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.scraper = ConcreteTestScraper()

        # Create sample job data
        self.sample_jobs = pd.DataFrame(
            {
                "title": ["Python Developer", "JavaScript Engineer", "Data Scientist"],
                "company": ["TechCorp", "WebDev Inc", "DataLab"],
                "location": ["Remote", "New York", "San Francisco"],
                "salary_formatted": ["$80,000 - $120,000", "€60,000 - €80,000", "$90,000+"],
                "currency": ["USD", "EUR", "USD"],
                "date_posted": ["2023-12-01", "2023-12-02", "2023-12-03"],
                "job_url": ["http://example.com/1", "http://example.com/2", "http://example.com/3"],
            }
        )

    def test_abstract_class_cannot_be_instantiated(self) -> None:
        """Test that BaseJobScraper cannot be instantiated directly."""
        with self.assertRaises(TypeError):
            BaseJobScraper()  # type: ignore[abstract]

    def test_concrete_scraper_can_be_instantiated(self) -> None:
        """Test that concrete implementation can be instantiated."""
        scraper = ConcreteTestScraper()
        self.assertIsInstance(scraper, BaseJobScraper)
        self.assertIsInstance(scraper, ConcreteTestScraper)

    def test_search_jobs_returns_required_fields(self) -> None:
        """Test that search_jobs always returns required fields."""
        # Test with empty results
        result = self.scraper.search_jobs(search_term="Python")

        # Should have all required fields
        required_fields = ["success", "jobs", "count", "search_time", "message", "metadata"]
        for field in required_fields:
            self.assertIn(field, result, f"Missing required field: {field}")

        # Validate field types
        self.assertIsInstance(result["success"], bool)
        self.assertTrue(isinstance(result["jobs"], (pd.DataFrame, type(None))))
        self.assertIsInstance(result["count"], int)
        self.assertIsInstance(result["search_time"], (int, float))
        self.assertIsInstance(result["message"], str)
        self.assertIsInstance(result["metadata"], dict)

    def test_search_jobs_includes_timing(self) -> None:
        """Test that search_time is properly calculated."""
        start_time = time.time()
        result = self.scraper.search_jobs(search_term="Python")
        end_time = time.time()

        # Search time should be reasonable
        self.assertGreater(result["search_time"], 0)
        self.assertLess(result["search_time"], end_time - start_time + 0.1)  # Allow small margin

    def test_search_jobs_with_mock_data(self) -> None:
        """Test search_jobs with actual mock data."""
        self.scraper.set_mock_data(self.sample_jobs)

        result = self.scraper.search_jobs(search_term="Python", location="Remote", include_remote=True)

        # Should be successful
        self.assertTrue(result["success"])
        self.assertIsInstance(result["jobs"], pd.DataFrame)
        self.assertEqual(result["count"], 3)  # All sample jobs
        self.assertGreater(result["search_time"], 0)

        # Should call API once
        self.assertEqual(self.scraper.api_call_count, 1)

    def test_api_vs_post_processing_filter_separation(self) -> None:
        """Test that filters are properly separated between API and post-processing."""
        self.scraper.set_mock_data(self.sample_jobs)

        # Use filters - some supported by API, some needing post-processing
        result = self.scraper.search_jobs(
            search_term="Python",  # API supported
            location="Remote",  # API supported
            include_remote=True,  # Remote level parameter
            salary_currency="USD",  # Post-processing needed
            company_size="50-100",  # Post-processing needed
        )

        # Check metadata shows correct filter usage
        metadata = result["metadata"]
        api_filters = metadata["api_filters_used"]
        post_filters = metadata["post_processing_applied"]

        # API filters should include supported ones
        self.assertIn("query", api_filters)  # search_term mapped to 'query'
        self.assertIn("location", api_filters)

        # Post-processing should include unsupported ones
        self.assertIn("salary_currency", post_filters)

    def test_post_processing_salary_currency_filter(self) -> None:
        """Test salary currency post-processing filter."""
        # Create jobs with different currencies
        multi_currency_jobs = pd.DataFrame(
            {
                "title": ["Job 1", "Job 2", "Job 3"],
                "company": ["Company A", "Company B", "Company C"],
                "currency": ["USD", "EUR", "USD"],
                "salary_formatted": ["$80,000", "€60,000", "$90,000"],
                "job_url": ["url1", "url2", "url3"],
            }
        )

        self.scraper.set_mock_data(multi_currency_jobs)

        # Filter for USD only
        result = self.scraper.search_jobs(search_term="Developer", salary_currency="USD")

        # Should only return USD jobs
        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)  # Only USD jobs

        # Check that the jobs are actually filtered
        filtered_jobs = result["jobs"]
        currencies = filtered_jobs["currency"].unique()
        self.assertEqual(len(currencies), 1)
        self.assertEqual(currencies[0], "USD")

    def test_rate_limiting(self) -> None:
        """Test that rate limiting is enforced."""
        # Set a shorter delay for testing
        self.scraper.min_delay = 0.1

        # First search
        result1 = self.scraper.search_jobs(search_term="Test1")

        # Second search immediately after
        start2 = time.time()
        result2 = self.scraper.search_jobs(search_term="Test2")
        end2 = time.time()

        # Both should succeed
        self.assertTrue(result1["success"])
        self.assertTrue(result2["success"])

        # Second search should have been delayed
        total_second_search_time = end2 - start2

        # The second search should have taken at least the minimum delay
        # (accounting for the delay enforcement)
        self.assertGreater(total_second_search_time, 0.05)  # At least some delay

    def test_error_handling(self) -> None:
        """Test error handling in search_jobs."""

        # Create a scraper that will raise an exception
        class FailingScraper(ConcreteTestScraper):
            def _call_scraping_api(self, search_params: Dict[str, Any]) -> pd.DataFrame:
                raise Exception("API is down!")

        failing_scraper = FailingScraper()
        result = failing_scraper.search_jobs(search_term="Test")

        # Should handle error gracefully
        self.assertFalse(result["success"])
        self.assertIsNone(result["jobs"])
        self.assertEqual(result["count"], 0)
        self.assertIn("API is down!", result["message"])
        self.assertIn("error", result["metadata"])

        # Should still include search_time even for errors
        self.assertIn("search_time", result)
        self.assertIsInstance(result["search_time"], (int, float))


if __name__ == "__main__":
    unittest.main()
