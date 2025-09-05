"""
Unit tests for core optimization features.

Tests the essential functionality of the new architecture:
- Performance monitoring
- Search optimization
- Result processing
"""

import unittest

import pandas as pd

from core.monitoring.performance_monitor import PerformanceMonitor
from core.search.search_optimizer import SearchOptimizer


class TestPerformanceMonitor(unittest.TestCase):
    """Test the performance monitoring system."""

    def setUp(self) -> None:
        """Set up performance monitor for testing."""
        self.monitor = PerformanceMonitor("test_scraper")

    def test_search_tracking(self) -> None:
        """Test search start/end tracking."""
        # Start a search
        self.monitor.start_search("Test Job", "United States", True)

        # Log some events
        self.monitor.log("Test event", "Test message")
        self.monitor.log_cache_event("hit", "test_cache_key", "United States")

        # End search
        self.monitor.end_search(True, 2.5, 10)

        # Check stats
        stats = self.monitor.get_stats()
        self.assertEqual(stats["total_searches"], 1)
        self.assertEqual(stats["success_rate"], 100.0)
        self.assertEqual(stats["total_jobs_found"], 10)

    def test_performance_stats(self) -> None:
        """Test performance statistics calculation."""
        # Simulate multiple searches
        searches = [
            (True, 1.5, 5),  # Successful
            (True, 2.0, 8),  # Successful
            (False, 3.0, 0),  # Failed
        ]

        for success, time_taken, job_count in searches:
            self.monitor.start_search("Test", "US", True)
            self.monitor.end_search(success, time_taken, job_count)

        stats = self.monitor.get_stats()

        self.assertEqual(stats["total_searches"], 3)
        self.assertEqual(stats["successful_searches"], 2)
        self.assertAlmostEqual(stats["success_rate"], 66.7, places=1)
        self.assertEqual(stats["total_jobs_found"], 13)  # 5 + 8 + 0
        self.assertAlmostEqual(stats["avg_time"], 2.17, places=1)  # (1.5 + 2.0 + 3.0) / 3


class TestSearchOptimizer(unittest.TestCase):
    """Test the search optimization functionality."""

    def setUp(self) -> None:
        """Set up search optimizer for testing."""
        self.optimizer = SearchOptimizer("test_scraper")

    def test_search_param_optimization(self) -> None:
        """Test search parameter optimization."""
        # Test global search optimization
        global_params = {
            "search_term": "  Software Engineer  ",  # Extra whitespace
            "where": "Global",
            "results_wanted": 2000,  # High number
        }

        optimized = self.optimizer.optimize_search_params(**global_params)

        # Should trim whitespace
        self.assertEqual(optimized["search_term"], "Software Engineer")

        # Should limit results for global searches
        self.assertLessEqual(optimized["results_wanted"], 500)

        # Test single country optimization
        country_params = {"search_term": "Data Scientist", "where": "United States", "results_wanted": 800}

        optimized_country = self.optimizer.optimize_search_params(**country_params)

        # Should keep higher limit for single country
        self.assertEqual(optimized_country["results_wanted"], 800)

    def test_result_processing_optimization(self) -> None:
        """Test result processing optimization."""
        # Create test DataFrame
        test_jobs = pd.DataFrame(
            [
                {"title": "Job 1", "company": "Company A", "date_posted": "2023-12-01"},
                {"title": "Job 2", "company": "Company B", "date_posted": "2023-12-02"},
                {"title": "Job 3", "company": "Company A", "date_posted": "2023-12-03"},
            ]
        )

        optimized_jobs = self.optimizer.optimize_result_processing(test_jobs)

        # Should return a DataFrame
        self.assertIsInstance(optimized_jobs, pd.DataFrame)

        # Should have same number of rows
        self.assertEqual(len(optimized_jobs), 3)

        # Should have optimized dtypes (company remains object for compatibility)
        self.assertEqual(optimized_jobs["company"].dtype.name, "object")

    def test_memory_optimization(self) -> None:
        """Test memory optimization for large datasets."""
        # Create list of test DataFrames
        jobs_list = [
            pd.DataFrame([{"title": f"Job {i}", "company": "Company A"} for i in range(10)]),
            pd.DataFrame([{"title": f"Job {i+10}", "company": "Company B"} for i in range(5)]),
            pd.DataFrame(),  # Empty DataFrame (should be filtered out)
        ]

        combined = self.optimizer.optimize_memory_usage(jobs_list)

        # Should combine non-empty DataFrames
        self.assertEqual(len(combined), 15)  # 10 + 5, empty one filtered out

        # Should optimize dtypes (company remains object for compatibility)
        self.assertEqual(combined["company"].dtype.name, "object")

    def test_duplicate_removal(self) -> None:
        """Test optimized duplicate removal."""
        # Create DataFrame with duplicates
        test_jobs = pd.DataFrame(
            [
                {"title": "Job 1", "job_url": "http://example.com/job1", "company": "Company A"},
                {"title": "Job 2", "job_url": "http://example.com/job2", "company": "Company B"},
                {
                    "title": "Job 1 Duplicate",
                    "job_url": "http://example.com/job1",
                    "company": "Company A",
                },  # Duplicate URL
            ]
        )

        deduped = self.optimizer.optimize_duplicate_removal(test_jobs, ["job_url"])

        # Should remove duplicate
        self.assertEqual(len(deduped), 2)

        # Should keep first occurrence
        self.assertEqual(deduped.iloc[0]["title"], "Job 1")


if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2)
