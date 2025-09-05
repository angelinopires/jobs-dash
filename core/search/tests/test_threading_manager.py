"""
Unit tests for ThreadingManager - Phase 2 parallel processing.

Tests:
- Parallel country searches with ThreadPoolExecutor
- Real-time progress callbacks
- Error handling for failed country searches
- Performance monitoring and speedup tracking
- Memory-efficient result aggregation
"""

import unittest
from typing import Any, Dict
from unittest.mock import Mock

import pandas as pd

from ..threading_manager import SearchResult, SearchTask, ThreadingManager


class TestThreadingManager(unittest.TestCase):
    """Test cases for ThreadingManager parallel processing."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.threading_manager = ThreadingManager(max_workers=2, timeout_per_country=10)

        # Sample job data
        self.sample_jobs = pd.DataFrame(
            {
                "job_title": ["Software Engineer", "Data Scientist"],
                "company": ["Tech Corp", "Data Inc"],
                "location": ["Remote", "New York"],
                "job_url": ["https://example.com/1", "https://example.com/2"],
                "salary": ["$100k", "$120k"],
            }
        )

    def test_init(self) -> None:
        """Test ThreadingManager initialization."""
        self.assertEqual(self.threading_manager.max_workers, 2)
        self.assertEqual(self.threading_manager.timeout_per_country, 10)
        self.assertEqual(self.threading_manager.total_searches, 0)
        self.assertEqual(self.threading_manager.successful_searches, 0)
        self.assertEqual(self.threading_manager.failed_searches, 0)

    def test_empty_countries_list(self) -> None:
        """Test handling of empty countries list."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        result = self.threading_manager.search_countries_parallel(
            countries=[],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["search_time"], 0.0)
        self.assertEqual(result["message"], "No countries provided")
        self.assertEqual(result["metadata"]["countries_searched"], 0)

        # Should not call search function
        mock_search_func.assert_not_called()

    def test_single_country_search(self) -> None:
        """Test parallel search with single country."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        # Mock successful search result
        mock_search_func.return_value = {"success": True, "jobs": self.sample_jobs, "count": 2, "search_time": 1.5}

        result = self.threading_manager.search_countries_parallel(
            countries=["United States"],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 2)
        self.assertGreater(result["search_time"], 0)
        self.assertEqual(result["metadata"]["countries_searched"], 1)
        self.assertEqual(result["metadata"]["total_countries"], 1)
        self.assertEqual(result["metadata"]["failed_countries"], 0)

        # Verify search function was called correctly
        mock_search_func.assert_called_once()
        call_args = mock_search_func.call_args
        self.assertEqual(call_args[1]["search_term"], "Software Engineer")
        self.assertEqual(call_args[1]["where"], "United States")

    def test_multiple_countries_parallel(self) -> None:
        """Test parallel search across multiple countries."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        # Mock different results for different countries
        def mock_search(**kwargs: Any) -> Dict[str, Any]:
            country = kwargs.get("where")
            if country == "United States":
                return {"success": True, "jobs": self.sample_jobs, "count": 2, "search_time": 1.0}
            elif country == "Canada":
                return {
                    "success": True,
                    "jobs": pd.DataFrame(
                        {
                            "job_title": ["Developer"],
                            "company": ["CanTech"],
                            "location": ["Toronto"],
                            "job_url": ["https://example.com/3"],
                            "salary": ["$90k"],
                        }
                    ),
                    "count": 1,
                    "search_time": 1.5,
                }
            else:
                return {"success": False, "jobs": pd.DataFrame(), "count": 0, "message": "Country not supported"}

        mock_search_func.side_effect = mock_search

        result = self.threading_manager.search_countries_parallel(
            countries=["United States", "Canada", "Invalid Country"],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 3)  # 2 from US + 1 from Canada
        self.assertGreater(result["search_time"], 0)
        self.assertEqual(result["metadata"]["countries_searched"], 2)  # US and Canada
        self.assertEqual(result["metadata"]["total_countries"], 3)
        self.assertEqual(result["metadata"]["failed_countries"], 1)  # Invalid Country

        # Verify search function was called for each country
        self.assertEqual(mock_search_func.call_count, 3)

    def test_failed_country_search(self) -> None:
        """Test handling of failed country searches."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        # Mock failed search
        mock_search_func.return_value = {"success": False, "jobs": pd.DataFrame(), "count": 0, "message": "API error"}

        result = self.threading_manager.search_countries_parallel(
            countries=["Failed Country"],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["metadata"]["countries_searched"], 0)
        self.assertEqual(result["metadata"]["failed_countries"], 1)

    def test_exception_handling(self) -> None:
        """Test handling of exceptions during search."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        # Mock function that raises exception
        mock_search_func.side_effect = Exception("Network error")

        result = self.threading_manager.search_countries_parallel(
            countries=["Exception Country"],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        self.assertTrue(result["success"])
        self.assertEqual(result["count"], 0)
        self.assertEqual(result["metadata"]["countries_searched"], 0)
        self.assertEqual(result["metadata"]["failed_countries"], 1)

    def test_progress_callback(self) -> None:
        """Test that progress callbacks are called correctly."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        # Mock successful search
        mock_search_func.return_value = {"success": True, "jobs": self.sample_jobs, "count": 2, "search_time": 1.0}

        self.threading_manager.search_countries_parallel(
            countries=["Country1", "Country2"],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        # Verify correct number of callback calls (1 initial + 2 countries + 1 final = 4)
        self.assertEqual(mock_progress_callback.call_count, 4)

        # Verify initial progress call
        initial_call = mock_progress_callback.call_args_list[0]
        self.assertIn("🚀 Starting parallel search across 2 countries", initial_call[0][0])
        self.assertEqual(initial_call[0][1], 0.05)

        # Verify per-country progress calls (calls 1 and 2)
        # Progress calculation: 0.05 + (completed_countries / total_countries) * 0.9
        # Country 1: 0.05 + (1/2) * 0.9 = 0.5
        # Country 2: 0.05 + (2/2) * 0.9 = 0.95
        # Note: Order of completion is not deterministic due to ThreadPoolExecutor
        first_country_call = mock_progress_callback.call_args_list[1]
        self.assertIn("🌍 1/2 countries", first_country_call[0][0])
        self.assertTrue("✅ Country1" in first_country_call[0][0] or "✅ Country2" in first_country_call[0][0])
        self.assertEqual(first_country_call[0][1], 0.5)

        second_country_call = mock_progress_callback.call_args_list[2]
        self.assertIn("🌍 2/2 countries", second_country_call[0][0])
        self.assertTrue("✅ Country1" in second_country_call[0][0] or "✅ Country2" in second_country_call[0][0])
        self.assertAlmostEqual(second_country_call[0][1], 0.95, places=2)

        # Verify both countries are mentioned across the two progress calls
        all_country_messages = [
            mock_progress_callback.call_args_list[1][0][0],
            mock_progress_callback.call_args_list[2][0][0],
        ]
        country1_mentioned = any("✅ Country1" in msg for msg in all_country_messages)
        country2_mentioned = any("✅ Country2" in msg for msg in all_country_messages)
        self.assertTrue(country1_mentioned, "Country1 should be mentioned in progress callbacks")
        self.assertTrue(country2_mentioned, "Country2 should be mentioned in progress callbacks")

        # Verify final progress call
        final_call = mock_progress_callback.call_args_list[3]
        self.assertIn("🎉 Parallel search complete", final_call[0][0])
        self.assertEqual(final_call[0][1], 1.0)

    def test_duplicate_removal(self) -> None:
        """Test that duplicate jobs are removed based on job_url."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        # Create jobs with duplicate URLs
        jobs1 = pd.DataFrame(
            {
                "job_title": ["Job 1", "Job 2"],
                "job_url": ["https://example.com/1", "https://example.com/2"],
                "company": ["Company A", "Company B"],
            }
        )

        jobs2 = pd.DataFrame(
            {
                "job_title": ["Job 2", "Job 3"],  # Job 2 is duplicate
                "job_url": ["https://example.com/2", "https://example.com/3"],
                "company": ["Company B", "Company C"],
            }
        )

        def mock_search(**kwargs: Any) -> Dict[str, Any]:
            country = kwargs.get("where")
            if country == "Country1":
                return {"success": True, "jobs": jobs1, "count": 2}
            else:
                return {"success": True, "jobs": jobs2, "count": 2}

        mock_search_func.side_effect = mock_search

        result = self.threading_manager.search_countries_parallel(
            countries=["Country1", "Country2"],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        # Should have 3 unique jobs (Job 1, Job 2, Job 3)
        self.assertEqual(result["count"], 3)

    def test_performance_stats(self) -> None:
        """Test performance statistics tracking."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        # Mock successful searches
        mock_search_func.return_value = {"success": True, "jobs": self.sample_jobs, "count": 2, "search_time": 1.0}

        # Run multiple searches
        for _ in range(3):
            self.threading_manager.search_countries_parallel(
                countries=["Country1", "Country2"],
                search_func=mock_search_func,
                search_term="Software Engineer",
                progress_callback=mock_progress_callback,
            )

        stats = self.threading_manager.get_performance_stats()

        self.assertEqual(stats["total_searches"], 6)  # 3 searches * 2 countries each
        self.assertEqual(stats["successful_searches"], 6)
        self.assertEqual(stats["failed_searches"], 0)
        self.assertEqual(stats["success_rate"], 1.0)
        self.assertGreater(stats["total_search_time"], 0)
        self.assertEqual(stats["max_workers"], 2)

    def test_reset_stats(self) -> None:
        """Test resetting performance statistics."""
        mock_search_func = Mock()
        mock_progress_callback = Mock()

        mock_search_func.return_value = {"success": True, "jobs": self.sample_jobs, "count": 2, "search_time": 1.0}

        # Run a search to accumulate stats
        self.threading_manager.search_countries_parallel(
            countries=["Country1"],
            search_func=mock_search_func,
            search_term="Software Engineer",
            progress_callback=mock_progress_callback,
        )

        # Verify stats were accumulated
        stats_before = self.threading_manager.get_performance_stats()
        self.assertGreater(stats_before["total_searches"], 0)

        # Reset stats
        self.threading_manager.reset_stats()

        # Verify stats were reset
        stats_after = self.threading_manager.get_performance_stats()
        self.assertEqual(stats_after["total_searches"], 0)
        self.assertEqual(stats_after["successful_searches"], 0)
        self.assertEqual(stats_after["failed_searches"], 0)
        self.assertEqual(stats_after["total_search_time"], 0.0)

    def test_search_task_dataclass(self) -> None:
        """Test SearchTask dataclass functionality."""
        task = SearchTask(
            country="Test Country",
            search_term="Software Engineer",
            include_remote=True,
            time_filter="24h",
            task_id="test_123",
        )

        self.assertEqual(task.country, "Test Country")
        self.assertEqual(task.search_term, "Software Engineer")
        self.assertTrue(task.include_remote)
        self.assertEqual(task.time_filter, "24h")
        self.assertEqual(task.task_id, "test_123")

    def test_search_result_dataclass(self) -> None:
        """Test SearchResult dataclass functionality."""
        result = SearchResult(
            country="Test Country",
            success=True,
            jobs=self.sample_jobs,
            search_time=1.5,
            jobs_count=2,
            task_id="test_123",
        )

        self.assertEqual(result.country, "Test Country")
        self.assertTrue(result.success)
        self.assertIsNotNone(result.jobs)
        assert result.jobs is not None  # Type guard for linter
        self.assertEqual(len(result.jobs), 2)
        self.assertEqual(result.search_time, 1.5)
        self.assertEqual(result.jobs_count, 2)
        self.assertEqual(result.task_id, "test_123")


if __name__ == "__main__":
    unittest.main()
