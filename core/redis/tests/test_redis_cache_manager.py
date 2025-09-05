"""
Unit tests for RedisCacheManager with proper mocking.

This module contains isolated unit tests that don't depend on external services.
All Redis dependencies are mocked to ensure tests run reliably in any environment.
"""

import unittest
from unittest.mock import Mock, patch

from core.redis.redis_cache_manager import RedisCacheManager


class TestRedisCacheManagerUnit(unittest.TestCase):
    """Unit tests for RedisCacheManager with mocked Redis."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        # Mock Redis manager to avoid external dependencies
        self.mock_redis_manager = Mock()
        self.mock_redis_manager.is_healthy.return_value = True
        self.mock_redis_manager.get_json.return_value = None  # Default to cache miss
        self.mock_redis_manager.set_json.return_value = True
        self.mock_redis_manager.get_connection_info.return_value = {"host": "localhost", "port": 6379}

        # Sample job data for testing
        self.sample_jobs = [
            {
                "title": "Software Engineer",
                "company": "Tech Corp",
                "location": "San Francisco, CA",
                "salary": "$100k - $150k",
                "description": "Great software engineering role",
                "job_url": "https://example.com/job1",
            }
        ]

        # Test parameters
        self.test_scraper = "indeed"
        self.test_search_term = "python developer"
        self.test_country = "usa"

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_cache_miss_scenario(self, mock_redis_manager_class: Mock) -> None:
        """Test cache miss returns None and increments miss counter."""
        # Configure mock
        mock_redis_manager_class.return_value = self.mock_redis_manager
        self.mock_redis_manager.get_json.return_value = None

        # Create cache manager
        cache_manager = RedisCacheManager(cache_ttl_seconds=2)
        cache_manager.reset_stats()

        # Test cache miss
        result = cache_manager.get_cached_result(
            scraper=self.test_scraper, search_term=self.test_search_term, country=self.test_country
        )

        self.assertIsNone(result)

        # Verify stats
        stats = cache_manager.get_cache_stats()
        self.assertGreater(stats["misses"], 0)
        self.assertEqual(stats["hits"], 0)

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_cache_hit_scenario(self, mock_redis_manager_class: Mock) -> None:
        """Test cache hit returns cached data and increments hit counter."""
        # Configure mock to return cached data
        mock_redis_manager_class.return_value = self.mock_redis_manager
        self.mock_redis_manager.get_json.return_value = self.sample_jobs

        # Create cache manager
        cache_manager = RedisCacheManager(cache_ttl_seconds=2)
        cache_manager.reset_stats()

        # Test cache hit
        result = cache_manager.get_cached_result(
            scraper=self.test_scraper, search_term=self.test_search_term, country=self.test_country
        )

        self.assertEqual(result, self.sample_jobs)

        # Verify stats
        stats = cache_manager.get_cache_stats()
        self.assertEqual(stats["misses"], 0)
        self.assertGreater(stats["hits"], 0)

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_cache_result_success(self, mock_redis_manager_class: Mock) -> None:
        """Test caching result successfully."""
        # Configure mock
        mock_redis_manager_class.return_value = self.mock_redis_manager
        self.mock_redis_manager.set_json.return_value = True

        # Create cache manager
        cache_manager = RedisCacheManager(cache_ttl_seconds=2)

        # Test caching (note: parameter order is scraper, search_term, country, result)
        success = cache_manager.cache_result(
            scraper=self.test_scraper,
            search_term=self.test_search_term,
            country=self.test_country,
            result=self.sample_jobs,
        )

        self.assertTrue(success)
        # Verify Redis was called
        self.mock_redis_manager.set_json.assert_called_once()

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_redis_unhealthy_fallback(self, mock_redis_manager_class: Mock) -> None:
        """Test graceful fallback when Redis is unhealthy."""
        # Configure mock to simulate unhealthy Redis
        mock_redis_manager_class.return_value = self.mock_redis_manager
        self.mock_redis_manager.is_healthy.return_value = False

        # Create cache manager
        cache_manager = RedisCacheManager(cache_ttl_seconds=2)
        cache_manager.reset_stats()

        # Test cache miss with unhealthy Redis
        result = cache_manager.get_cached_result(
            scraper=self.test_scraper, search_term=self.test_search_term, country=self.test_country
        )

        self.assertIsNone(result)

        # Verify error counter incremented
        stats = cache_manager.get_cache_stats()
        self.assertGreater(stats["errors"], 0)

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_clear_scraper_cache_limitation(self, mock_redis_manager_class: Mock) -> None:
        """Test clear scraper cache returns -1 (limitation indicator)."""
        # Configure mock
        mock_redis_manager_class.return_value = self.mock_redis_manager

        # Create cache manager
        cache_manager = RedisCacheManager(cache_ttl_seconds=2)

        # Test clear cache (should return -1 for Redis-only limitation)
        result = cache_manager.clear_scraper_cache("test_scraper")
        self.assertEqual(result, -1)

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_cache_stats_structure(self, mock_redis_manager_class: Mock) -> None:
        """Test cache stats returns expected structure."""
        # Configure mock
        mock_redis_manager_class.return_value = self.mock_redis_manager

        # Create cache manager
        cache_manager = RedisCacheManager(cache_ttl_seconds=2)

        # Get stats
        stats = cache_manager.get_cache_stats()

        # Verify structure
        expected_keys = {
            "cache_type",
            "enabled",
            "ttl_seconds",
            "hits",
            "misses",
            "errors",
            "total_requests",
            "hit_rate_percent",
            "redis_healthy",
            "redis_connection",
        }
        self.assertEqual(set(stats.keys()), expected_keys)
        self.assertIsInstance(stats["hit_rate_percent"], (int, float))


if __name__ == "__main__":
    unittest.main()
