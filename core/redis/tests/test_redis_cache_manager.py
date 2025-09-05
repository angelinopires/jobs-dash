"""
Unit tests for RedisCacheManager.

This module contains comprehensive tests for the Redis-only caching functionality
including cache hits, misses, TTL expiration, and Redis failure scenarios.
"""

import unittest
from unittest.mock import Mock, patch

from core.redis.redis_cache_manager import RedisCacheManager


class TestRedisCacheManager(unittest.TestCase):
    """Test cases for RedisCacheManager functionality."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        # Use a short TTL for testing
        self.cache_manager = RedisCacheManager(cache_ttl_seconds=2)

        # Sample job data for testing
        self.sample_jobs = [
            {
                "title": "Software Engineer",
                "company": "Tech Corp",
                "location": "San Francisco, CA",
                "salary": "$100k - $150k",
                "description": "Great software engineering role",
                "job_url": "https://example.com/job1",
            },
            {
                "title": "Data Scientist",
                "company": "Data Inc",
                "location": "Remote",
                "salary": "$120k - $180k",
                "description": "Exciting data science position",
                "job_url": "https://example.com/job2",
            },
        ]

        # Test parameters
        self.test_scraper = "indeed"
        self.test_search_term = "python developer"
        self.test_country = "usa"

    def tearDown(self) -> None:
        """Clean up after each test method."""
        # Reset cache stats for clean test isolation
        if hasattr(self.cache_manager, "reset_stats"):
            self.cache_manager.reset_stats()

    def test_cache_manager_initialization(self) -> None:
        """Test RedisCacheManager initializes correctly."""
        # Test default TTL initialization
        default_cache = RedisCacheManager()
        self.assertIsInstance(default_cache.cache_ttl_seconds, int)
        self.assertGreater(default_cache.cache_ttl_seconds, 0)

        # Test custom TTL initialization
        custom_cache = RedisCacheManager(cache_ttl_seconds=300)
        self.assertEqual(custom_cache.cache_ttl_seconds, 300)

        # Test that Redis manager and key generator are initialized
        self.assertIsNotNone(default_cache.redis_manager)
        self.assertIsNotNone(default_cache.key_generator)

    def test_cache_miss_scenario(self) -> None:
        """Test cache miss returns None."""
        # Test with parameters that shouldn't be cached
        result = self.cache_manager.get_cached_result(
            scraper="nonexistent_scraper", search_term="nonexistent_term", country="nonexistent_country"
        )

        self.assertIsNone(result)

        # Verify stats show cache miss
        stats = self.cache_manager.get_cache_stats()
        self.assertGreater(stats["misses"], 0)
        self.assertEqual(stats["hits"], 0)

    def test_cache_hit_scenario(self) -> None:
        """Test cache hit scenario with actual Redis."""
        # First, cache some data
        cache_success = self.cache_manager.cache_result(
            scraper=self.test_scraper,
            search_term=self.test_search_term,
            country=self.test_country,
            result=self.sample_jobs,
        )

        # Only test cache hit if caching was successful (Redis available)
        if cache_success:
            # Then try to retrieve it
            cached_result = self.cache_manager.get_cached_result(
                scraper=self.test_scraper, search_term=self.test_search_term, country=self.test_country
            )

            self.assertIsNotNone(cached_result)
            assert cached_result is not None
            self.assertEqual(len(cached_result), 2)
            self.assertEqual(cached_result[0]["title"], "Software Engineer")
            self.assertEqual(cached_result[1]["title"], "Data Scientist")

            # Verify stats show cache hit
            stats = self.cache_manager.get_cache_stats()
            self.assertGreater(stats["hits"], 0)
        else:
            self.skipTest("Redis not available for cache hit testing")

    def test_empty_result_not_cached(self) -> None:
        """Test that empty results are not cached."""
        # Try to cache empty result
        cache_success = self.cache_manager.cache_result(
            scraper=self.test_scraper, search_term="empty_search", country=self.test_country, result=[]
        )

        # Should return False for empty results
        self.assertFalse(cache_success)

    def test_cache_with_additional_parameters(self) -> None:
        """Test caching with additional search parameters."""
        # Cache with extra parameters
        extra_params = {"remote": True, "posting_age": "Past Week", "salary_min": 100000}

        cache_success = self.cache_manager.cache_result(
            scraper=self.test_scraper,
            search_term=self.test_search_term,
            country=self.test_country,
            result=self.sample_jobs,
            **extra_params,
        )

        if cache_success:
            # Retrieve with same parameters
            cached_result = self.cache_manager.get_cached_result(
                scraper=self.test_scraper, search_term=self.test_search_term, country=self.test_country, **extra_params
            )

            self.assertIsNotNone(cached_result)
            assert cached_result is not None
            self.assertEqual(len(cached_result), 2)

            # Retrieve with different parameters should miss
            different_result = self.cache_manager.get_cached_result(
                scraper=self.test_scraper,
                search_term=self.test_search_term,
                country=self.test_country,
                remote=False,  # Different parameter
                **{k: v for k, v in extra_params.items() if k != "remote"},
            )

            # Should be None because parameters are different
            self.assertIsNone(different_result)
        else:
            self.skipTest("Redis not available for parameter testing")

    def test_health_check(self) -> None:
        """Test cache health check functionality."""
        health_status = self.cache_manager.health_check()
        self.assertIsInstance(health_status, bool)

        # Force health check
        force_health = self.cache_manager.force_health_check()
        self.assertIsInstance(force_health, bool)

    def test_cache_stats(self) -> None:
        """Test cache statistics functionality."""
        # Reset stats first
        self.cache_manager.reset_stats()

        stats = self.cache_manager.get_cache_stats()

        # Check required stats fields
        required_fields = [
            "cache_type",
            "enabled",
            "ttl_seconds",
            "hits",
            "misses",
            "errors",
            "total_requests",
            "hit_rate_percent",
            "redis_healthy",
        ]

        for field in required_fields:
            self.assertIn(field, stats)

        # Check stats data types
        self.assertEqual(stats["cache_type"], "redis_only")
        self.assertIsInstance(stats["enabled"], bool)
        self.assertIsInstance(stats["ttl_seconds"], int)
        self.assertIsInstance(stats["hits"], int)
        self.assertIsInstance(stats["misses"], int)
        self.assertIsInstance(stats["errors"], int)
        self.assertIsInstance(stats["total_requests"], int)
        self.assertIsInstance(stats["hit_rate_percent"], (int, float))
        self.assertIsInstance(stats["redis_healthy"], bool)

    def test_clear_scraper_cache(self) -> None:
        """Test clearing scraper cache (Redis-only limitation)."""
        # This should return -1 indicating Redis-only mode limitation
        result = self.cache_manager.clear_scraper_cache("test_scraper")
        self.assertEqual(result, -1)

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_redis_unavailable_fallback(self, mock_redis_manager_class: Mock) -> None:
        """Test graceful fallback when Redis is unavailable."""
        # Mock Redis manager to simulate Redis being down
        mock_redis_manager = Mock()
        mock_redis_manager.is_healthy.return_value = False
        mock_redis_manager_class.return_value = mock_redis_manager

        # Create cache manager with mocked Redis
        cache_manager = RedisCacheManager(cache_ttl_seconds=60)

        # Cache operations should handle Redis being down gracefully
        cache_result = cache_manager.cache_result(
            scraper="test", search_term="test", country="test", result=self.sample_jobs
        )
        self.assertFalse(cache_result)  # Should return False when Redis down

        get_result = cache_manager.get_cached_result(scraper="test", search_term="test", country="test")
        self.assertIsNone(get_result)  # Should return None when Redis down

        # Health check should return False
        health = cache_manager.health_check()
        self.assertFalse(health)

    @patch("core.redis.redis_cache_manager.RedisManager")
    def test_redis_error_handling(self, mock_redis_manager_class: Mock) -> None:
        """Test error handling during Redis operations."""
        # Mock Redis manager to raise exceptions
        mock_redis_manager = Mock()
        mock_redis_manager.is_healthy.return_value = True
        mock_redis_manager.get_json.side_effect = Exception("Redis connection error")
        mock_redis_manager.set_json.side_effect = Exception("Redis write error")
        mock_redis_manager_class.return_value = mock_redis_manager

        cache_manager = RedisCacheManager(cache_ttl_seconds=60)

        # Get operation should handle errors gracefully
        result = cache_manager.get_cached_result(scraper="test", search_term="test", country="test")
        self.assertIsNone(result)

        # Cache operation should handle errors gracefully
        cache_success = cache_manager.cache_result(
            scraper="test", search_term="test", country="test", result=self.sample_jobs
        )
        self.assertFalse(cache_success)

        # Stats should show errors
        stats = cache_manager.get_cache_stats()
        self.assertGreater(stats["errors"], 0)

    def test_ttl_expiration_simulation(self) -> None:
        """Test TTL expiration behavior (simulated)."""
        # This test verifies that TTL is set correctly
        # Note: Actual expiration testing would require waiting for TTL

        # Cache data with short TTL
        short_ttl_cache = RedisCacheManager(cache_ttl_seconds=1)

        if short_ttl_cache.redis_manager.is_healthy():
            # Cache some data
            cache_success = short_ttl_cache.cache_result(
                scraper="ttl_test", search_term="ttl_search", country="ttl_country", result=self.sample_jobs
            )

            if cache_success:
                # Verify immediate retrieval works
                immediate_result = short_ttl_cache.get_cached_result(
                    scraper="ttl_test", search_term="ttl_search", country="ttl_country"
                )
                self.assertIsNotNone(immediate_result)

                # Verify TTL is set correctly in stats
                stats = short_ttl_cache.get_cache_stats()
                self.assertEqual(stats["ttl_seconds"], 1)
            else:
                self.skipTest("Redis not available for TTL testing")
        else:
            self.skipTest("Redis not healthy for TTL testing")

    def test_data_type_validation(self) -> None:
        """Test that cached data maintains proper types."""
        if self.cache_manager.redis_manager.is_healthy():
            # Cache data
            cache_success = self.cache_manager.cache_result(
                scraper=self.test_scraper,
                search_term=self.test_search_term,
                country=self.test_country,
                result=self.sample_jobs,
            )

            if cache_success:
                # Retrieve and verify data types
                cached_result = self.cache_manager.get_cached_result(
                    scraper=self.test_scraper, search_term=self.test_search_term, country=self.test_country
                )

                self.assertIsInstance(cached_result, list)
                assert cached_result is not None
                for job in cached_result:
                    self.assertIsInstance(job, dict)
                    self.assertIn("title", job)
                    self.assertIn("company", job)
            else:
                self.skipTest("Redis not available for data type testing")
        else:
            self.skipTest("Redis not healthy for data type testing")


class TestRedisCacheManagerIntegration(unittest.TestCase):
    """Integration tests for RedisCacheManager with real Redis operations."""

    def setUp(self) -> None:
        """Set up integration test fixtures."""
        self.cache_manager = RedisCacheManager(cache_ttl_seconds=5)

    def test_real_redis_integration(self) -> None:
        """Test actual Redis integration if Redis is available."""
        if not self.cache_manager.redis_manager.is_healthy():
            self.skipTest("Redis not available for integration testing")

        # Test cache miss
        result = self.cache_manager.get_cached_result(
            scraper="integration_test", search_term="integration_search", country="integration_country"
        )
        self.assertIsNone(result)

        # Test cache storage
        test_jobs = [{"title": "Integration Test Job", "company": "Test Corp"}]
        cache_success = self.cache_manager.cache_result(
            scraper="integration_test",
            search_term="integration_search",
            country="integration_country",
            result=test_jobs,
        )
        self.assertTrue(cache_success)

        # Test cache hit
        cached_result = self.cache_manager.get_cached_result(
            scraper="integration_test", search_term="integration_search", country="integration_country"
        )
        self.assertIsNotNone(cached_result)
        assert cached_result is not None
        self.assertEqual(len(cached_result), 1)
        self.assertEqual(cached_result[0]["title"], "Integration Test Job")


if __name__ == "__main__":
    unittest.main()
