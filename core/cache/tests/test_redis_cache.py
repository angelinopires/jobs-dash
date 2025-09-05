"""
Unit tests for Redis cache components.

This module contains comprehensive tests for Redis manager and global cache manager.
"""

import json
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from redis.exceptions import ConnectionError, TimeoutError

from config.environment import RedisConfig

from ...redis.redis_manager import RedisManager
from ..global_cache_manager import GlobalCacheManager


class TestRedisManager(unittest.TestCase):
    """Test cases for RedisManager class."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.redis_config = RedisConfig(
            url="redis://localhost:6379",
            ttl=3600,
            max_connections=5,
            retry_attempts=2,
            retry_delay=0.1,
            health_check_interval=10,
        )

        # Mock Redis client
        self.mock_redis_client = MagicMock()

    def tearDown(self) -> None:
        """Clean up after each test method."""
        pass

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_initialization(self, mock_redis: MagicMock) -> None:
        """Test Redis manager initialization with valid configuration."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True

        # Act
        redis_manager = RedisManager(redis_url="redis://localhost:6379", max_connections=5)

        # Assert
        self.assertIsNotNone(redis_manager)
        self.assertEqual(redis_manager.redis_url, "redis://localhost:6379")
        self.assertEqual(redis_manager.max_connections, 5)
        mock_redis.assert_called_once()

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_connection_failure(self, mock_redis: MagicMock) -> None:
        """Test Redis manager initialization with connection failure."""
        # Arrange
        mock_redis.side_effect = ConnectionError("Connection failed")

        # Act
        redis_manager = RedisManager()

        # Assert
        self.assertIsNotNone(redis_manager)
        self.assertFalse(redis_manager.is_healthy())

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_health_check(self, mock_redis: MagicMock) -> None:
        """Test Redis health check functionality."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True

        redis_manager = RedisManager()

        # Act
        is_healthy = redis_manager.is_healthy()

        # Assert
        self.assertTrue(is_healthy)
        self.mock_redis_client.ping.assert_called()

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_set_json_success(self, mock_redis: MagicMock) -> None:
        """Test successful JSON data storage in Redis."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True
        self.mock_redis_client.setex.return_value = True

        redis_manager = RedisManager()
        test_data = {"key": "value", "number": 42}

        # Act
        result = redis_manager.set_json("test_key", test_data, ttl=60)

        # Assert
        self.assertTrue(result)
        self.mock_redis_client.setex.assert_called_once()

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_get_json_success(self, mock_redis: MagicMock) -> None:
        """Test successful JSON data retrieval from Redis."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True

        test_data = {"key": "value", "number": 42}
        json_data = json.dumps(test_data)
        self.mock_redis_client.get.return_value = json_data

        redis_manager = RedisManager()

        # Act
        result = redis_manager.get_json("test_key")

        # Assert
        self.assertEqual(result, test_data)
        self.mock_redis_client.get.assert_called_once_with("test_key")

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_get_json_not_found(self, mock_redis: MagicMock) -> None:
        """Test JSON data retrieval when key doesn't exist."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True
        self.mock_redis_client.get.return_value = None

        redis_manager = RedisManager()

        # Act
        result = redis_manager.get_json("nonexistent_key")

        # Assert
        self.assertIsNone(result)

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_delete_success(self, mock_redis: MagicMock) -> None:
        """Test successful key deletion from Redis."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True
        self.mock_redis_client.delete.return_value = 1

        redis_manager = RedisManager()

        # Act
        result = redis_manager.delete("test_key")

        # Assert
        self.assertTrue(result)
        self.mock_redis_client.delete.assert_called_once_with("test_key")

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_exists_success(self, mock_redis: MagicMock) -> None:
        """Test successful existence check in Redis."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True
        self.mock_redis_client.exists.return_value = 1

        redis_manager = RedisManager()

        # Act
        result = redis_manager.exists("test_key")

        # Assert
        self.assertTrue(result)
        self.mock_redis_client.exists.assert_called_once_with("test_key")

    @patch("core.redis_manager.redis.Redis")
    def test_redis_manager_retry_logic(self, mock_redis: MagicMock) -> None:
        """Test retry logic for failed operations."""
        # Arrange
        mock_redis.return_value = self.mock_redis_client
        self.mock_redis_client.ping.return_value = True
        self.mock_redis_client.get.side_effect = [TimeoutError("Timeout"), '{"key": "value"}']

        redis_manager = RedisManager(retry_attempts=2, retry_delay=0.01)

        # Act
        result = redis_manager.get_json("test_key")

        # Assert
        self.assertEqual(result, {"key": "value"})
        self.assertEqual(self.mock_redis_client.get.call_count, 2)


class TestGlobalCacheManager(unittest.TestCase):
    """Test cases for GlobalCacheManager class."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.test_jobs = [
            {"title": "Software Engineer", "company": "Tech Corp", "location": "San Francisco"},
            {"title": "Senior Developer", "company": "Startup Inc", "location": "Remote"},
        ]

        self.test_metadata = {"source": "test", "timestamp": datetime.now().isoformat()}

    def tearDown(self) -> None:
        """Clean up after each test method."""
        pass

    @patch("core.global_cache_manager.get_redis_config")
    @patch("core.global_cache_manager.RedisManager")
    def test_cache_manager_initialization(self, mock_redis_manager: MagicMock, mock_get_config: MagicMock) -> None:
        """Test global cache manager initialization."""
        # Arrange
        mock_config = MagicMock()
        mock_config.url = "redis://localhost:6379"
        mock_config.max_connections = 10
        mock_config.retry_attempts = 3
        mock_config.retry_delay = 1.0
        mock_config.health_check_interval = 30
        mock_get_config.return_value = mock_config

        mock_redis_instance = MagicMock()
        mock_redis_manager.return_value = mock_redis_instance

        # Act
        cache_manager = GlobalCacheManager()

        # Assert
        self.assertIsNotNone(cache_manager)
        mock_redis_manager.assert_called_once()

    def test_generate_cache_key_consistent(self) -> None:
        """Test that cache key generation is consistent for same parameters."""
        # Arrange
        with patch("core.global_cache_manager.get_redis_config"):
            with patch("core.global_cache_manager.RedisManager"):
                cache_manager = GlobalCacheManager()

                # Act
                key1 = cache_manager.generate_cache_key(
                    scraper_name="indeed", job_title="Software Engineer", location="San Francisco", remote=True
                )

                key2 = cache_manager.generate_cache_key(
                    scraper_name="indeed", job_title="Software Engineer", location="San Francisco", remote=True
                )

                # Assert
                self.assertEqual(key1, key2)

    def test_generate_cache_key_different_parameters(self) -> None:
        """Test that cache key generation produces different keys for different parameters."""
        # Arrange
        with patch("core.global_cache_manager.get_redis_config"):
            with patch("core.global_cache_manager.RedisManager"):
                cache_manager = GlobalCacheManager()

                # Act
                key1 = cache_manager.generate_cache_key(
                    scraper_name="indeed", job_title="Software Engineer", location="San Francisco", remote=False
                )

                key2 = cache_manager.generate_cache_key(
                    scraper_name="indeed", job_title="Software Engineer", location="San Francisco", remote=True
                )

                # Assert
                self.assertNotEqual(key1, key2)

    @patch("core.global_cache_manager.get_redis_config")
    @patch("core.global_cache_manager.RedisManager")
    def test_cache_set_and_get_success(self, mock_redis_manager: MagicMock, mock_get_config: MagicMock) -> None:
        """Test successful cache set and get operations."""
        # Arrange
        mock_config = MagicMock()
        mock_config.ttl = 3600
        mock_get_config.return_value = mock_config

        mock_redis_instance = MagicMock()
        mock_redis_instance.is_healthy.return_value = True
        mock_redis_instance.set_json.return_value = True
        mock_redis_instance.get_json.return_value = {
            "jobs": self.test_jobs,
            "metadata": self.test_metadata,
            "access_count": 0,
            "last_accessed": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat(),
            "ttl": 3600,
        }
        mock_redis_manager.return_value = mock_redis_instance

        cache_manager = GlobalCacheManager()
        cache_key = cache_manager.generate_cache_key(
            scraper_name="indeed", job_title="Software Engineer", location="San Francisco"
        )

        # Act
        set_result = cache_manager.set(cache_key, self.test_jobs, self.test_metadata)
        get_result = cache_manager.get(cache_key)

        # Assert
        self.assertTrue(set_result)
        self.assertIsNotNone(get_result)
        self.assertEqual(len(get_result["jobs"]), 2)  # type: ignore[index]
        self.assertEqual(get_result["metadata"]["source"], "test")  # type: ignore[index]

    @patch("core.global_cache_manager.get_redis_config")
    @patch("core.global_cache_manager.RedisManager")
    def test_cache_get_miss(self, mock_redis_manager: MagicMock, mock_get_config: MagicMock) -> None:
        """Test cache get when key doesn't exist."""
        # Arrange
        mock_config = MagicMock()
        mock_get_config.return_value = mock_config

        mock_redis_instance = MagicMock()
        mock_redis_instance.is_healthy.return_value = True
        mock_redis_instance.get_json.return_value = None
        mock_redis_manager.return_value = mock_redis_instance

        cache_manager = GlobalCacheManager()

        # Act
        result = cache_manager.get("nonexistent_key")

        # Assert
        self.assertIsNone(result)

    @patch("core.global_cache_manager.get_redis_config")
    @patch("core.global_cache_manager.RedisManager")
    def test_cache_redis_fallback(self, mock_redis_manager: MagicMock, mock_get_config: MagicMock) -> None:
        """Test fallback to file cache when Redis is unhealthy."""
        # Arrange
        mock_config = MagicMock()
        mock_get_config.return_value = mock_config

        mock_redis_instance = MagicMock()
        mock_redis_instance.is_healthy.return_value = False
        mock_redis_instance.force_health_check.return_value = False  # Redis still down
        mock_redis_manager.return_value = mock_redis_instance

        cache_manager = GlobalCacheManager()

        # Act
        result = cache_manager.get("test_key")

        # Assert
        self.assertIsNone(result)  # File cache returns None in our mock implementation

    @patch("core.global_cache_manager.get_redis_config")
    @patch("core.global_cache_manager.RedisManager")
    def test_cache_statistics(self, mock_redis_manager: MagicMock, mock_get_config: MagicMock) -> None:
        """Test cache statistics tracking."""
        # Arrange
        mock_config = MagicMock()
        mock_get_config.return_value = mock_config

        mock_redis_instance = MagicMock()
        mock_redis_instance.is_healthy.return_value = True
        mock_redis_instance.get_json.return_value = None
        mock_redis_instance.get_connection_info.return_value = {
            "url": "redis://localhost:6379",
            "healthy": True,
            "connection_attempts": 0,
            "last_health_check": 1234567890,
            "max_connections": 10,
        }
        mock_redis_manager.return_value = mock_redis_instance

        cache_manager = GlobalCacheManager()

        # Act - Simulate cache miss
        cache_manager.get("test_key")

        # Get statistics
        stats = cache_manager.get_cache_stats()

        # Assert
        self.assertEqual(stats["cache_misses"], 1)
        self.assertEqual(stats["cache_hits"], 0)
        self.assertEqual(stats["total_requests"], 1)
        self.assertEqual(stats["hit_rate_percent"], 0.0)
        self.assertTrue(stats["redis_healthy"])

    @patch("core.global_cache_manager.get_redis_config")
    @patch("core.global_cache_manager.RedisManager")
    def test_cache_delete_success(self, mock_redis_manager: MagicMock, mock_get_config: MagicMock) -> None:
        """Test successful cache deletion."""
        # Arrange
        mock_config = MagicMock()
        mock_get_config.return_value = mock_config

        mock_redis_instance = MagicMock()
        mock_redis_instance.is_healthy.return_value = True
        mock_redis_instance.delete.return_value = True
        mock_redis_manager.return_value = mock_redis_instance

        cache_manager = GlobalCacheManager()

        # Act
        result = cache_manager.delete("test_key")

        # Assert
        self.assertTrue(result)
        mock_redis_instance.delete.assert_called_once_with("test_key")

    @patch("core.global_cache_manager.get_redis_config")
    @patch("core.global_cache_manager.RedisManager")
    def test_cache_exists_success(self, mock_redis_manager: MagicMock, mock_get_config: MagicMock) -> None:
        """Test successful cache existence check."""
        # Arrange
        mock_config = MagicMock()
        mock_get_config.return_value = mock_config

        mock_redis_instance = MagicMock()
        mock_redis_instance.is_healthy.return_value = True
        mock_redis_instance.exists.return_value = True
        mock_redis_manager.return_value = mock_redis_instance

        cache_manager = GlobalCacheManager()

        # Act
        result = cache_manager.exists("test_key")

        # Assert
        self.assertTrue(result)
        mock_redis_instance.exists.assert_called_once_with("test_key")


if __name__ == "__main__":
    unittest.main()
