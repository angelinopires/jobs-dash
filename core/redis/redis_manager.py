"""
Redis Connection Manager

This module provides Redis connection management with connection pooling,
retry logic, health checks, and JSON serialization for job data.
It includes fallback to file cache when Redis is unavailable.

Think of this like a database connection pool in Node.js, but for Redis.
"""

import json
import logging
import time
from typing import Any, Dict, Optional
from urllib.parse import urlparse

import redis
from redis.exceptions import AuthenticationError, ConnectionError, RedisError, TimeoutError

from settings.environment import get_redis_config

logger = logging.getLogger(__name__)


class RedisManager:
    """
    Redis Connection Manager with Fallback Support

    This class manages Redis connections with automatic retry logic,
    health checks, and graceful fallback to file cache when Redis is unavailable.
    Similar to a connection pool in Node.js/Express applications.
    """

    def __init__(
        self,
        redis_url: Optional[str] = None,
        max_connections: Optional[int] = None,
        retry_attempts: Optional[int] = None,
        retry_delay: Optional[float] = None,
        health_check_interval: Optional[int] = None,
    ) -> None:
        """
        Initialize Redis manager with connection settings

        Args:
            redis_url: Redis connection URL (uses environment config if None)
            max_connections: Maximum number of connections in pool (uses environment config if None)
            retry_attempts: Number of retry attempts for failed operations (uses environment config if None)
            retry_delay: Delay between retry attempts in seconds (uses environment config if None)
            health_check_interval: Health check interval in seconds (uses environment config if None)
        """
        # Get configuration from environment with optional overrides
        redis_config = get_redis_config()
        self.redis_url = redis_url or redis_config.url
        self.max_connections = max_connections or redis_config.max_connections
        self.retry_attempts = retry_attempts or redis_config.retry_attempts
        self.retry_delay = retry_delay or redis_config.retry_delay
        self.health_check_interval = health_check_interval or redis_config.health_check_interval

        # Connection state
        self._redis_client: Optional[redis.Redis] = None
        self._last_health_check = 0
        self._is_healthy = False
        self._connection_attempts = 0

        # Initialize connection
        self._initialize_connection()

    def _initialize_connection(self) -> None:
        """Initialize Redis connection with connection pooling"""
        try:
            # Parse Redis URL to extract connection parameters
            parsed_url = urlparse(self.redis_url)

            # Create Redis connection pool
            self._redis_client = redis.Redis(
                host=parsed_url.hostname or "localhost",
                port=parsed_url.port or 6379,
                password=parsed_url.password,
                db=int(parsed_url.path[1:]) if parsed_url.path and len(parsed_url.path) > 1 else 0,
                max_connections=self.max_connections,
                retry_on_timeout=True,
                socket_connect_timeout=5,
                socket_timeout=5,
                decode_responses=True,  # Return strings instead of bytes
            )

            # Test connection
            self._test_connection()
            logger.info(f"Redis connection established: {self.redis_url}")

        except Exception as e:
            logger.warning(f"Failed to initialize Redis connection: {e}")
            self._redis_client = None
            self._is_healthy = False

    def _test_connection(self) -> bool:
        """Test Redis connection with timeout"""
        if not self._redis_client:
            return False

        try:
            # Simple ping test
            response = self._redis_client.ping()
            self._is_healthy = bool(response)
            self._last_health_check = int(time.time())
            return bool(response)
        except Exception as e:
            logger.debug(f"Redis connection test failed: {e}")
            self._is_healthy = False
            return False

    def is_healthy(self) -> bool:
        """
        Check if Redis connection is healthy

        Returns:
            bool: True if Redis is available and responding
        """
        current_time = time.time()

        # Check if we need to perform a health check
        if current_time - self._last_health_check > self.health_check_interval:
            self._test_connection()
        elif self._connection_attempts > 0:
            # If we've had recent connection attempts, check more frequently
            if current_time - self._last_health_check > 5:  # Check every 5 seconds after failures
                self._test_connection()

        return self._is_healthy

    def force_health_check(self) -> bool:
        """
        Force an immediate health check

        Returns:
            bool: True if Redis is available and responding
        """
        return self._test_connection()

    def _execute_with_retry(self, operation: str, *args: Any, **kwargs: Any) -> Any:
        """
        Execute Redis operation with retry logic

        Args:
            operation: Redis operation name (get, set, delete, etc.)
            *args: Arguments for the Redis operation
            **kwargs: Keyword arguments for the Redis operation

        Returns:
            Any: Result of the Redis operation

        Raises:
            RedisError: If all retry attempts fail
        """
        if not self._redis_client:
            raise RedisError("Redis client not initialized")

        last_exception = None

        for attempt in range(self.retry_attempts):
            try:
                # Get the method from redis client
                method = getattr(self._redis_client, operation)
                result = method(*args, **kwargs)

                # Reset connection attempts on success
                self._connection_attempts = 0
                return result

            except (ConnectionError, TimeoutError) as e:
                last_exception = e
                self._connection_attempts += 1
                logger.warning(f"Redis {operation} failed (attempt {attempt + 1}/{self.retry_attempts}): {e}")

                # Try to reconnect if connection is lost
                if attempt < self.retry_attempts - 1:
                    time.sleep(self.retry_delay * (2**attempt))  # Exponential backoff
                    self._initialize_connection()

            except AuthenticationError as e:
                logger.error(f"Redis authentication failed: {e}")
                raise e

            except RedisError as e:
                logger.error(f"Redis {operation} failed: {e}")
                raise e

        # All retry attempts failed
        logger.error(f"Redis {operation} failed after {self.retry_attempts} attempts")
        raise last_exception or RedisError(f"Redis {operation} failed")

    def set_json(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Store JSON-serializable data in Redis

        Args:
            key: Redis key
            value: Data to store (must be JSON serializable)
            ttl: Time to live in seconds (optional)

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Serialize data to JSON
            json_data = json.dumps(value, default=str)

            # Store in Redis
            if ttl:
                result = self._execute_with_retry("setex", key, ttl, json_data)
            else:
                result = self._execute_with_retry("set", key, json_data)

            return bool(result)
        except Exception as e:
            logger.error(f"Failed to set JSON data for key '{key}': {e}")
            return False

    def get_json(self, key: str) -> Optional[Any]:
        """
        Retrieve and deserialize JSON data from Redis

        Args:
            key: Redis key

        Returns:
            Any: Deserialized data or None if not found/failed
        """
        try:
            # Get data from Redis
            json_data = self._execute_with_retry("get", key)

            if json_data is None:
                return None

            # Deserialize JSON data
            return json.loads(json_data)

        except Exception as e:
            logger.error(f"Failed to get JSON data for key '{key}': {e}")
            return None

    def delete(self, key: str) -> bool:
        """
        Delete a key from Redis

        Args:
            key: Redis key to delete

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self._execute_with_retry("delete", key)
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to delete key '{key}': {e}")
            return False

    def exists(self, key: str) -> bool:
        """
        Check if a key exists in Redis

        Args:
            key: Redis key to check

        Returns:
            bool: True if key exists, False otherwise
        """
        try:
            result = self._execute_with_retry("exists", key)
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to check existence of key '{key}': {e}")
            return False

    def expire(self, key: str, ttl: int) -> bool:
        """
        Set expiration time for a key

        Args:
            key: Redis key
            ttl: Time to live in seconds

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            result = self._execute_with_retry("expire", key, ttl)
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to set expiration for key '{key}': {e}")
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get Redis connection information

        Returns:
            Dict: Connection information including health status
        """
        return {
            "url": self.redis_url,
            "healthy": self.is_healthy(),
            "connection_attempts": self._connection_attempts,
            "last_health_check": self._last_health_check,
            "max_connections": self.max_connections,
        }

    def get_client(self) -> Optional[redis.Redis]:
        """
        Get the Redis client instance

        Returns:
            Optional[redis.Redis]: Redis client if healthy, None otherwise
        """
        return self._redis_client if self._is_healthy else None

    def close(self) -> None:
        """Close Redis connection"""
        if self._redis_client:
            try:
                self._redis_client.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
            finally:
                self._redis_client = None
                self._is_healthy = False

    def __enter__(self) -> "RedisManager":
        """Context manager entry"""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit"""
        self.close()
