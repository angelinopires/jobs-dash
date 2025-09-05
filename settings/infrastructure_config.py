"""
Environment Configuration Management

This module handles loading and validating environment variables for the Jobs Dashboard.
It provides fallback defaults and type validation for all configuration options.
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

# Set up logging for configuration loading
logger = logging.getLogger(__name__)


@dataclass
class CircuitBreakerConfig:
    """
    Circuit Breaker Configuration

    This dataclass holds all circuit breaker settings with hardcoded defaults.
    """

    threshold: int = 5
    timeout: int = 300

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.threshold < 1:
            raise ValueError("Circuit breaker threshold must be at least 1")
        if self.timeout < 1:
            raise ValueError("Circuit breaker timeout must be at least 1 second")


@dataclass
class RedisConfig:
    """
    Redis Configuration

    This dataclass holds all Redis connection settings with sensible defaults.
    """

    url: str
    ttl: int
    max_connections: int = 10
    retry_attempts: int = 3
    retry_delay: float = 1.0
    health_check_interval: int = 30

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.ttl < 1:
            raise ValueError("Redis TTL must be at least 1 second")
        if self.max_connections < 1:
            raise ValueError("Max connections must be at least 1")
        if self.retry_attempts < 1:
            raise ValueError("Retry attempts must be at least 1")
        if self.retry_delay < 0:
            raise ValueError("Retry delay must be non-negative")
        if self.health_check_interval < 1:
            raise ValueError("Health check interval must be at least 1 second")


@dataclass
class ThreadingConfig:
    """
    Threading Configuration

    This dataclass holds all threading and parallel processing settings.
    """

    max_workers: int
    timeout_per_country: int = 30

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.max_workers < 1:
            raise ValueError("Max workers must be at least 1")
        if self.max_workers > 20:
            raise ValueError("Max workers cannot exceed 20 (to prevent API overload)")
        if self.timeout_per_country < 1:
            raise ValueError("Timeout per country must be at least 1 second")


@dataclass
class CacheConfig:
    """
    Cache Configuration

    This dataclass holds cache-specific settings for Redis caching strategy.
    Uses the existing Redis TTL setting directly in seconds (no conversion needed).
    Cache is always enabled when Redis is available (keep it simple).
    """

    ttl_seconds: int

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.ttl_seconds < 1:  # Minimum 1 second
            raise ValueError("Cache TTL must be at least 1 second")
        if self.ttl_seconds > 86400:  # 24 hours in seconds
            raise ValueError("Cache TTL cannot exceed 86400 seconds (24 hours)")


class EnvironmentManager:
    """
    Environment Variable Manager

    This class manages all environment variables with validation and defaults.
    Similar to a configuration service in Angular or a context provider in React.
    """

    def __init__(self) -> None:
        """Initialize the environment manager and load all configurations"""
        self._circuit_breaker_config: Optional[CircuitBreakerConfig] = None
        self._redis_config: Optional[RedisConfig] = None
        self._threading_config: Optional[ThreadingConfig] = None
        self._cache_config: Optional[CacheConfig] = None
        self._load_configurations()

    def _load_configurations(self) -> None:
        """Load and validate all environment configurations"""
        logger.info("Loading environment configurations...")

        try:

            self._circuit_breaker_config = self._load_circuit_breaker_config()
            self._redis_config = self._load_redis_config()
            self._threading_config = self._load_threading_config()
            self._cache_config = self._load_cache_config()

            logger.info("Environment configurations loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load environment configurations: {e}")
            # Use safe defaults if configuration fails
            self._circuit_breaker_config = CircuitBreakerConfig()
            self._redis_config = RedisConfig(
                url="redis://localhost:6379",
                ttl=3600,
                max_connections=10,
            )
            self._threading_config = ThreadingConfig(max_workers=4)
            self._cache_config = CacheConfig(ttl_seconds=3600)  # 1 hour default

    def _load_circuit_breaker_config(self) -> CircuitBreakerConfig:
        """
        Load circuit breaker configuration with hardcoded defaults

        Returns:
            CircuitBreakerConfig: Validated configuration object
        """
        logger.debug("Circuit breaker config - using hardcoded defaults: threshold=5, timeout=300s")
        return CircuitBreakerConfig()

    def _load_redis_config(self) -> RedisConfig:
        """
        Load Redis configuration from environment variables

        Returns:
            RedisConfig: Validated configuration object
        """
        # Get environment variables with fallback defaults
        url = os.getenv("REDIS_URL", "redis://localhost:6379")
        ttl = self._get_env_int("REDIS_TTL", default=3600)
        max_connections = self._get_env_int("REDIS_MAX_CONNECTIONS", default=10)

        logger.debug(f"Redis config - url: {url}, ttl: {ttl}s, max_connections: {max_connections}")

        return RedisConfig(
            url=url,
            ttl=ttl,
            max_connections=max_connections,
        )

    def _load_threading_config(self) -> ThreadingConfig:
        """
        Load threading configuration from environment variables

        Returns:
            ThreadingConfig: Validated configuration object
        """
        # Get environment variables with fallback defaults
        max_workers = self._get_env_int("THREADING_MAX_WORKERS", default=4)

        logger.debug(f"Threading config - max_workers: {max_workers}, timeout_per_country: 30s (hardcoded)")

        return ThreadingConfig(max_workers=max_workers)

    def _load_cache_config(self) -> CacheConfig:
        """
        Load cache configuration from environment variables

        Uses the existing REDIS_TTL setting directly in seconds (no conversion).
        Cache is always enabled when Redis is available (simple strategy).

        Returns:
            CacheConfig: Validated configuration object
        """
        # Use existing Redis TTL setting directly (no conversion needed)
        ttl_seconds = self._get_env_int("REDIS_TTL", default=3600)

        logger.debug(f"Cache config - ttl_seconds: {ttl_seconds}s (from REDIS_TTL)")

        return CacheConfig(ttl_seconds=ttl_seconds)

    def _get_env_float(self, key: str, default: float) -> float:
        """
        Get float environment variable with validation

        Args:
            key: Environment variable name
            default: Default value if not set or invalid

        Returns:
            float: Validated float value
        """
        value = os.getenv(key)

        if value is None:
            logger.debug(f"Environment variable {key} not set, using default: {default}")
            return default

        try:
            float_value = float(value)
            if float_value < 0:
                logger.warning(f"Environment variable {key} is negative, using default: {default}")
                return default
            return float_value

        except ValueError:
            logger.warning(f"Environment variable {key} is not a valid float, using default: {default}")
            return default

    def _get_env_int(self, key: str, default: int) -> int:
        """
        Get integer environment variable with validation

        Args:
            key: Environment variable name
            default: Default value if not set or invalid

        Returns:
            int: Validated integer value
        """
        value = os.getenv(key)

        if value is None:
            logger.debug(f"Environment variable {key} not set, using default: {default}")
            return default

        try:
            int_value = int(value)
            if int_value < 0:
                logger.warning(f"Environment variable {key} is negative, using default: {default}")
                return default
            return int_value

        except ValueError:
            logger.warning(f"Environment variable {key} is not a valid integer, using default: {default}")
            return default

    @property
    def circuit_breaker(self) -> CircuitBreakerConfig:
        """
        Get circuit breaker configuration

        Returns:
            CircuitBreakerConfig: Current circuit breaker settings
        """
        if self._circuit_breaker_config is None:
            # Fallback to defaults if not loaded
            self._circuit_breaker_config = CircuitBreakerConfig()
        return self._circuit_breaker_config

    @property
    def redis(self) -> RedisConfig:
        """
        Get Redis configuration

        Returns:
            RedisConfig: Current Redis settings
        """
        if self._redis_config is None:
            # Fallback to defaults if not loaded
            self._redis_config = RedisConfig(
                url="redis://localhost:6379",
                ttl=3600,
                max_connections=10,
            )
        return self._redis_config

    @property
    def threading(self) -> ThreadingConfig:
        """
        Get threading configuration

        Returns:
            ThreadingConfig: Current threading settings
        """
        if self._threading_config is None:
            # Fallback to defaults if not loaded
            self._threading_config = ThreadingConfig(max_workers=4)
        return self._threading_config

    @property
    def cache(self) -> CacheConfig:
        """
        Get cache configuration

        Returns:
            CacheConfig: Current cache settings
        """
        if self._cache_config is None:
            # Fallback to defaults if not loaded
            self._cache_config = CacheConfig(ttl_seconds=3600)  # 1 hour default
        return self._cache_config


# Global environment manager instance
# This is like a singleton service in Angular or a global context in React
_environment_manager: Optional[EnvironmentManager] = None


def get_environment_manager() -> EnvironmentManager:
    """
    Get the global environment manager instance

    Returns:
        EnvironmentManager: Singleton environment manager
    """
    global _environment_manager
    if _environment_manager is None:
        _environment_manager = EnvironmentManager()
    return _environment_manager


def get_circuit_breaker_config() -> CircuitBreakerConfig:
    """
    Convenience function to get circuit breaker configuration

    Returns:
        CircuitBreakerConfig: Current circuit breaker settings
    """
    return get_environment_manager().circuit_breaker


def get_redis_config() -> RedisConfig:
    """
    Convenience function to get Redis configuration

    Returns:
        RedisConfig: Current Redis settings
    """
    return get_environment_manager().redis


def get_threading_config() -> ThreadingConfig:
    """
    Convenience function to get threading configuration

    Returns:
        ThreadingConfig: Current threading settings
    """
    return get_environment_manager().threading


def get_cache_config() -> CacheConfig:
    """
    Convenience function to get cache configuration

    Returns:
        CacheConfig: Current cache settings
    """
    return get_environment_manager().cache
