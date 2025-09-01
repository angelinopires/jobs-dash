"""
Environment Configuration Management

This module handles loading and validating environment variables for the Jobs Dashboard.
It provides fallback defaults and type validation for all configuration options.
"""

import logging
import os
from dataclasses import dataclass
from typing import Any, Optional

# Set up logging for configuration loading
logger = logging.getLogger(__name__)


@dataclass
class CircuitBreakerConfig:
    """
    Circuit Breaker Configuration

    This dataclass holds all circuit breaker settings.
    """

    threshold: int
    timeout: int

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.threshold < 1:
            raise ValueError("Circuit breaker threshold must be at least 1")
        if self.timeout < 1:
            raise ValueError("Circuit breaker timeout must be at least 1 second")


@dataclass
class RateLimitConfig:
    """
    Rate Limiter Configuration

    This dataclass holds all rate limiting settings.
    """

    base_delay: float
    max_delay: float

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.base_delay < 0:
            raise ValueError("Base delay must be non-negative")
        if self.max_delay < self.base_delay:
            raise ValueError("Max delay must be greater than base delay")


@dataclass
class RedisConfig:
    """
    Redis Configuration

    This dataclass holds all Redis connection settings.
    """

    url: str
    ttl: int
    max_connections: int
    retry_attempts: int
    retry_delay: float
    health_check_interval: int

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
    timeout_per_country: int

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.max_workers < 1:
            raise ValueError("Max workers must be at least 1")
        if self.max_workers > 20:
            raise ValueError("Max workers cannot exceed 20 (to prevent API overload)")
        if self.timeout_per_country < 1:
            raise ValueError("Timeout per country must be at least 1 second")


@dataclass
class CacheWarmingConfig:
    """
    Cache Warming Configuration

    This dataclass holds all cache warming settings.
    """

    interval_hours: int
    max_results_per_search: int

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.interval_hours < 1:
            raise ValueError("Cache warming interval must be at least 1 hour")
        if self.interval_hours > 168:  # 1 week max
            raise ValueError("Cache warming interval cannot exceed 168 hours (1 week)")
        if self.max_results_per_search < 1:
            raise ValueError("Max results per search must be at least 1")
        if self.max_results_per_search > 100:
            raise ValueError("Max results per search cannot exceed 100")


class EnvironmentManager:
    """
    Environment Variable Manager

    This class manages all environment variables with validation and defaults.
    Similar to a configuration service in Angular or a context provider in React.
    """

    def __init__(self) -> None:
        """Initialize the environment manager and load all configurations"""
        self._circuit_breaker_config: Optional[CircuitBreakerConfig] = None
        self._rate_limit_config: Optional[RateLimitConfig] = None
        self._redis_config: Optional[RedisConfig] = None
        self._threading_config: Optional[ThreadingConfig] = None
        self._cache_warming_config: Optional[CacheWarmingConfig] = None
        self._load_configurations()

    def _load_configurations(self) -> None:
        """Load and validate all environment configurations"""
        logger.info("Loading environment configurations...")

        try:

            self._circuit_breaker_config = self._load_circuit_breaker_config()
            self._rate_limit_config = self._load_rate_limit_config()
            self._redis_config = self._load_redis_config()
            self._threading_config = self._load_threading_config()
            self._cache_warming_config = self._load_cache_warming_config()

            logger.info("Environment configurations loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load environment configurations: {e}")
            # Use safe defaults if configuration fails
            self._circuit_breaker_config = CircuitBreakerConfig(threshold=5, timeout=300)
            self._rate_limit_config = RateLimitConfig(base_delay=2.0, max_delay=60.0)
            self._redis_config = RedisConfig(
                url=REDIS_DEFAULTS["url"],
                ttl=REDIS_DEFAULTS["ttl"],
                max_connections=REDIS_DEFAULTS["max_connections"],
                retry_attempts=REDIS_DEFAULTS["retry_attempts"],
                retry_delay=REDIS_DEFAULTS["retry_delay"],
                health_check_interval=REDIS_DEFAULTS["health_check_interval"],
            )
            self._threading_config = ThreadingConfig(max_workers=4, timeout_per_country=30)

    def _load_circuit_breaker_config(self) -> CircuitBreakerConfig:
        """
        Load circuit breaker configuration from environment variables

        Returns:
            CircuitBreakerConfig: Validated configuration object
        """
        # Get environment variables with fallback defaults
        threshold = self._get_env_int("CIRCUIT_BREAKER_THRESHOLD", default=5)
        timeout = self._get_env_int("CIRCUIT_BREAKER_TIMEOUT", default=300)

        logger.debug(f"Circuit breaker config - threshold: {threshold}, timeout: {timeout}s")

        return CircuitBreakerConfig(threshold=threshold, timeout=timeout)

    def _load_rate_limit_config(self) -> RateLimitConfig:
        """
        Load rate limiting configuration from environment variables

        Returns:
            RateLimitConfig: Validated configuration object
        """
        # Get environment variables with fallback defaults
        base_delay = self._get_env_float("RATE_LIMITER_BASE_DELAY", default=2.0)
        max_delay = self._get_env_float("RATE_LIMITER_MAX_DELAY", default=60.0)

        logger.debug(f"Rate limiter config - base_delay: {base_delay}s, max_delay: {max_delay}s")

        return RateLimitConfig(base_delay=base_delay, max_delay=max_delay)

    def _load_redis_config(self) -> RedisConfig:
        """
        Load Redis configuration from environment variables

        Returns:
            RedisConfig: Validated configuration object
        """
        # Get environment variables with fallback defaults
        url = os.getenv("REDIS_URL", REDIS_DEFAULTS["url"])
        ttl = self._get_env_int("REDIS_TTL", default=REDIS_DEFAULTS["ttl"])
        max_connections = self._get_env_int("REDIS_MAX_CONNECTIONS", default=REDIS_DEFAULTS["max_connections"])
        retry_attempts = self._get_env_int("REDIS_RETRY_ATTEMPTS", default=REDIS_DEFAULTS["retry_attempts"])
        retry_delay = self._get_env_float("REDIS_RETRY_DELAY", default=REDIS_DEFAULTS["retry_delay"])
        health_check_interval = self._get_env_int(
            "REDIS_HEALTH_CHECK_INTERVAL", default=REDIS_DEFAULTS["health_check_interval"]
        )

        logger.debug(f"Redis config - url: {url}, ttl: {ttl}s, max_connections: {max_connections}")

        return RedisConfig(
            url=url,
            ttl=ttl,
            max_connections=max_connections,
            retry_attempts=retry_attempts,
            retry_delay=retry_delay,
            health_check_interval=health_check_interval,
        )

    def _load_threading_config(self) -> ThreadingConfig:
        """
        Load threading configuration from environment variables

        Returns:
            ThreadingConfig: Validated configuration object
        """
        # Get environment variables with fallback defaults
        max_workers = self._get_env_int("THREADING_MAX_WORKERS", default=4)
        timeout_per_country = self._get_env_int("THREADING_TIMEOUT_PER_COUNTRY", default=30)

        logger.debug(f"Threading config - max_workers: {max_workers}, timeout_per_country: {timeout_per_country}s")

        return ThreadingConfig(max_workers=max_workers, timeout_per_country=timeout_per_country)

    def _load_cache_warming_config(self) -> CacheWarmingConfig:
        """
        Load cache warming configuration from environment variables

        Returns:
            CacheWarmingConfig: Validated configuration object
        """
        # Get environment variables with fallback defaults
        interval_hours = self._get_env_int("CACHE_WARMING_INTERVAL_HOURS", default=6)
        max_results_per_search = self._get_env_int("CACHE_WARMING_MAX_RESULTS_PER_SEARCH", default=50)

        logger.debug(
            f"Cache warming config - interval_hours: {interval_hours}, "
            f"max_results_per_search: {max_results_per_search}"
        )

        return CacheWarmingConfig(interval_hours=interval_hours, max_results_per_search=max_results_per_search)

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
            self._circuit_breaker_config = CircuitBreakerConfig(threshold=5, timeout=300)
        return self._circuit_breaker_config

    @property
    def rate_limit(self) -> RateLimitConfig:
        """
        Get rate limiting configuration

        Returns:
            RateLimitConfig: Current rate limiting settings
        """
        if self._rate_limit_config is None:
            # Fallback to defaults if not loaded
            self._rate_limit_config = RateLimitConfig(base_delay=2.0, max_delay=60.0)
        return self._rate_limit_config

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
                url=REDIS_DEFAULTS["url"],
                ttl=REDIS_DEFAULTS["ttl"],
                max_connections=REDIS_DEFAULTS["max_connections"],
                retry_attempts=REDIS_DEFAULTS["retry_attempts"],
                retry_delay=REDIS_DEFAULTS["retry_delay"],
                health_check_interval=REDIS_DEFAULTS["health_check_interval"],
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
            self._threading_config = ThreadingConfig(max_workers=4, timeout_per_country=30)
        return self._threading_config

    @property
    def cache_warming(self) -> CacheWarmingConfig:
        """
        Get cache warming configuration

        Returns:
            CacheWarmingConfig: Current cache warming settings
        """
        if self._cache_warming_config is None:
            # Fallback to defaults if not loaded
            self._cache_warming_config = CacheWarmingConfig(interval_hours=6, max_results_per_search=50)
        return self._cache_warming_config


# Default configuration values
REDIS_DEFAULTS: dict[str, Any] = {
    "url": "redis://localhost:6379",
    "ttl": 3600,
    "max_connections": 10,
    "retry_attempts": 3,
    "retry_delay": 1.0,
    "health_check_interval": 30,
}

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


def get_rate_limit_config() -> RateLimitConfig:
    """
    Convenience function to get rate limiting configuration

    Returns:
        RateLimitConfig: Current rate limiting settings
    """
    return get_environment_manager().rate_limit


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


def get_cache_warming_config() -> CacheWarmingConfig:
    """
    Convenience function to get cache warming configuration

    Returns:
        CacheWarmingConfig: Current cache warming settings
    """
    return get_environment_manager().cache_warming
