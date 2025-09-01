"""
Environment Configuration Management

This module handles loading and validating environment variables for the Jobs Dashboard.
It provides fallback defaults and type validation for all configuration options.
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

# Set up logging for configuration loading
logger = logging.getLogger(__name__)


@dataclass
class CircuitBreakerConfig:
    """
    Circuit Breaker Configuration

    This dataclass holds all circuit breaker settings.
    Think of it like a TypeScript interface or React props object.
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
    Think of it like a TypeScript interface for rate limiting props.
    """

    base_delay: float
    max_delay: float

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.base_delay < 0:
            raise ValueError("Base delay must be non-negative")
        if self.max_delay < self.base_delay:
            raise ValueError("Max delay must be greater than base delay")


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
        self._load_configurations()

    def _load_configurations(self) -> None:
        """Load and validate all environment configurations"""
        logger.info("Loading environment configurations...")

        try:
            # Load circuit breaker configuration
            self._circuit_breaker_config = self._load_circuit_breaker_config()

            # Load rate limiting configuration
            self._rate_limit_config = self._load_rate_limit_config()

            logger.info("Environment configurations loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load environment configurations: {e}")
            # Use safe defaults if configuration fails
            self._circuit_breaker_config = CircuitBreakerConfig(threshold=5, timeout=300)
            self._rate_limit_config = RateLimitConfig(base_delay=2.0, max_delay=60.0)

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
