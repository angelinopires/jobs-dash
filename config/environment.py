"""
Environment Configuration Management

This module handles loading and validating environment variables for the Jobs Dashboard.
It provides fallback defaults and type validation for all configuration options.

For front-end developers learning Python:
- This is like having a centralized config object in React/JS
- Environment variables are like process.env in Node.js
- Type hints help catch errors early (like TypeScript)
- The @property decorator creates getter methods (like computed properties in Vue)
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


class EnvironmentManager:
    """
    Environment Variable Manager

    This class manages all environment variables with validation and defaults.
    Similar to a configuration service in Angular or a context provider in React.
    """

    def __init__(self) -> None:
        """Initialize the environment manager and load all configurations"""
        self._circuit_breaker_config: Optional[CircuitBreakerConfig] = None
        self._load_configurations()

    def _load_configurations(self) -> None:
        """Load and validate all environment configurations"""
        logger.info("Loading environment configurations...")

        try:
            # Load circuit breaker configuration
            self._circuit_breaker_config = self._load_circuit_breaker_config()
            logger.info("Environment configurations loaded successfully")

        except Exception as e:
            logger.error(f"Failed to load environment configurations: {e}")
            # Use safe defaults if configuration fails
            self._circuit_breaker_config = CircuitBreakerConfig(threshold=5, timeout=300)

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
