"""
Settings module for the job dashboard.

Contains environment configuration and application settings.
"""

from .environment import (
    CacheConfig,
    CircuitBreakerConfig,
    RedisConfig,
    ThreadingConfig,
    get_cache_config,
    get_circuit_breaker_config,
    get_environment_manager,
    get_redis_config,
    get_threading_config,
)

__all__ = [
    # Configuration classes
    "CircuitBreakerConfig",
    "RedisConfig",
    "ThreadingConfig",
    "CacheConfig",
    # Environment manager
    "get_environment_manager",
    # Convenience functions
    "get_circuit_breaker_config",
    "get_redis_config",
    "get_threading_config",
    "get_cache_config",
]
