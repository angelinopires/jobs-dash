"""
Redis health check utility to prevent crashes when Redis is unavailable.
"""

import logging
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


def check_redis_health() -> bool:
    """
    Check if Redis is available and healthy.

    Returns:
        bool: True if Redis is available, False otherwise
    """
    try:
        import redis

        # Test if we can actually create a connection
        client = redis.Redis(host="localhost", port=6379, socket_connect_timeout=1)
        client.ping()
        return True
    except (ImportError, Exception):
        logger.warning("Redis package not available or connection failed")
        return False


def get_redis_client() -> Optional[Any]:
    """
    Get Redis client if available.

    Returns:
        Optional[redis.Redis]: Redis client or None if not available
    """
    if not check_redis_health():
        return None

    try:
        import redis

        # Basic Redis client - you can extend this with your config
        client = redis.Redis(host="localhost", port=6379, decode_responses=True)
        return client
    except Exception as e:
        logger.error(f"Failed to create Redis client: {e}")
        return None


def safe_redis_operation(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to safely execute Redis operations.

    Args:
        func: Function to decorate

    Returns:
        Decorated function that handles Redis errors gracefully
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Redis operation failed: {e}")
            return None

    return wrapper
