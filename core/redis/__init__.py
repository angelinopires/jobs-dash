"""
Redis module for connection management and caching.

Provides Redis connection pooling, health monitoring, and caching operations.
"""

from .redis_manager import RedisManager

__all__ = ["RedisManager"]
