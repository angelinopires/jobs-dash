"""
Cache module for job search results.

Provides various caching strategies including file-based, Redis-based,
and hybrid caching solutions.
"""

from .cache_key_generator import CacheKeyGenerator

__all__ = ["CacheKeyGenerator"]
