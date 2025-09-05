"""
Cache module for job search results.

Provides various caching strategies including file-based, Redis-based,
and hybrid caching solutions.
"""

from .simple_cache_key_generator import SimpleCacheKeyGenerator

__all__ = ["SimpleCacheKeyGenerator"]
