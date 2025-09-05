"""
Core module for the jobs dashboard.

Provides the main infrastructure components including caching, Redis management,
monitoring, scraping, and resilience mechanisms.
"""

# Import from submodules
from .cache import SimpleCacheKeyGenerator
from .monitoring import PerformanceMonitor, SearchAnalytics
from .redis import RedisCacheManager, RedisManager
from .resilience import (
    CircuitBreaker,
    CircuitOpenException,
    CircuitState,
    EndpointStats,
    IntelligentRateLimiter,
    RateLimitConfig,
    RateLimitState,
    get_circuit_breaker,
    get_rate_limiter,
)
from .scrapers import BaseJobScraper, FilterCapabilities, get_indeed_scraper
from .search import SearchOptimizer, SearchOrchestrator, SearchResult, SearchTask, ThreadingManager

__all__ = [
    # Cache
    "SimpleCacheKeyGenerator",
    # Redis
    "RedisManager",
    "RedisCacheManager",
    # Monitoring
    "PerformanceMonitor",
    "SearchAnalytics",
    # Search Infrastructure
    "SearchOrchestrator",
    "SearchOptimizer",
    "ThreadingManager",
    "SearchTask",
    "SearchResult",
    # Job Board Scrapers
    "BaseJobScraper",
    "FilterCapabilities",
    "get_indeed_scraper",
    # Resilience
    "RateLimitConfig",
    "RateLimitState",
    "EndpointStats",
    "IntelligentRateLimiter",
    "get_rate_limiter",
    "CircuitState",
    "CircuitBreaker",
    "CircuitOpenException",
    "get_circuit_breaker",
]
