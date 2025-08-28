"""
Core optimization framework for job scrapers.

This package provides:
- BaseScraper: Abstract base class for all scrapers
- CacheManager: Intelligent caching system
- PerformanceMonitor: Performance tracking and logging
- BaseOptimizer: Memory and processing optimizations
- ThreadingManager: Parallel processing for global searches (Phase 2)
"""

from .base_optimizer import BaseOptimizer
from .base_scraper import BaseScraper
from .cache_manager import CacheManager
from .performance_monitor import PerformanceMonitor
from .threading_manager import ThreadingManager

__all__ = ["BaseScraper", "CacheManager", "PerformanceMonitor", "BaseOptimizer", "ThreadingManager"]
