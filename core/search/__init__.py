"""
Search module for job search infrastructure and orchestration.

Provides search optimization, parallel execution, and coordination
for job searches across multiple countries and job boards.
"""

from .search_optimizer import SearchOptimizer
from .search_orchestrator import SearchOrchestrator
from .threading_manager import SearchResult, SearchTask, ThreadingManager

__all__ = ["SearchOrchestrator", "SearchOptimizer", "ThreadingManager", "SearchTask", "SearchResult"]
