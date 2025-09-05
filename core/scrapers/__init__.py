"""
Scrapers module for job board implementations.

Contains specific implementations for different job boards (Indeed, LinkedIn, etc.)
and defines the interface/contract that all scrapers must implement.
"""

from .base_scraper import BaseJobScraper, FilterCapabilities
from .indeed_scraper import get_indeed_scraper

__all__ = ["BaseJobScraper", "FilterCapabilities", "get_indeed_scraper"]
