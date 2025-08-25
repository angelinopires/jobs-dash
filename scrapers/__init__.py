"""
Scrapers module for the job dashboard.
Contains specialized scrapers for different job boards.
"""

from .optimized_indeed_scraper import get_indeed_scraper

__all__ = ['get_indeed_scraper']
