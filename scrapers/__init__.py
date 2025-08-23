"""
Scrapers module for the job dashboard.
Contains specialized scrapers for different job boards.
"""

from .indeed_scraper import IndeedScraper

__all__ = ['IndeedScraper']
