"""
Simple Cache Key Generator

Eliminates cache misses on identical searches by generating consistent, human-readable keys.

Key Format: {scraper}:{country_code}:{remote_flag}:{time_hours}:{base_search_term}
Examples:
- indeed:usa:remote:24:software_engineer
- indeed:usa:onsite:24:software_engineer
- indeed:global:remote:168:data_scientist
"""

import re
from typing import Dict

from data.job_filters import GLOBAL_COUNTRIES


class SimpleCacheKeyGenerator:
    """
    Simple, predictable cache key generator for job search parameters.

    This is like a URL slug generator but for cache keys. It takes messy search parameters
    and creates clean, consistent keys that are both human-readable and guarantee that
    identical searches always produce the same key.

    Why this approach rocks:
    - Same input = same key, always (no more cache misses on identical searches)
    - Human-readable keys make debugging a breeze
    - 50 lines instead of 292 lines = way easier to maintain
    - No complex normalization = fewer bugs
    """

    def __init__(self) -> None:
        """Initialize the simple cache key generator."""
        # Map country names to simple codes using existing GLOBAL_COUNTRIES
        # This ensures consistency with the existing country system
        self._country_code_map = _build_country_code_map_static()

    def generate_cache_key(self, scraper: str, search_term: str, country: str, remote: bool, time_filter: str) -> str:
        """
        Generate simple, predictable cache keys.

        Args:
            scraper: Name of the scraper (e.g., 'indeed')
            search_term: Job search term (may include remote keywords)
            country: Country name for the search
            remote: Whether searching for remote jobs
            time_filter: Time filter string (e.g., "Last 24h", "Past Week")

        Returns:
            str: Cache key in format: scraper:country_code:remote_flag:hours:base_term

        Examples:
            >>> generator = SimpleCacheKeyGenerator()
            >>> generator.generate_cache_key(
            ...     "indeed", "Software Engineer", "United States", True, "Last 24h"
            ... )
            'indeed:usa:remote:24:software_engineer'
        """
        country_code = self._normalize_country(country)
        remote_flag = "remote" if remote else "onsite"
        hours = self._extract_hours(time_filter)
        base_term = self._extract_base_search_term(search_term)
        cache_key = f"{scraper}:{country_code}:{remote_flag}:{hours}:{base_term}"

        return cache_key.lower()  # Ensure consistent casing

    def _normalize_country(self, country: str) -> str:
        """
        Map country names to simple codes.

        This uses the existing GLOBAL_COUNTRIES mapping to ensure consistency
        with the rest of the application.

        Args:
            country: Country name (e.g., "United States", "Global")

        Returns:
            str: Country code (e.g., "usa", "global")
        """
        if not country or not country.strip():
            return "global"

        # Handle common variations
        country_lower = country.lower().strip()

        # Handle global variations first
        if country_lower in ["global", "worldwide", "anywhere"]:
            return "global"

        # Use the country code mapping that includes variations
        country_map = self._country_code_map
        return country_map.get(country_lower, "global")

    def _extract_hours(self, time_filter: str) -> str:
        """
        Extract hours from time filter strings.

        Args:
            time_filter: Time filter (e.g., "Last 24h", "Past Week", "Past Month")

        Returns:
            str: Hours as string ("24", "72", "168", "any")

        Examples:
            >>> generator = SimpleCacheKeyGenerator()
            >>> generator._extract_hours("Last 24h")
            '24'
            >>> generator._extract_hours("Past Week")
            '168'
        """
        if not time_filter:
            return "any"

        time_lower = time_filter.lower().strip()

        # Extract numbers first (handles "24h", "72h", etc.)
        if "24" in time_lower:
            return "24"
        elif "72" in time_lower:
            return "72"
        elif "week" in time_lower:
            return "168"  # 7 days * 24 hours
        elif "month" in time_lower:
            return "any"  # Treat month as "any" for caching
        else:
            return "any"

    def _extract_base_search_term(self, search_term: str) -> str:
        """
        Extract base search term, removing remote keywords added by the system.

        The remote keyword enhancement adds patterns like:
        "Software Engineer (remote OR "work from home" OR WFH OR distributed OR telecommute OR "home office")"

        We want to extract just "Software Engineer" for the cache key.

        Args:
            search_term: Search term (may include remote keywords in parentheses)

        Returns:
            str: Clean base search term (e.g., "software_engineer")

        Examples:
            >>> generator = SimpleCacheKeyGenerator()
            >>> generator._extract_base_search_term(
            ...     'Software Engineer (remote OR "work from home" OR WFH)'
            ... )
            'software_engineer'
            >>> generator._extract_base_search_term("Data Scientist")
            'data_scientist'
        """
        if not search_term or not search_term.strip():
            return "unknown_job"

        search_term = search_term.strip()

        # First, check if this has remote keywords appended by the system
        # Pattern: "Base Term (remote OR "work from home" OR ...)"
        # We need to be careful to only remove remote keyword patterns, not all parentheses
        remote_pattern = r"\s+\(.*(?:remote|work from home|wfh|distributed|telecommute|home office).*\)$"
        if re.search(remote_pattern, search_term, re.IGNORECASE):
            # This appears to be a remote keyword pattern, extract the base term
            base_term = re.sub(remote_pattern, "", search_term, flags=re.IGNORECASE).strip()
        else:
            # Keep the full term, including any parentheses that might contain real job details
            base_term = search_term

        # Clean and normalize the base term
        # Convert to lowercase, replace spaces/hyphens with underscores, keep alphanumeric
        normalized = base_term.lower().strip()
        # Keep letters, numbers, spaces, hyphens - remove special chars for consistency
        normalized = re.sub(r"[^a-z0-9\s\-_]", "", normalized)  # Only keep basic alphanumeric chars
        normalized = re.sub(r"[\s\-]+", "_", normalized)  # Replace spaces/hyphens with underscores
        normalized = re.sub(r"_+", "_", normalized)  # Collapse multiple underscores
        normalized = normalized.strip("_")  # Remove leading/trailing underscores

        return normalized if normalized else "unknown_job"


# Dictionary mapping for country codes
# This ensures consistency with the existing GLOBAL_COUNTRIES system
def _build_country_code_map_static() -> Dict[str, str]:
    """Build a static country code mapping for use by SimpleCacheKeyGenerator."""
    base_map = {name.lower(): code.lower() for name, code in GLOBAL_COUNTRIES}

    # Add common variations for better coverage
    variations = {
        "america": "usa",
        "anywhere": "global",
        "br": "brazil",
        "brasil": "brazil",
        "britain": "uk",
        "ca": "canada",
        "england": "uk",
        "global": "global",
        "states": "usa",
        "uk": "uk",
        "us": "usa",
        "usa": "usa",  # Add this explicitly
        "worldwide": "global",
    }

    return {**base_map, **variations}
