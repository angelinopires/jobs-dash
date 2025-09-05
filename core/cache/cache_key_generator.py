"""
Cache Key Generator

Provides intelligent, consistent cache key generation across all cache layers.
Supports complex search parameters, analytics tracking, and cache optimization.
"""

import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


class CacheKeyGenerator:
    """
    Intelligent cache key generator for multi-layer caching strategy.

    Features:
    - Consistent key generation across Redis, File, and API layers
    - Human-readable keys with hash-based uniqueness
    - Support for complex search parameters
    - Analytics-friendly key structure
    - Cache optimization insights
    """

    def __init__(self, include_timestamp: bool = False, hash_length: int = 8, separator: str = "_"):
        """
        Initialize the cache key generator.

        Args:
            include_timestamp: Whether to include timestamp in keys
            hash_length: Length of hash suffix (default: 8 chars)
            separator: Character to separate key components
        """
        self.include_timestamp = include_timestamp
        self.hash_length = hash_length
        self.separator = separator

        # Common job title variations for cache optimization
        self._job_title_variations = {
            "software engineer": ["software engineer", "software developer", "dev", "programmer"],
            "data scientist": ["data scientist", "data analyst", "ml engineer", "ai engineer"],
            "product manager": ["product manager", "pm", "product owner", "scrum master"],
            "devops engineer": ["devops engineer", "sre", "platform engineer", "infrastructure engineer"],
            "frontend developer": ["frontend developer", "front-end dev", "ui developer", "web developer"],
            "backend developer": ["backend developer", "back-end dev", "api developer", "server developer"],
            "full stack developer": ["full stack developer", "fullstack", "full-stack", "web developer"],
        }

        # Location normalization for cache optimization
        self._location_variations = {
            "global": ["global", "worldwide", "remote", "anywhere", ""],
            "united states": ["united states", "usa", "us", "america", "states"],
            "san francisco": ["san francisco", "sf", "bay area", "silicon valley", "california"],
            "new york": ["new york", "nyc", "ny", "manhattan", "brooklyn"],
            "london": ["london", "uk", "united kingdom", "england"],
            "toronto": ["toronto", "canada", "ontario", "gta"],
            "sydney": ["sydney", "australia", "nsw", "oz"],
            "berlin": ["berlin", "germany", "deutschland", "de"],
            "amsterdam": ["amsterdam", "netherlands", "holland", "nl"],
            "brazil": ["brazil", "brasil", "br", "são paulo", "rio de janeiro"],
        }

    def generate_cache_key(
        self,
        scraper: str,
        search_term: str,
        location: str,
        remote: bool = True,
        posting_age: str = "Past Week",
        **kwargs: Any,
    ) -> str:
        """
        Generate a comprehensive cache key for job search parameters.

        Args:
            scraper: Name of the scraper (e.g., 'indeed', 'linkedin')
            search_term: Job title or search term
            location: Geographic location
            remote: Whether to search for remote jobs
            posting_age: Posting age filter
            **kwargs: Additional search parameters

        Returns:
            str: Generated cache key
        """
        try:
            # Normalize parameters
            normalized_scraper = self._normalize_scraper(scraper)
            normalized_search = self._normalize_search_term(search_term)
            normalized_location = self._normalize_location(location)
            normalized_posting_age = self._normalize_posting_age(posting_age)

            # Build key components
            key_components = [
                normalized_scraper,
                normalized_search,
                normalized_location,
                "remote" if remote else "onsite",
                normalized_posting_age,
            ]

            # Add additional parameters
            for key, value in sorted(kwargs.items()):
                if value is not None and value != "":
                    key_components.append(f"{key}_{value}")

            # Add timestamp if enabled
            if self.include_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H")
                key_components.append(timestamp)

            # Create human-readable key
            human_key = self.separator.join(key_components)

            # Generate hash for uniqueness
            hash_suffix = self._generate_hash(human_key)

            # Final key format: human_readable_hash
            final_key = f"{human_key}_{hash_suffix}"

            logger.debug(f"Generated cache key: {final_key}")
            return final_key

        except Exception as e:
            logger.error(f"Error generating cache key: {e}")
            # Fallback to simple key
            return f"{scraper}_{search_term}_{location}_{hashlib.md5(str(kwargs).encode()).hexdigest()[:8]}"

    def parse_cache_key(self, cache_key: str) -> Dict[str, Any]:
        """
        Parse a cache key back into its components.

        Args:
            cache_key: The cache key to parse

        Returns:
            Dict[str, Any]: Parsed components
        """
        try:
            # Remove hash suffix
            if "_" in cache_key:
                key_parts = cache_key.rsplit("_", 1)
                if len(key_parts) == 2 and len(key_parts[1]) == self.hash_length:
                    cache_key = key_parts[0]

            # Split by separator
            components = cache_key.split(self.separator)

            # Parse components
            parsed = {}
            if len(components) >= 5:
                parsed["scraper"] = components[0]
                # Handle search terms that might contain underscores
                if len(components) >= 5:
                    # The search term might span multiple components if it contains underscores
                    # We know the structure: scraper, search_term_parts..., location, remote, posting_age
                    # So we need to find where the location starts
                    location_index = -3  # location is 3rd from the end
                    remote_index = -2  # remote is 2nd from the end
                    posting_age_index = -1  # posting_age is last

                    # Reconstruct search term from all components between scraper and location
                    search_term_parts = components[1:location_index]
                    parsed["search_term"] = "_".join(search_term_parts)
                    parsed["location"] = components[location_index]
                    parsed["remote"] = str(components[remote_index] == "remote")
                    parsed["posting_age"] = components[posting_age_index]

                # Parse additional parameters if they exist
                for component in components[5:]:
                    if "_" in component:
                        key, value = component.split("_", 1)
                        parsed[key] = value
            else:
                return {"raw_key": cache_key}

            return parsed

        except Exception as e:
            logger.error(f"Error parsing cache key: {e}")
            return {"raw_key": cache_key}

    def _normalize_scraper(self, scraper: str) -> str:
        """Normalize scraper name."""
        scraper_map = {
            "indeed": "indeed",
            "linkedin": "linkedin",
            "glassdoor": "glassdoor",
            "ziprecruiter": "ziprecruiter",
            "dice": "dice",
            "monster": "monster",
        }
        return scraper_map.get(scraper.lower().strip(), scraper.lower().strip())

    def _normalize_search_term(self, search_term: str) -> str:
        """Normalize search term for consistency."""
        if not search_term:
            return ""
        # Remove special characters and normalize spaces
        normalized = search_term.lower().strip()
        normalized = normalized.replace(" ", self.separator).replace("-", self.separator)
        normalized = "".join(c for c in normalized if c.isalnum() or c == self.separator)
        return normalized

    def _normalize_location(self, location: str) -> str:
        """Normalize location for consistency."""
        if not location or location.lower() in ["global", "worldwide", "remote", "anywhere"]:
            return "global"

        # Normalize common locations
        location_map = {
            "san francisco": "sf",
            "new york": "nyc",
            "los angeles": "la",
            "united states": "usa",
            "united kingdom": "uk",
            "canada": "ca",
            "australia": "au",
            "germany": "de",
            "netherlands": "nl",
            "brazil": "br",
        }

        normalized = location.lower().strip()
        return location_map.get(normalized, normalized.replace(" ", "_"))

    def _normalize_posting_age(self, posting_age: str) -> str:
        """Normalize posting age filter."""
        # Map time filter options from utils/time_filters.py to normalized forms
        age_map = {
            "last 24h": "24h",
            "last 72h": "72h",
            "past week": "week",
            "past month": "month",
            # Handle variations that might be passed
            "past 24 hours": "24h",
            "past 72 hours": "72h",
        }
        return age_map.get(posting_age.lower().strip(), "month")

    def _generate_hash(self, key_string: str) -> str:
        """Generate hash suffix for cache key."""
        return hashlib.md5(key_string.encode()).hexdigest()[: self.hash_length]

    def get_cache_optimization_tips(self, search_patterns: List[str]) -> List[str]:
        """
        Get cache optimization tips based on search patterns.

        Args:
            search_patterns: List of recent search patterns

        Returns:
            List[str]: Optimization tips
        """
        tips = []

        # Analyze search patterns
        if len(search_patterns) > 10:
            tips.append("Consider implementing cache warming for popular searches")

        # Check for location variations
        locations = set()
        for pattern in search_patterns:
            parsed = self.parse_cache_key(pattern)
            if "location" in parsed:
                locations.add(parsed["location"])

        if len(locations) > 5:
            tips.append("Multiple locations detected - consider location-based cache partitioning")

        # Check for job title variations
        job_titles = set()
        for pattern in search_patterns:
            parsed = self.parse_cache_key(pattern)
            if "search_term" in parsed:
                job_titles.add(parsed["search_term"])

        if len(job_titles) > 8:
            tips.append("Multiple job titles detected - consider title-based cache optimization")

        return tips
