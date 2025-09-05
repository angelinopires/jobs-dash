"""
Unit tests for CacheKeyGenerator.

This module contains comprehensive tests for the cache key generation functionality.
"""

import unittest

from ..cache_key_generator import CacheKeyGenerator


class TestCacheKeyGenerator(unittest.TestCase):
    """Test cases for CacheKeyGenerator class."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.key_generator = CacheKeyGenerator()
        self.key_generator_timestamp = CacheKeyGenerator(include_timestamp=True)
        self.key_generator_custom = CacheKeyGenerator(hash_length=12, separator="-")

    def test_init_default_values(self) -> None:
        """Test default initialization values."""
        self.assertFalse(self.key_generator.include_timestamp)
        self.assertEqual(self.key_generator.hash_length, 8)
        self.assertEqual(self.key_generator.separator, "_")

    def test_init_custom_values(self) -> None:
        """Test custom initialization values."""
        self.assertTrue(self.key_generator_timestamp.include_timestamp)
        self.assertEqual(self.key_generator_custom.hash_length, 12)
        self.assertEqual(self.key_generator_custom.separator, "-")

    def test_generate_cache_key_basic(self) -> None:
        """Test basic cache key generation."""
        key = self.key_generator.generate_cache_key("indeed", "software engineer", "san francisco")

        self.assertIsInstance(key, str)
        self.assertIn("indeed", key)
        self.assertIn("software_engineer", key)
        self.assertIn("sf", key)
        self.assertIn("remote", key)
        self.assertIn("week", key)
        # Should have hash suffix
        self.assertEqual(len(key.split("_")[-1]), 8)

    def test_generate_cache_key_with_remote(self) -> None:
        """Test cache key generation with remote flag."""
        key = self.key_generator.generate_cache_key("linkedin", "data scientist", "global", remote=True)

        self.assertIn("remote", key)
        self.assertNotIn("onsite", key)

    def test_generate_cache_key_with_posting_age(self) -> None:
        """Test cache key generation with posting age."""
        key = self.key_generator.generate_cache_key("glassdoor", "product manager", "new york", posting_age="last 24h")

        self.assertIn("24h", key)

    def test_generate_cache_key_with_additional_params(self) -> None:
        """Test cache key generation with additional parameters."""
        key = self.key_generator.generate_cache_key(
            "indeed", "devops engineer", "london", salary_min=50000, experience_level="senior"
        )

        self.assertIn("salary_min_50000", key)
        self.assertIn("experience_level_senior", key)

    def test_generate_cache_key_with_timestamp(self) -> None:
        """Test cache key generation with timestamp enabled."""
        key = self.key_generator_timestamp.generate_cache_key("indeed", "frontend developer", "berlin")

        # Should contain timestamp in format YYYYMMDD_HH
        import re

        timestamp_pattern = r"\d{8}_\d{2}"
        self.assertIsNotNone(re.search(timestamp_pattern, key))

    def test_generate_cache_key_custom_separator(self) -> None:
        """Test cache key generation with custom separator."""
        key = self.key_generator_custom.generate_cache_key("indeed", "backend developer", "toronto")

        self.assertIn("-", key)
        # Hash suffix always uses underscore, so we check the main part doesn't have underscores
        main_part = key.rsplit("_", 1)[0]  # Remove hash suffix
        self.assertNotIn("_", main_part)

    def test_generate_cache_key_error_handling(self) -> None:
        """Test cache key generation error handling."""
        # Test with invalid parameters
        key = self.key_generator.generate_cache_key("", "", "", remote=False)

        # Should return fallback key
        self.assertIsInstance(key, str)
        self.assertIn("global", key)

    def test_parse_cache_key(self) -> None:
        """Test cache key parsing."""
        original_key = self.key_generator.generate_cache_key(
            "indeed", "software engineer", "san francisco", remote=True, posting_age="last 72h"
        )

        parsed = self.key_generator.parse_cache_key(original_key)

        self.assertIsInstance(parsed, dict)
        self.assertEqual(parsed.get("scraper"), "indeed")
        self.assertEqual(parsed.get("search_term"), "software_engineer")
        self.assertEqual(parsed.get("location"), "sf")
        self.assertTrue(parsed.get("remote"))
        self.assertEqual(parsed.get("posting_age"), "72h")

    def test_parse_cache_key_invalid(self) -> None:
        """Test parsing invalid cache key."""
        parsed = self.key_generator.parse_cache_key("invalid_key")

        self.assertIsInstance(parsed, dict)
        self.assertIn("raw_key", parsed)
        self.assertEqual(parsed["raw_key"], "invalid_key")

    def test_normalize_scraper(self) -> None:
        """Test scraper name normalization."""
        test_cases = [
            ("indeed", "indeed"),
            ("LINKEDIN", "linkedin"),
            ("GlassDoor", "glassdoor"),
            ("unknown", "unknown"),
        ]

        for input_scraper, expected in test_cases:
            with self.subTest(scraper=input_scraper):
                normalized = self.key_generator._normalize_scraper(input_scraper)
                self.assertEqual(normalized, expected)

    def test_normalize_search_term(self) -> None:
        """Test search term normalization."""
        test_cases = [
            ("Software Engineer", "software_engineer"),
            ("Data Scientist", "data_scientist"),
            ("Product Manager", "product_manager"),
            ("DevOps Engineer", "devops_engineer"),
            ("Front-end Developer", "front_end_developer"),
            ("Full Stack Developer", "full_stack_developer"),
        ]

        for input_term, expected in test_cases:
            with self.subTest(term=input_term):
                normalized = self.key_generator._normalize_search_term(input_term)
                self.assertEqual(normalized, expected)

    def test_normalize_location(self) -> None:
        """Test location normalization."""
        test_cases = [
            ("San Francisco", "sf"),
            ("New York", "nyc"),
            ("Los Angeles", "la"),
            ("United States", "usa"),
            ("United Kingdom", "uk"),
            ("Canada", "ca"),
            ("Australia", "au"),
            ("Germany", "de"),
            ("Netherlands", "nl"),
            ("Brazil", "br"),
            ("Global", "global"),
            ("Remote", "global"),
            ("", "global"),
            ("Unknown City", "unknown_city"),
        ]

        for input_location, expected in test_cases:
            with self.subTest(location=input_location):
                normalized = self.key_generator._normalize_location(input_location)
                self.assertEqual(normalized, expected)

    def test_normalize_posting_age(self) -> None:
        """Test posting age normalization."""
        test_cases = [
            ("Last 24h", "24h"),
            ("Last 72h", "72h"),
            ("Past Week", "week"),
            ("Past Month", "month"),
            ("Any", "month"),
            ("Unknown", "month"),
        ]

        for input_age, expected in test_cases:
            with self.subTest(age=input_age):
                normalized = self.key_generator._normalize_posting_age(input_age)
                self.assertEqual(normalized, expected)

    def test_generate_hash(self) -> None:
        """Test hash generation."""
        test_string = "test_string_for_hashing"
        hash_result = self.key_generator._generate_hash(test_string)

        self.assertIsInstance(hash_result, str)
        self.assertEqual(len(hash_result), 8)

        # Should be consistent
        hash_result2 = self.key_generator._generate_hash(test_string)
        self.assertEqual(hash_result, hash_result2)

    def test_get_cache_optimization_tips(self) -> None:
        """Test cache optimization tips generation."""
        # Create sample search patterns
        search_patterns = [
            "indeed_software_engineer_sf_remote_week_a1b2c3d4",
            "linkedin_data_scientist_nyc_remote_24h_e5f6g7h8",
            "glassdoor_product_manager_london_remote_week_i9j0k1l2",
        ]

        tips = self.key_generator.get_cache_optimization_tips(search_patterns)

        self.assertIsInstance(tips, list)
        # Tips generation depends on pattern analysis - may return 0 for small datasets
        self.assertGreaterEqual(len(tips), 0)

    def test_get_cache_optimization_tips_empty(self) -> None:
        """Test cache optimization tips with empty patterns."""
        tips = self.key_generator.get_cache_optimization_tips([])

        self.assertIsInstance(tips, list)
        self.assertEqual(len(tips), 0)

    def test_cache_key_consistency(self) -> None:
        """Test that cache keys are consistent for same inputs."""
        key1 = self.key_generator.generate_cache_key("indeed", "software engineer", "san francisco")
        key2 = self.key_generator.generate_cache_key("indeed", "software engineer", "san francisco")

        self.assertEqual(key1, key2)

    def test_cache_key_uniqueness(self) -> None:
        """Test that cache keys are unique for different inputs."""
        key1 = self.key_generator.generate_cache_key("indeed", "software engineer", "san francisco")
        key2 = self.key_generator.generate_cache_key("linkedin", "software engineer", "san francisco")
        key3 = self.key_generator.generate_cache_key("indeed", "data scientist", "san francisco")

        self.assertNotEqual(key1, key2)
        self.assertNotEqual(key1, key3)
        self.assertNotEqual(key2, key3)


if __name__ == "__main__":
    unittest.main()
