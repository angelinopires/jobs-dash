"""
Unit tests for SimpleCacheKeyGenerator.

This module contains comprehensive tests for the new simple cache key generator,
ensuring consistent, predictable cache key generation across all scenarios.
"""

import unittest
from typing import Any, Dict

from core.cache.simple_cache_key_generator import SimpleCacheKeyGenerator


class TestSimpleCacheKeyGenerator(unittest.TestCase):
    """Test cases for SimpleCacheKeyGenerator."""

    def setUp(self) -> None:
        """Set up test fixtures before each test method."""
        self.generator = SimpleCacheKeyGenerator()

    def test_basic_cache_key_generation(self) -> None:
        """Test basic cache key generation with standard inputs."""
        result = self.generator.generate_cache_key(
            scraper="indeed",
            search_term="Software Engineer",
            country="United States",
            remote=True,
            time_filter="Last 24h",
        )

        expected = "indeed:usa:remote:24:software_engineer"
        self.assertEqual(result, expected)

    def test_cache_key_generation_onsite(self) -> None:
        """Test cache key generation for onsite jobs."""
        result = self.generator.generate_cache_key(
            scraper="indeed", search_term="Data Scientist", country="Canada", remote=False, time_filter="Past Week"
        )

        expected = "indeed:canada:onsite:168:data_scientist"
        self.assertEqual(result, expected)

    def test_cache_key_generation_global(self) -> None:
        """Test cache key generation for global searches."""
        result = self.generator.generate_cache_key(
            scraper="indeed", search_term="Python Developer", country="Global", remote=True, time_filter="Past Month"
        )

        expected = "indeed:global:remote:any:python_developer"
        self.assertEqual(result, expected)

    def test_key_consistency_identical_inputs(self) -> None:
        """Test that identical inputs always generate the same key."""
        params: Dict[str, Any] = {
            "scraper": "indeed",
            "search_term": "Software Engineer",
            "country": "United States",
            "remote": True,
            "time_filter": "Last 24h",
        }

        key1 = self.generator.generate_cache_key(**params)
        key2 = self.generator.generate_cache_key(**params)
        key3 = self.generator.generate_cache_key(**params)

        self.assertEqual(key1, key2)
        self.assertEqual(key2, key3)
        self.assertEqual(key1, "indeed:usa:remote:24:software_engineer")

    def test_key_uniqueness_different_inputs(self) -> None:
        """Test that different inputs generate different keys."""
        base_params: Dict[str, Any] = {
            "scraper": "indeed",
            "search_term": "Software Engineer",
            "country": "United States",
            "remote": True,
            "time_filter": "Last 24h",
        }

        key_base = self.generator.generate_cache_key(**base_params)

        # Different scraper
        key_scraper = self.generator.generate_cache_key(**{**base_params, "scraper": "linkedin"})

        # Different search term
        key_search = self.generator.generate_cache_key(**{**base_params, "search_term": "Data Scientist"})

        # Different country
        key_country = self.generator.generate_cache_key(**{**base_params, "country": "Canada"})

        # Different remote flag
        key_remote = self.generator.generate_cache_key(**{**base_params, "remote": False})

        # Different time filter
        key_time = self.generator.generate_cache_key(**{**base_params, "time_filter": "Past Week"})

        keys = [key_base, key_scraper, key_search, key_country, key_remote, key_time]
        self.assertEqual(len(set(keys)), 6, "All keys should be unique")

    def test_extract_base_search_term_with_remote_keywords(self) -> None:
        """Test extracting base search term when remote keywords are present."""
        test_cases = [
            (
                'Software Engineer (remote OR "work from home" OR WFH OR distributed OR telecommute OR "home office")',
                "software_engineer",
            ),
            ('Data Scientist (remote OR "work from home")', "data_scientist"),
            ("Python Developer", "python_developer"),  # No remote keywords
            ("Full Stack Developer", "full_stack_developer"),
            ("DevOps Engineer (remote)", "devops_engineer"),
        ]

        for search_term, expected in test_cases:
            with self.subTest(search_term=search_term):
                result = self.generator._extract_base_search_term(search_term)
                self.assertEqual(result, expected)

    def test_extract_base_search_term_special_characters(self) -> None:
        """Test base search term extraction with special characters."""
        test_cases = [
            ("Software Engineer (C++)", "software_engineer_c"),
            ("Data Scientist @ TechCorp", "data_scientist_techcorp"),
            ("Frontend Developer - React", "frontend_developer_react"),
            ("Backend Developer / API", "backend_developer_api"),
            ("ML Engineer & AI Specialist", "ml_engineer_ai_specialist"),
            ("Product Manager (PM)", "product_manager_pm"),
            ("", "unknown_job"),  # Empty string should return unknown_job
            ("   ", "unknown_job"),  # Whitespace only should return unknown_job
            ("C# Developer", "c_developer"),
            ("Node.js Developer", "nodejs_developer"),
        ]

        for search_term, expected in test_cases:
            with self.subTest(search_term=search_term):
                result = self.generator._extract_base_search_term(search_term)
                self.assertEqual(result, expected)

    def test_normalize_country_global_countries(self) -> None:
        """Test country normalization for all GLOBAL_COUNTRIES."""
        test_cases = [
            ("United States", "usa"),
            ("Canada", "canada"),
            ("Mexico", "mexico"),
            ("Brazil", "brazil"),
            ("United Kingdom", "uk"),
            ("Portugal", "portugal"),
            ("Spain", "spain"),
            ("Global", "global"),
            ("global", "global"),
            ("GLOBAL", "global"),
        ]

        for country, expected in test_cases:
            with self.subTest(country=country):
                result = self.generator._normalize_country(country)
                self.assertEqual(result, expected)

    def test_normalize_country_variations(self) -> None:
        """Test country normalization with common variations."""
        test_cases = [
            ("USA", "usa"),  # This should work with the mapping
            ("us", "usa"),
            ("america", "usa"),
            ("states", "usa"),
            ("uk", "uk"),
            ("england", "uk"),
            ("britain", "uk"),
            ("ca", "canada"),
            ("br", "brazil"),
            ("brasil", "brazil"),
            ("Worldwide", "global"),
            ("Anywhere", "global"),
            ("", "global"),  # Empty string
            ("   ", "global"),  # Whitespace
            ("Unknown Country", "global"),  # Fallback to global
        ]

        for country, expected in test_cases:
            with self.subTest(country=country):
                result = self.generator._normalize_country(country)
                self.assertEqual(result, expected)

    def test_extract_hours_time_filters(self) -> None:
        """Test time filter hour extraction for all variations."""
        test_cases = [
            ("Last 24h", "24"),
            ("Last 24 hours", "24"),
            ("Past 24h", "24"),
            ("Past 24 hours", "24"),
            ("24h", "24"),
            ("24 hours", "24"),
            ("Last 72h", "72"),
            ("Last 72 hours", "72"),
            ("Past 72h", "72"),
            ("Past 72 hours", "72"),
            ("72h", "72"),
            ("72 hours", "72"),
            ("Past Week", "168"),
            ("Last Week", "168"),
            ("1 week", "168"),
            ("Week", "168"),
            ("WEEK", "168"),
            ("Past Month", "any"),
            ("Last Month", "any"),
            ("Month", "any"),
            ("Any", "any"),
            ("", "any"),  # Empty string
            ("   ", "any"),  # Whitespace
            ("Custom Filter", "any"),  # Unknown filter
        ]

        for time_filter, expected in test_cases:
            with self.subTest(time_filter=time_filter):
                result = self.generator._extract_hours(time_filter)
                self.assertEqual(result, expected)

    def test_cache_key_case_sensitivity(self) -> None:
        """Test that cache keys are consistently lowercase."""
        test_cases: list[Dict[str, Any]] = [
            {
                "scraper": "INDEED",
                "search_term": "SOFTWARE ENGINEER",
                "country": "UNITED STATES",
                "remote": True,
                "time_filter": "LAST 24H",
            },
            {
                "scraper": "Indeed",
                "search_term": "Software Engineer",
                "country": "United States",
                "remote": True,
                "time_filter": "Last 24h",
            },
            {
                "scraper": "indeed",
                "search_term": "software engineer",
                "country": "united states",
                "remote": True,
                "time_filter": "last 24h",
            },
        ]

        expected = "indeed:usa:remote:24:software_engineer"

        for params in test_cases:
            with self.subTest(params=params):
                result = self.generator.generate_cache_key(**params)
                self.assertEqual(result, expected)

    def test_empty_and_none_parameters(self) -> None:
        """Test handling of empty and None parameters."""
        # Test with empty search term
        result = self.generator.generate_cache_key(
            scraper="indeed", search_term="", country="United States", remote=True, time_filter="Last 24h"
        )
        self.assertEqual(result, "indeed:usa:remote:24:unknown_job")

        # Test with empty country (should default to global)
        result = self.generator.generate_cache_key(
            scraper="indeed", search_term="Software Engineer", country="", remote=True, time_filter="Last 24h"
        )
        self.assertEqual(result, "indeed:global:remote:24:software_engineer")

        # Test with empty time filter (should default to any)
        result = self.generator.generate_cache_key(
            scraper="indeed", search_term="Software Engineer", country="United States", remote=True, time_filter=""
        )
        self.assertEqual(result, "indeed:usa:remote:any:software_engineer")

    def test_complex_search_terms(self) -> None:
        """Test cache key generation with complex, real-world search terms."""
        test_cases = [
            {
                "search_term": "Senior Full Stack Developer (React + Node.js)",
                "expected_base": "senior_full_stack_developer_react_nodejs",
            },
            {
                "search_term": "Machine Learning Engineer - Computer Vision",
                "expected_base": "machine_learning_engineer_computer_vision",
            },
            {
                "search_term": "DevOps Engineer / SRE (Kubernetes & AWS)",
                "expected_base": "devops_engineer_sre_kubernetes_aws",
            },
            {"search_term": "Product Manager (B2B SaaS) - Remote", "expected_base": "product_manager_b2b_saas_remote"},
            {
                "search_term": 'Data Scientist (Python, R) (remote OR "work from home" OR WFH)',
                "expected_base": "data_scientist",  # Remote keywords should be removed
            },
        ]

        for case in test_cases:
            with self.subTest(search_term=case["search_term"]):
                result = self.generator.generate_cache_key(
                    scraper="indeed",
                    search_term=case["search_term"],
                    country="United States",
                    remote=True,
                    time_filter="Last 24h",
                )
                expected = f"indeed:usa:remote:24:{case['expected_base']}"
                self.assertEqual(result, expected)

    def test_cache_key_format_structure(self) -> None:
        """Test that all generated cache keys follow the expected format."""
        test_params = [
            ("indeed", "Software Engineer", "United States", True, "Last 24h"),
            ("linkedin", "Data Scientist", "Canada", False, "Past Week"),
            ("glassdoor", "Product Manager", "Global", True, "Past Month"),
            ("ziprecruiter", "DevOps Engineer", "Brazil", False, "Last 72h"),
        ]

        for scraper, search_term, country, remote, time_filter in test_params:
            with self.subTest(params=(scraper, search_term, country, remote, time_filter)):
                result = self.generator.generate_cache_key(
                    scraper=scraper, search_term=search_term, country=country, remote=remote, time_filter=time_filter
                )

                # Check format: scraper:country_code:remote_flag:hours:base_term
                parts = result.split(":")
                self.assertEqual(len(parts), 5, f"Key should have 5 parts separated by colons: {result}")

                # Validate each part
                self.assertTrue(parts[0], "Scraper part should not be empty")
                self.assertTrue(parts[1], "Country code part should not be empty")
                self.assertIn(parts[2], ["remote", "onsite"], "Remote flag should be 'remote' or 'onsite'")
                self.assertTrue(parts[3], "Hours part should not be empty")
                self.assertTrue(parts[4], "Base term part should not be empty")


if __name__ == "__main__":
    unittest.main()
