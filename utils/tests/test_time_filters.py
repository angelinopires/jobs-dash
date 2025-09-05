"""
Unit tests for utils.time_filters.

This module contains comprehensive tests for time filter utility functions.
"""

import unittest

# Import the functions being tested
from utils.time_filters import (
    TIME_FILTERS,
    get_filter_from_hours,
    get_hours_from_filter,
    get_time_filter_options,
    is_time_filter_enabled,
)


class TestTimeFilters(unittest.TestCase):
    """Test cases for time filter utility functions."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        # Expected time filter mappings
        self.expected_filters = {
            "Last 24h": 24,
            "Last 72h": 72,
            "Past Week": 168,  # 7 days
            "Past Month": 720,  # 30 days (24 * 30)
        }

    def test_time_filters_constant_structure(self) -> None:
        """Test that TIME_FILTERS constant has expected structure."""
        # Should be a dictionary
        self.assertIsInstance(TIME_FILTERS, dict)

        # Should have expected keys
        expected_keys = ["Last 24h", "Last 72h", "Past Week", "Past Month"]
        for key in expected_keys:
            self.assertIn(key, TIME_FILTERS)

        # Should have expected values
        for key, expected_hours in self.expected_filters.items():
            self.assertEqual(TIME_FILTERS[key], expected_hours)

    def test_get_time_filter_options(self) -> None:
        """Test getting list of time filter options."""
        options = get_time_filter_options()

        # Should return a list
        self.assertIsInstance(options, list)

        # Should contain all expected options
        expected_options = ["Last 24h", "Last 72h", "Past Week", "Past Month"]
        self.assertEqual(set(options), set(expected_options))

        # Should not be empty
        self.assertGreater(len(options), 0)

    def test_get_hours_from_filter_valid_filters(self) -> None:
        """Test converting valid time filters to hours."""
        test_cases = [
            ("Last 24h", 24),
            ("Last 72h", 72),
            ("Past Week", 168),
            ("Past Month", 720),
        ]

        for filter_name, expected_hours in test_cases:
            with self.subTest(filter=filter_name):
                result = get_hours_from_filter(filter_name)
                self.assertEqual(result, expected_hours)

    def test_get_hours_from_filter_invalid_filters(self) -> None:
        """Test converting invalid time filters to hours."""
        invalid_filters = [
            "Invalid Filter",
            "",
            "Last Day",
            "Past Year",
            None,
        ]

        for invalid_filter in invalid_filters:
            with self.subTest(filter=invalid_filter):
                result = get_hours_from_filter(str(invalid_filter))
                self.assertIsNone(result)

    def test_get_hours_from_filter_edge_cases(self) -> None:
        """Test edge cases for get_hours_from_filter."""
        # Test with case sensitivity
        result = get_hours_from_filter("last 24h")
        self.assertIsNone(result)

        # Test with whitespace
        result = get_hours_from_filter("  Last 24h  ")
        self.assertIsNone(result)

        # Test with numbers
        result = get_hours_from_filter("24")
        self.assertIsNone(result)

    def test_get_filter_from_hours_valid_hours(self) -> None:
        """Test converting valid hours back to filter options."""
        test_cases = [
            (24, "Last 24h"),
            (72, "Last 72h"),
            (168, "Past Week"),
            (720, "Past Month"),
        ]

        for hours, expected_filter in test_cases:
            with self.subTest(hours=hours):
                result = get_filter_from_hours(hours)
                self.assertEqual(result, expected_filter)

    def test_get_filter_from_hours_invalid_hours(self) -> None:
        """Test converting invalid hours back to filter options."""
        invalid_hours = [
            -1,  # Negative hours
            0,  # Zero hours
            25,  # Between 24h and 72h
            100,  # Between 72h and 168h
            500,  # Between 168h and 720h
            1000,  # More than 720h
            None,  # None value
        ]

        for invalid_hour in invalid_hours:
            with self.subTest(hours=invalid_hour):
                result = get_filter_from_hours(invalid_hour)
                # Should default to "Past Month" for invalid hours
                self.assertEqual(result, "Past Month")

    def test_get_filter_from_hours_edge_cases(self) -> None:
        """Test edge cases for get_filter_from_hours."""
        # Test with None (should default to "Past Month")
        result = get_filter_from_hours(None)
        self.assertEqual(result, "Past Month")

        # Test with very large number
        result = get_filter_from_hours(999999)
        self.assertEqual(result, "Past Month")

        # Test with very small number
        result = get_filter_from_hours(-999999)
        self.assertEqual(result, "Past Month")

    def test_is_time_filter_enabled_valid_filters(self) -> None:
        """Test checking if valid time filters are enabled."""
        valid_filters = ["Last 24h", "Last 72h", "Past Week", "Past Month"]

        for filter_name in valid_filters:
            with self.subTest(filter=filter_name):
                result = is_time_filter_enabled(filter_name)
                self.assertTrue(result)

    def test_is_time_filter_enabled_invalid_filters(self) -> None:
        """Test checking if invalid time filters are enabled."""
        invalid_filters = [
            "Invalid Filter",
            "",
            "Last Day",
            "Past Year",
            None,
        ]

        for invalid_filter in invalid_filters:
            with self.subTest(filter=invalid_filter):
                result = is_time_filter_enabled(str(invalid_filter))
                self.assertFalse(result)

    def test_is_time_filter_enabled_edge_cases(self) -> None:
        """Test edge cases for is_time_filter_enabled."""
        # Test with case sensitivity
        result = is_time_filter_enabled("last 24h")
        self.assertFalse(result)

        # Test with whitespace
        result = is_time_filter_enabled("  Last 24h  ")
        self.assertFalse(result)

        # Test with numbers
        result = is_time_filter_enabled("24")
        self.assertFalse(result)

    def test_time_filter_consistency(self) -> None:
        """Test consistency between filter functions."""
        # Test that get_hours_from_filter and get_filter_from_hours are inverse operations
        for filter_name in TIME_FILTERS.keys():
            with self.subTest(filter=filter_name):
                hours = get_hours_from_filter(filter_name)
                filter_back = get_filter_from_hours(hours)
                self.assertEqual(filter_back, filter_name)

        # Test that all enabled filters return the expected hours
        for filter_name, expected_hours in TIME_FILTERS.items():
            with self.subTest(filter=filter_name):
                self.assertTrue(is_time_filter_enabled(filter_name))
                actual_hours = get_hours_from_filter(filter_name)
                self.assertEqual(actual_hours, expected_hours)


if __name__ == "__main__":
    unittest.main()
