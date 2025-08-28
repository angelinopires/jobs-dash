"""
Unit tests for config.countries.

This module contains comprehensive tests for country configuration functions.
"""

import unittest
from typing import Dict, Tuple, List

# Import the functions being tested
from config.countries import (
    get_country_options,
    get_indeed_country_name,
    has_glassdoor_support,
    get_country_info,
    COUNTRIES
)


class TestCountries(unittest.TestCase):
    """Test cases for country configuration functions."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Sample country data for testing
        self.sample_countries = {
            "United States": ("United States", "usa", True),
            "Canada": ("Canada", "canada", True),
            "United Kingdom": ("United Kingdom", "uk", True),
            "Germany": ("Germany", "germany", True),
            "France": ("France", "france", True),
        }
    
    def test_countries_constant_structure(self):
        """Test that COUNTRIES constant has expected structure."""
        # Should be a dictionary
        self.assertIsInstance(COUNTRIES, dict)
        
        # Should not be empty
        self.assertGreater(len(COUNTRIES), 0)
        
        # Should contain expected sample countries
        for country_name in self.sample_countries.keys():
            self.assertIn(country_name, COUNTRIES)
        
        # Each country should have tuple with 3 elements
        for country_name, country_data in COUNTRIES.items():
            self.assertIsInstance(country_data, tuple)
            self.assertEqual(len(country_data), 3)
            
            # Check tuple structure: (display_name, indeed_name, glassdoor_support)
            display_name, indeed_name, glassdoor_support = country_data
            
            # Display name should be string
            self.assertIsInstance(display_name, str)
            self.assertEqual(display_name, country_name)
            
            # Indeed name should be string
            self.assertIsInstance(indeed_name, str)
            
            # Glassdoor support should be boolean
            self.assertIsInstance(glassdoor_support, bool)
    
    def test_get_country_options(self):
        """Test getting list of country options."""
        options = get_country_options()
        
        # Should return a list
        self.assertIsInstance(options, list)
        
        # Should not be empty
        self.assertGreater(len(options), 0)
        
        # First option should be "Global"
        self.assertEqual(options[0], "Global")
        
        # Should contain all countries from COUNTRIES
        for country_name in COUNTRIES.keys():
            self.assertIn(country_name, options)
        
        # Should be sorted (excluding "Global" at first position)
        sorted_countries = sorted(COUNTRIES.keys())
        self.assertEqual(options[1:], sorted_countries)
    
    def test_get_indeed_country_name_valid_countries(self):
        """Test getting Indeed country names for valid countries."""
        test_cases = [
            ("United States", "usa"),
            ("Canada", "canada"),
            ("United Kingdom", "uk"),
            ("Germany", "germany"),
            ("France", "france"),
            ("Australia", "australia"),
            ("Japan", "japan"),
        ]
        
        for display_name, expected_indeed_name in test_cases:
            with self.subTest(country=display_name):
                result = get_indeed_country_name(display_name)
                self.assertEqual(result, expected_indeed_name)
    
    def test_get_indeed_country_name_invalid_countries(self):
        """Test getting Indeed country names for invalid countries."""
        invalid_countries = [
            "Invalid Country",
            "",
            "NonExistent",
            "Unknown",
            None,
        ]
        
        for invalid_country in invalid_countries:
            with self.subTest(country=invalid_country):
                result = get_indeed_country_name(invalid_country)
                # Should default to "usa"
                self.assertEqual(result, "usa")
    
    def test_get_indeed_country_name_edge_cases(self):
        """Test edge cases for get_indeed_country_name."""
        # Test with case sensitivity
        result = get_indeed_country_name("united states")
        self.assertEqual(result, "usa")
        
        # Test with whitespace
        result = get_indeed_country_name("  United States  ")
        self.assertEqual(result, "usa")
        
        # Test with numbers
        result = get_indeed_country_name("123")
        self.assertEqual(result, "usa")
    
    def test_has_glassdoor_support_valid_countries(self):
        """Test checking Glassdoor support for valid countries."""
        # Test countries with Glassdoor support
        glassdoor_supported = [
            "United States",
            "Canada",
            "United Kingdom",
            "Germany",
            "France",
            "Australia",
            "Netherlands",
        ]
        
        for country_name in glassdoor_supported:
            with self.subTest(country=country_name):
                result = has_glassdoor_support(country_name)
                self.assertTrue(result)
        
        # Test countries without Glassdoor support
        glassdoor_not_supported = [
            "Japan",
            "China",
            "Argentina",
            "South Africa",
        ]
        
        for country_name in glassdoor_not_supported:
            with self.subTest(country=country_name):
                result = has_glassdoor_support(country_name)
                self.assertFalse(result)
    
    def test_has_glassdoor_support_invalid_countries(self):
        """Test checking Glassdoor support for invalid countries."""
        invalid_countries = [
            "Invalid Country",
            "",
            "NonExistent",
            "Unknown",
            None,
        ]
        
        for invalid_country in invalid_countries:
            with self.subTest(country=invalid_country):
                result = has_glassdoor_support(invalid_country)
                # Should default to False
                self.assertFalse(result)
    
    def test_get_country_info_valid_countries(self):
        """Test getting full country information for valid countries."""
        test_cases = [
            ("United States", ("United States", "usa", True)),
            ("Canada", ("Canada", "canada", True)),
            ("United Kingdom", ("United Kingdom", "uk", True)),
            ("Japan", ("Japan", "japan", False)),
            ("China", ("China", "china", False)),
        ]
        
        for display_name, expected_info in test_cases:
            with self.subTest(country=display_name):
                result = get_country_info(display_name)
                self.assertEqual(result, expected_info)
                
                # Check tuple structure
                self.assertIsInstance(result, tuple)
                self.assertEqual(len(result), 3)
                
                display, indeed, glassdoor = result
                self.assertIsInstance(display, str)
                self.assertIsInstance(indeed, str)
                self.assertIsInstance(glassdoor, bool)
    
    def test_get_country_info_invalid_countries(self):
        """Test getting country information for invalid countries."""
        invalid_countries = [
            "Invalid Country",
            "",
            "NonExistent",
            "Unknown",
            None,
        ]
        
        for invalid_country in invalid_countries:
            with self.subTest(country=invalid_country):
                result = get_country_info(invalid_country)
                # Should default to ("United States", "US", False)
                self.assertEqual(result, ("United States", "US", False))
    
    def test_country_data_consistency(self):
        """Test consistency between country data functions."""
        # Test that all functions work together consistently
        for country_name in COUNTRIES.keys():
            with self.subTest(country=country_name):
                # Get country info
                display, indeed, glassdoor = get_country_info(country_name)
                
                # Should match COUNTRIES data
                self.assertEqual(display, country_name)
                self.assertEqual(indeed, get_indeed_country_name(country_name))
                self.assertEqual(glassdoor, has_glassdoor_support(country_name))
                
                # Should be in country options
                self.assertIn(country_name, get_country_options())
    
    def test_country_data_quality(self):
        """Test quality of country data."""
        for country_name, country_data in COUNTRIES.items():
            with self.subTest(country=country_name):
                display, indeed, glassdoor = country_data
                
                # Display name should not be empty
                self.assertGreater(len(display), 0)
                
                # Indeed name should not be empty
                self.assertGreater(len(indeed), 0)
                
                # Indeed name should be lowercase
                self.assertEqual(indeed, indeed.lower())
                
                # Indeed name should not contain special characters
                self.assertTrue(indeed.replace(' ', '').isalpha() or indeed.replace(' ', '').isalnum())
    
    def test_glassdoor_support_distribution(self):
        """Test distribution of Glassdoor support across countries."""
        total_countries = len(COUNTRIES)
        glassdoor_supported = sum(1 for _, _, glassdoor in COUNTRIES.values() if glassdoor)
        glassdoor_not_supported = total_countries - glassdoor_supported
        
        # Should have reasonable distribution
        self.assertGreater(glassdoor_supported, 0)
        self.assertGreater(glassdoor_not_supported, 0)
        
        # At least 20% should have Glassdoor support
        glassdoor_percentage = (glassdoor_supported / total_countries) * 100
        self.assertGreaterEqual(glassdoor_percentage, 20)
        
        # At least 50% should not have Glassdoor support
        non_glassdoor_percentage = (glassdoor_not_supported / total_countries) * 100
        self.assertGreaterEqual(non_glassdoor_percentage, 50)


if __name__ == '__main__':
    unittest.main()
