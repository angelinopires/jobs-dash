"""
Unit tests for core optimization features.

Tests the essential functionality of the new architecture:
- Cache management (session + file backup)
- Performance monitoring 
- Search optimization
- Result processing
"""

import unittest
import tempfile
import shutil
import os
import pandas as pd
from datetime import datetime, timedelta
import time

# Import the core modules to test
from core.cache_manager import CacheManager
from core.performance_monitor import PerformanceMonitor
from core.base_optimizer import SearchOptimizer


class TestCacheManager(unittest.TestCase):
    """Test the hybrid caching system."""
    
    def setUp(self):
        """Set up test environment with temporary cache directory."""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_manager = CacheManager(cache_ttl_minutes=1, cache_dir=self.temp_dir)
    
    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.temp_dir)
    
    def test_cache_key_generation(self):
        """Test that cache keys are generated consistently."""
        key1 = self.cache_manager.generate_cache_key(
            scraper="indeed",
            search_term="Software Engineer",
            country="United States", 
            include_remote=True
        )
        
        key2 = self.cache_manager.generate_cache_key(
            scraper="indeed",
            search_term="Software Engineer",
            country="United States",
            include_remote=True
        )
        
        # Same parameters should generate same key
        self.assertEqual(key1, key2)
        
        # Different parameters should generate different keys
        key3 = self.cache_manager.generate_cache_key(
            scraper="indeed",
            search_term="Data Scientist",
            country="United States",
            include_remote=True
        )
        
        self.assertNotEqual(key1, key3)
        
        # Test that time_filter affects cache key
        key4 = self.cache_manager.generate_cache_key(
            scraper="indeed",
            search_term="Software Engineer",
            country="United States",
            include_remote=True,
            time_filter="Last 24h"
        )
        
        key5 = self.cache_manager.generate_cache_key(
            scraper="indeed",
            search_term="Software Engineer",
            country="United States",
            include_remote=True,
            time_filter="Last 7 days"
        )
        
        # Different time filters should generate different keys
        self.assertNotEqual(key4, key5)
        self.assertNotEqual(key1, key4)  # Should be different from key without time_filter
    
    def test_cache_storage_and_retrieval(self):
        """Test storing and retrieving cached results."""
        # Create test result
        test_result = {
            "success": True,
            "jobs": pd.DataFrame([{"title": "Test Job", "company": "Test Co"}]),
            "count": 1,
            "message": "Test result"
        }
        
        cache_key = "test_key_123"
        
        # Store result
        self.cache_manager.cache_result(cache_key, test_result)
        
        # Retrieve result
        retrieved = self.cache_manager.get_cached_result(cache_key)
        
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved["success"], True)
        self.assertEqual(retrieved["count"], 1)
        self.assertIsInstance(retrieved["jobs"], pd.DataFrame)
    
    def test_cache_expiration(self):
        """Test that cache entries expire after TTL."""
        # Use very short TTL for testing
        short_cache = CacheManager(cache_ttl_minutes=0.01, cache_dir=self.temp_dir)  # 0.6 seconds
        
        test_result = {"success": True, "count": 1}
        cache_key = "expire_test"
        
        # Store result
        short_cache.cache_result(cache_key, test_result)
        
        # Should be available immediately
        retrieved = short_cache.get_cached_result(cache_key)
        self.assertIsNotNone(retrieved)
        
        # Wait for expiration
        time.sleep(1)
        
        # Should be expired now
        retrieved_after = short_cache.get_cached_result(cache_key)
        self.assertIsNone(retrieved_after)


class TestPerformanceMonitor(unittest.TestCase):
    """Test the performance monitoring system."""
    
    def setUp(self):
        """Set up performance monitor for testing."""
        self.monitor = PerformanceMonitor("test_scraper")
    
    def test_search_tracking(self):
        """Test search start/end tracking."""
        # Start a search
        self.monitor.start_search("Test Job", "United States", True)
        
        # Log some events
        self.monitor.log("Test event", "Test message")
        self.monitor.log_cache_event("hit", "test_cache_key", "United States")
        
        # End search
        self.monitor.end_search(True, 2.5, 10)
        
        # Check stats
        stats = self.monitor.get_stats()
        self.assertEqual(stats["total_searches"], 1)
        self.assertEqual(stats["success_rate"], 100.0)
        self.assertEqual(stats["total_jobs_found"], 10)
    
    def test_performance_stats(self):
        """Test performance statistics calculation."""
        # Simulate multiple searches
        searches = [
            (True, 1.5, 5),   # Successful
            (True, 2.0, 8),   # Successful  
            (False, 3.0, 0),  # Failed
        ]
        
        for success, time_taken, job_count in searches:
            self.monitor.start_search("Test", "US", True)
            self.monitor.end_search(success, time_taken, job_count)
        
        stats = self.monitor.get_stats()
        
        self.assertEqual(stats["total_searches"], 3)
        self.assertEqual(stats["successful_searches"], 2)
        self.assertAlmostEqual(stats["success_rate"], 66.7, places=1)
        self.assertEqual(stats["total_jobs_found"], 13)  # 5 + 8 + 0
        self.assertAlmostEqual(stats["avg_time"], 2.17, places=1)  # (1.5 + 2.0 + 3.0) / 3


class TestSearchOptimizer(unittest.TestCase):
    """Test the search optimization functionality."""
    
    def setUp(self):
        """Set up search optimizer for testing."""
        self.optimizer = SearchOptimizer("test_scraper")
    
    def test_search_param_optimization(self):
        """Test search parameter optimization."""
        # Test global search optimization
        global_params = {
            "search_term": "  Software Engineer  ",  # Extra whitespace
            "where": "Global",
            "results_wanted": 2000  # High number
        }
        
        optimized = self.optimizer.optimize_search_params(**global_params)
        
        # Should trim whitespace
        self.assertEqual(optimized["search_term"], "Software Engineer")
        
        # Should limit results for global searches
        self.assertLessEqual(optimized["results_wanted"], 500)
        
        # Test single country optimization
        country_params = {
            "search_term": "Data Scientist",
            "where": "United States", 
            "results_wanted": 800
        }
        
        optimized_country = self.optimizer.optimize_search_params(**country_params)
        
        # Should keep higher limit for single country
        self.assertEqual(optimized_country["results_wanted"], 800)
    
    def test_result_processing_optimization(self):
        """Test result processing optimization."""
        # Create test DataFrame
        test_jobs = pd.DataFrame([
            {"title": "Job 1", "company": "Company A", "date_posted": "2023-12-01"},
            {"title": "Job 2", "company": "Company B", "date_posted": "2023-12-02"},
            {"title": "Job 3", "company": "Company A", "date_posted": "2023-12-03"},
        ])
        
        optimized_jobs = self.optimizer.optimize_result_processing(test_jobs)
        
        # Should return a DataFrame
        self.assertIsInstance(optimized_jobs, pd.DataFrame)
        
        # Should have same number of rows
        self.assertEqual(len(optimized_jobs), 3)
        
        # Should have optimized dtypes (company remains object for compatibility)
        self.assertEqual(optimized_jobs['company'].dtype.name, 'object')
    
    def test_memory_optimization(self):
        """Test memory optimization for large datasets."""
        # Create list of test DataFrames
        jobs_list = [
            pd.DataFrame([{"title": f"Job {i}", "company": "Company A"} for i in range(10)]),
            pd.DataFrame([{"title": f"Job {i+10}", "company": "Company B"} for i in range(5)]),
            pd.DataFrame(),  # Empty DataFrame (should be filtered out)
        ]
        
        combined = self.optimizer.optimize_memory_usage(jobs_list)
        
        # Should combine non-empty DataFrames
        self.assertEqual(len(combined), 15)  # 10 + 5, empty one filtered out
        
        # Should optimize dtypes (company remains object for compatibility)
        self.assertEqual(combined['company'].dtype.name, 'object')
    
    def test_duplicate_removal(self):
        """Test optimized duplicate removal."""
        # Create DataFrame with duplicates
        test_jobs = pd.DataFrame([
            {"title": "Job 1", "job_url": "http://example.com/job1", "company": "Company A"},
            {"title": "Job 2", "job_url": "http://example.com/job2", "company": "Company B"},
            {"title": "Job 1 Duplicate", "job_url": "http://example.com/job1", "company": "Company A"},  # Duplicate URL
        ])
        
        deduped = self.optimizer.optimize_duplicate_removal(test_jobs, ['job_url'])
        
        # Should remove duplicate
        self.assertEqual(len(deduped), 2)
        
        # Should keep first occurrence
        self.assertEqual(deduped.iloc[0]['title'], "Job 1")


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)
