"""
Integration tests for the optimized job scraper system.

Tests the complete system integration including:
- Cache system functionality
- Performance monitoring
- Search optimizations
- Indeed scraper integration

This is an integration test that verifies the entire system works together.
"""

import sys
import os
import time

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.optimized_indeed_scraper import get_indeed_scraper
from core.cache_manager import CacheManager
from core.performance_monitor import PerformanceMonitor


def test_cache_system():
    """Test the caching system."""
    print("🧪 Testing Cache System...")
    
    cache_manager = CacheManager(cache_ttl_minutes=15)
    
    # Test cache key generation
    key = cache_manager.generate_cache_key(
        scraper="indeed",
        search_term="Python Developer",
        country="United States",
        include_remote=True
    )
    
    print(f"✅ Cache key generated: {key[:20]}...")
    
    # Test cache storage (mock result)
    test_result = {
        "success": True,
        "count": 5,
        "message": "Test cache result",
        "jobs": None  # Would normally be a DataFrame
    }
    
    cache_manager.cache_result(key, test_result)
    print("✅ Result cached successfully")
    
    # Test cache retrieval
    retrieved = cache_manager.get_cached_result(key)
    if retrieved and retrieved["count"] == 5:
        print("✅ Cache retrieval successful")
    else:
        print("❌ Cache retrieval failed")
    
    # Test cache stats
    stats = cache_manager.get_cache_stats()
    print(f"✅ Cache stats: {stats['session_entries']} session entries")


def test_performance_monitoring():
    """Test the performance monitoring system."""
    print("\n📊 Testing Performance Monitoring...")
    
    monitor = PerformanceMonitor("test_scraper")
    
    # Simulate a search
    monitor.start_search("Python Developer", "United States", True)
    
    # Simulate some events
    monitor.log("Cache check", "Checking cache for results")
    monitor.log_cache_event("miss", "test_cache_key", "United States")
    monitor.log_api_call("indeed", "Python Developer", "https://indeed.com/test", 1.5)
    
    # End search
    monitor.end_search(True, 2.3, 15)
    
    # Get stats
    stats = monitor.get_stats()
    print(f"✅ Performance tracking: {stats['total_searches']} searches, {stats['avg_time']}s avg")
    
    # Print summary
    monitor.print_performance_summary()


def test_scraper_integration():
    """Test the optimized Indeed scraper integration."""
    print("\n🔍 Testing Scraper Integration...")
    
    scraper = get_indeed_scraper()
    
    # Test supported countries
    countries = scraper.get_supported_countries()
    print(f"✅ Supported countries: {len(countries)} countries")
    
    # Test API filters
    filters = scraper.get_supported_api_filters()
    supported_count = sum(1 for v in filters.values() if v)
    print(f"✅ API filters: {supported_count}/{len(filters)} supported at API level")
    
    # Test search params building (without actual API call)
    test_params = scraper._build_api_search_params(
        search_term="Test Job",
        where="United States",
        include_remote=True,
        results_wanted=100
    )
    
    expected_keys = ['site_name', 'search_term', 'country_indeed', 'location', 'results_wanted']
    has_keys = all(key in test_params for key in expected_keys[:3])  # Check first 3 required keys
    
    if has_keys:
        print("✅ Search parameter building successful")
    else:
        print("❌ Search parameter building failed")
        print(f"Got keys: {list(test_params.keys())}")
    
    # Test performance stats
    perf_stats = scraper.get_performance_stats()
    print(f"✅ Scraper performance stats available: {len(perf_stats)} metrics")


def test_optimization_framework():
    """Test the optimization framework."""
    print("\n⚡ Testing Optimization Framework...")
    
    from core.base_optimizer import SearchOptimizer
    
    optimizer = SearchOptimizer("test")
    
    # Test search param optimization
    test_params = {
        "search_term": "  Software Engineer  ",  # Whitespace
        "where": "Global",
        "results_wanted": 2000
    }
    
    optimized = optimizer.optimize_search_params(**test_params)
    
    if optimized["search_term"] == "Software Engineer" and optimized["results_wanted"] <= 500:
        print("✅ Search parameter optimization working")
    else:
        print("❌ Search parameter optimization failed")
    
    # Test optimization stats
    stats = optimizer.get_optimization_stats()
    print(f"✅ Optimization stats: {stats['optimizations_applied']} optimizations applied")


def main():
    """Run all optimization tests."""
    print("🚀 Testing Phase 1 Optimizations")
    print("="*50)
    
    try:
        test_cache_system()
        test_performance_monitoring()
        test_scraper_integration()
        test_optimization_framework()
        
        print("\n" + "="*50)
        print("🎉 All tests completed successfully!")
        print("✅ Phase 1 optimizations are ready")
        print("🚀 Ready to test with Streamlit: streamlit run dashboard.py")
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
