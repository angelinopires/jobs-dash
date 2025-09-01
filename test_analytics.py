#!/usr/bin/env python3
"""
Test script for the analytics system.

This script tests the lightweight analytics system to ensure it's working correctly.
"""

import sys
from datetime import datetime
from pathlib import Path

from core.lightweight_analytics import LightweightAnalytics, log_search_quick

# Add project root to path
sys.path.append(str(Path(__file__).parent))

# Import after path setup


def test_analytics_system() -> bool:
    """Test the analytics system functionality."""
    print("🧪 Testing Analytics System...")

    try:
        # Initialize analytics
        analytics = LightweightAnalytics()
        print("✅ Analytics system initialized")

        # Test logging some searches
        print("\n📝 Logging test searches...")

        # Log some sample searches
        log_search_quick("Software Engineer", "Global", True, "indeed", "Last 24h")
        log_search_quick("Data Scientist", "Brazil", True, "indeed", "Last 72h")
        log_search_quick("Product Manager", "Global", True, "indeed", "Past Week")
        log_search_quick("Software Engineer", "Global", True, "indeed", "Last 24h")
        log_search_quick("DevOps Engineer", "Brazil", False, "indeed", "Last 24h")

        print("✅ Test searches logged")

        # Test getting daily stats
        print("\n📊 Getting daily statistics...")
        today = datetime.now().strftime("%Y-%m-%d")
        daily_stats = analytics.get_daily_stats(today)

        print(f"   Date: {daily_stats['date']}")
        print(f"   Total searches: {daily_stats['total_searches']}")
        print(f"   Job title counts: {daily_stats['job_title_counts']}")
        print(f"   Location counts: {daily_stats['location_counts']}")

        # Test getting popular searches
        print("\n🏆 Getting popular searches...")
        popular_data = analytics.get_popular_searches(days=1)

        print(f"   Period: {popular_data['period_days']} days")
        print(f"   Total searches: {popular_data['total_searches']}")
        print(f"   Popular job titles: {popular_data['popular_job_titles']}")
        print(f"   Popular locations: {popular_data['popular_locations']}")

        # Test storage stats
        print("\n💾 Getting storage statistics...")
        storage_stats = analytics.get_storage_stats()

        print(f"   Total size: {storage_stats['total_size_mb']} MB")
        print(f"   File counts: {storage_stats['file_counts']}")
        print(f"   Compression ratio: {storage_stats['compression_ratio']}")

        print("\n✅ All tests completed successfully!")

        return True

    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_analytics_system()
    sys.exit(0 if success else 1)
