#!/usr/bin/env python3
"""
Redis Cache Clearer
Safely clears all cached job search results from Redis.

How to use: python cache_improvements/clear_redis_cache.py
"""

import os
import sys
from typing import List, Optional, cast

import redis

from core.redis.redis_manager import RedisManager

# Add project root to Python path so we can import core modules
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def clear_redis_cache(confirm: bool = False) -> None:
    """
    Clear all Redis cache entries.

    Args:
        confirm: If True, skip confirmation prompt
    """
    print("🗑️  Redis Cache Clearer")
    print("=" * 30)

    # Initialize Redis manager
    redis_manager = RedisManager()

    # Check Redis health
    if not redis_manager.is_healthy():
        print("❌ Redis is not healthy or not accessible")
        print("Make sure Redis is running: docker-compose up -d redis")
        return

    print("✅ Redis connection successful")
    print()

    try:
        # Get Redis client
        redis_client: Optional[redis.Redis] = redis_manager.get_client()
        if not redis_client:
            print("❌ Cannot get Redis client")
            return

        # Get all keys
        all_keys: List[str] = cast(List[str], redis_client.keys("*"))

        if not all_keys:
            print("📭 No cache entries found - Redis is already empty")
            return

        print(f"📋 Found {len(all_keys)} cache entries:")
        for key in all_keys:
            print(f"   • {key}")

        print()

        # Confirmation prompt
        if not confirm:
            response = input("⚠️  Are you sure you want to clear ALL cache entries? (yes/no): ").lower().strip()
            if response not in ["yes", "y"]:
                print("❌ Cache clearing cancelled")
                return

        # Clear all keys
        print("🗑️  Clearing cache entries...")
        deleted_count = redis_client.delete(*all_keys)

        print(f"✅ Successfully cleared {deleted_count} cache entries")
        print("🎯 Redis cache is now empty")

        # Verify cache is empty
        remaining_keys: List[str] = cast(List[str], redis_client.keys("*"))
        if not remaining_keys:
            print("✅ Verification: Cache is completely empty")
        else:
            print(f"⚠️  Warning: {len(remaining_keys)} keys still remain")

    except Exception as e:
        print(f"❌ Error clearing cache: {e}")


def clear_specific_keys(keys: List[str]) -> None:
    """
    Clear specific cache keys.

    Args:
        keys: List of keys to clear
    """
    print("🎯 Selective Cache Clearer")
    print("=" * 30)

    redis_manager = RedisManager()

    if not redis_manager.is_healthy():
        print("❌ Redis is not healthy or not accessible")
        return

    redis_client: Optional[redis.Redis] = redis_manager.get_client()
    if not redis_client:
        print("❌ Cannot get Redis client")
        return

    print(f"📋 Clearing {len(keys)} specific keys:")
    for key in keys:
        print(f"   • {key}")

    print()

    # Clear specific keys
    deleted_count = redis_client.delete(*keys)
    print(f"✅ Successfully cleared {deleted_count} keys")


def main() -> None:
    """Main function."""
    if len(sys.argv) > 1:
        if sys.argv[1] == "--force":
            clear_redis_cache(confirm=True)
        elif sys.argv[1] == "--help":
            print("Usage:")
            print("  python clear_redis_cache.py           # Clear all with confirmation")
            print("  python clear_redis_cache.py --force   # Clear all without confirmation")
            print("  python clear_redis_cache.py --help    # Show this help")
        else:
            # Treat as specific keys to clear
            keys = sys.argv[1:]
            clear_specific_keys(keys)
    else:
        clear_redis_cache()


if __name__ == "__main__":
    main()
