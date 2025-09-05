#!/usr/bin/env python3
"""
Rate Limiting Test Script

This script helps you test different rate limiting configurations
and see how they affect scraping performance.

Usage:
    python scripts/test_rate_limiting.py [profile]

Available profiles:
    - production (default)
    - development
    - testing
    - redis_cache_testing
"""

import argparse
import sys
import time
from typing import List

# Add project root to path
sys.path.insert(0, ".")

# Project imports (after path modification)
from config.environment import get_config_summary, set_environment_for_testing  # noqa: E402
from core.resilience.rate_limiter import get_rate_limiter  # noqa: E402


def test_rate_limiting_profile(profile: str, test_duration: int = 30) -> None:
    """
    Test a specific rate limiting profile.

    Args:
        profile: Rate limiting profile to test
        test_duration: How long to run the test (seconds)
    """
    print(f"\n{'='*60}")
    print(f"🧪 TESTING RATE LIMITING PROFILE: {profile.upper()}")
    print(f"{'='*60}")

    # Set environment for testing
    set_environment_for_testing(profile)

    # Get configuration summary
    config_summary = get_config_summary()
    print("\n📊 Configuration Summary:")
    print(f"Environment: {config_summary['environment']}")
    print(f"Base delay: {config_summary['rate_limiting']['base_delay']}s")
    print(f"Max delay: {config_summary['rate_limiting']['max_delay']}s")
    print(f"Slow multiplier: {config_summary['rate_limiting']['slow_multiplier']}x")
    print(f"Aggressive multiplier: {config_summary['rate_limiting']['aggressive_multiplier']}x")

    # Create a fresh rate limiter instance
    rate_limiter = get_rate_limiter()

    print(f"\n⏱️  Testing rate limiting behavior for {test_duration} seconds...")
    print("Making repeated API calls to observe delays...\n")

    start_time = time.time()
    call_count = 0
    delays: List[float] = []

    while time.time() - start_time < test_duration:
        # Simulate different endpoints/attempts
        endpoint = f"test_endpoint_{call_count % 3}"  # Rotate between 3 endpoints
        attempt = (call_count // 3) + 1  # Increment attempt every 3 calls

        # Calculate what delay would be applied
        calculated_delay = rate_limiter.calculate_delay(endpoint, attempt)
        delays.append(calculated_delay)

        print(f"Call {call_count + 1}: endpoint={endpoint}, attempt={attempt}, delay={calculated_delay:.3f}s")

        # Actually apply the delay
        rate_limiter.wait_if_needed(endpoint, attempt)

        call_count += 1

        # Break if delays get too long for testing
        if calculated_delay > 10:
            print(f"⚠️  Delays getting too long ({calculated_delay:.1f}s), stopping test early")
            break

    elapsed_time = time.time() - start_time

    print("\n📈 Test Results:")
    print(f"Total time: {elapsed_time:.2f}s")
    print(f"Total calls: {call_count}")
    print(f"Average delay: {sum(delays)/len(delays):.3f}s")
    print(f"Min delay: {min(delays):.3f}s")
    print(f"Max delay: {max(delays):.3f}s")
    print(f"Calls per minute: {(call_count / elapsed_time) * 60:.1f}")


def test_redis_cache_scenario() -> None:
    """Test a realistic Redis cache testing scenario."""
    print(f"\n{'='*60}")
    print("🔧 REDIS CACHE TESTING SCENARIO")
    print(f"{'='*60}")

    # Use ultra-fast configuration for cache testing
    set_environment_for_testing("redis_cache_testing")

    print("Simulating Redis cache testing workflow:")
    print("1. Cache miss → API call")
    print("2. Cache hit → No API call")
    print("3. Cache miss → API call")
    print("4. Multiple cache hits")

    start_time = time.time()

    # Simulate cache miss scenarios (these would trigger rate limiting)
    print("\n⏱️  Cache miss simulations:")
    for i in range(3):
        call_start = time.time()
        rate_limiter = get_rate_limiter()
        rate_limiter.wait_if_needed(f"indeed_api_test_{i}")
        delay = time.time() - call_start
        print(f"API call {i+1}: {delay:.3f}s delay")

    total_time = time.time() - start_time
    print(f"\nTotal time for 3 API calls: {total_time:.3f}s")
    print(f"Average time per call: {total_time/3:.3f}s")

    if total_time < 2.0:
        print("✅ Fast enough for Redis cache testing!")
    else:
        print("⚠️  Still too slow for efficient cache testing")


def compare_all_profiles() -> None:
    """Compare delay calculations across all profiles."""
    print(f"\n{'='*60}")
    print("📊 RATE LIMITING PROFILE COMPARISON")
    print(f"{'='*60}")

    profiles = ["production", "development", "testing", "redis_cache_testing"]

    print(f"{'Profile':<20} {'Base':<8} {'Max':<8} {'Attempt 1':<12} {'Attempt 5':<12} {'Aggressive':<12}")
    print("-" * 80)

    for profile in profiles:
        set_environment_for_testing(profile)
        rate_limiter = get_rate_limiter()

        # Get config details
        config = rate_limiter.config

        # Calculate delays for different scenarios
        delay_attempt_1 = rate_limiter.calculate_delay("test", attempt=1)
        delay_attempt_5 = rate_limiter.calculate_delay("test", attempt=5)

        # Simulate aggressive state
        stats = rate_limiter.get_endpoint_stats("test_aggressive")
        # Force aggressive state by adding slow response times
        for _ in range(5):
            stats.update_response_time(15.0)  # Slow response
        delay_aggressive = rate_limiter.calculate_delay("test_aggressive", attempt=1)

        print(
            f"{profile:<20} {config.base_delay:<8.2f} {config.max_delay:<8.1f} "
            f"{delay_attempt_1:<12.3f} {delay_attempt_5:<12.3f} {delay_aggressive:<12.3f}"
        )


def main() -> None:
    """Main function with argument parsing."""
    parser = argparse.ArgumentParser(
        description="Test rate limiting configurations for Jobs Dashboard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Test development profile
    python scripts/test_rate_limiting.py development

    # Test Redis cache testing profile
    python scripts/test_rate_limiting.py redis_cache_testing

    # Compare all profiles
    python scripts/test_rate_limiting.py --compare

    # Redis cache testing scenario
    python scripts/test_rate_limiting.py --redis-scenario
        """,
    )

    parser.add_argument(
        "profile",
        nargs="?",
        choices=["production", "development", "testing", "redis_cache_testing"],
        default="production",
        help="Rate limiting profile to test",
    )

    parser.add_argument("--compare", action="store_true", help="Compare all rate limiting profiles")

    parser.add_argument("--redis-scenario", action="store_true", help="Run Redis cache testing scenario")

    parser.add_argument("--duration", type=int, default=30, help="Test duration in seconds (default: 30)")

    args = parser.parse_args()

    print("🚀 Jobs Dashboard Rate Limiting Test Tool")

    if args.compare:
        compare_all_profiles()
    elif args.redis_scenario:
        test_redis_cache_scenario()
    else:
        test_rate_limiting_profile(args.profile, args.duration)

    print("\n✅ Testing complete!")


if __name__ == "__main__":
    main()
