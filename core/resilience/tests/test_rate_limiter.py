"""
Unit Tests for Intelligent Rate Limiter Implementation

This module tests the intelligent rate limiter to ensure proper:
- Exponential backoff calculation
- Jitter implementation
- Response time adaptation
- Per-endpoint tracking
- State transitions
"""

import unittest

from ..rate_limiter import EndpointStats, IntelligentRateLimiter, RateLimitConfig, RateLimitState


class TestRateLimitConfig(unittest.TestCase):
    """Test cases for RateLimitConfig class"""

    def test_valid_configuration(self) -> None:
        """Test valid configuration creation"""
        config = RateLimitConfig(base_delay=2.0, max_delay=60.0)
        self.assertEqual(config.base_delay, 2.0)
        self.assertEqual(config.max_delay, 60.0)

    def test_invalid_base_delay(self) -> None:
        """Test negative base delay validation"""
        with self.assertRaises(ValueError):
            RateLimitConfig(base_delay=-1.0, max_delay=60.0)

    def test_invalid_max_delay(self) -> None:
        """Test max delay less than base delay"""
        with self.assertRaises(ValueError):
            RateLimitConfig(base_delay=10.0, max_delay=5.0)


class TestEndpointStats(unittest.TestCase):
    """Test cases for EndpointStats class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.stats = EndpointStats("test_endpoint")

    def test_initial_state(self) -> None:
        """Test initial statistics state"""
        self.assertEqual(self.stats.endpoint, "test_endpoint")
        self.assertEqual(self.stats.call_count, 0)
        self.assertEqual(self.stats.average_response_time, 0.0)
        self.assertEqual(self.stats.state, RateLimitState.NORMAL)

    def test_update_response_time_fast(self) -> None:
        """Test updating with fast response time"""
        self.stats.update_response_time(1.0)

        self.assertEqual(self.stats.call_count, 1)
        self.assertEqual(self.stats.average_response_time, 1.0)
        self.assertEqual(self.stats.consecutive_fast_calls, 1)
        self.assertEqual(self.stats.consecutive_slow_calls, 0)
        self.assertEqual(self.stats.state, RateLimitState.NORMAL)

    def test_update_response_time_slow(self) -> None:
        """Test updating with slow response time"""
        self.stats.update_response_time(6.0)

        self.assertEqual(self.stats.call_count, 1)
        self.assertEqual(self.stats.average_response_time, 6.0)
        self.assertEqual(self.stats.consecutive_fast_calls, 0)
        self.assertEqual(self.stats.consecutive_slow_calls, 1)
        self.assertEqual(self.stats.state, RateLimitState.SLOW)


class TestIntelligentRateLimiter(unittest.TestCase):
    """Test cases for IntelligentRateLimiter class"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.config = RateLimitConfig(base_delay=1.0, max_delay=10.0)
        self.rate_limiter = IntelligentRateLimiter(self.config)

    def test_initialization(self) -> None:
        """Test rate limiter initialization"""
        self.assertEqual(self.rate_limiter.config.base_delay, 1.0)
        self.assertEqual(self.rate_limiter.config.max_delay, 10.0)

    def test_get_endpoint_stats_creation(self) -> None:
        """Test creating endpoint statistics"""
        stats = self.rate_limiter.get_endpoint_stats("test_endpoint")
        self.assertIsInstance(stats, EndpointStats)
        self.assertEqual(stats.endpoint, "test_endpoint")

    def test_calculate_delay_normal_state(self) -> None:
        """Test delay calculation in normal state"""
        delay = self.rate_limiter.calculate_delay("test_endpoint", attempt=1)
        # Should be base_delay with jitter
        self.assertGreaterEqual(delay, 0.8)  # 1.0 * 0.8 (jitter)
        self.assertLessEqual(delay, 1.2)  # 1.0 * 1.2 (jitter)

    def test_calculate_delay_exponential_backoff(self) -> None:
        """Test exponential backoff delay calculation"""
        delay1 = self.rate_limiter.calculate_delay("test_endpoint", attempt=1)
        delay2 = self.rate_limiter.calculate_delay("test_endpoint", attempt=2)
        delay3 = self.rate_limiter.calculate_delay("test_endpoint", attempt=3)

        # Each attempt should have higher base delay
        self.assertLess(delay1, delay2)
        self.assertLess(delay2, delay3)

    def test_call_with_rate_limiting_success(self) -> None:
        """Test successful function call with rate limiting"""

        def test_func() -> str:
            return "success"

        result = self.rate_limiter.call_with_rate_limiting(test_func, "test_endpoint")
        self.assertEqual(result, "success")

        # Check that stats were updated
        stats = self.rate_limiter.get_endpoint_stats("test_endpoint")
        self.assertEqual(stats.call_count, 1)
        self.assertGreater(stats.average_response_time, 0)


if __name__ == "__main__":
    unittest.main()
