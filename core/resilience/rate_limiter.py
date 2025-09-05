"""
Intelligent Rate Limiter Implementation

This module implements sophisticated rate limiting with:
- Exponential backoff algorithm (1.5^attempt seconds, capped at 10s)
- Jitter implementation (random delays 0.8-1.2x multiplier)
- Response time adaptation (1.2-1.4x delay when API is slow)
- Per-API endpoint tracking
- Base delay: 1.5-2 seconds with random jitter
"""

import logging
import random
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional

# Set up logging
logger = logging.getLogger(__name__)


class RateLimitState(Enum):
    """
    Rate Limiter States

    Think of this like React loading states:
    - NORMAL: Standard rate limiting (like "idle" state)
    - SLOW: API is responding slowly (like "loading" state)
    - AGGRESSIVE: API is very slow (like "error" state)
    """

    NORMAL = "NORMAL"
    SLOW = "SLOW"
    AGGRESSIVE = "AGGRESSIVE"


@dataclass
class RateLimitConfig:
    """
    Rate Limiter Configuration

    This dataclass holds all rate limiting settings.
    Think of it like a TypeScript interface for rate limiting props.
    """

    base_delay: float = 1.5  # Base delay in seconds
    max_delay: float = 10.0  # Maximum delay in seconds
    jitter_factor: float = 0.2  # Jitter range (0.8-1.2x multiplier)
    slow_response_threshold: float = 8.0  # Response time threshold for "slow" state
    aggressive_response_threshold: float = 15.0  # Response time threshold for "aggressive" state
    slow_multiplier: float = 1.2  # Delay multiplier when API is slow
    aggressive_multiplier: float = 1.4  # Delay multiplier when API is very slow

    def __post_init__(self) -> None:
        """Validate configuration after initialization"""
        if self.base_delay < 0:
            raise ValueError("Base delay must be non-negative")
        if self.max_delay < self.base_delay:
            raise ValueError("Max delay must be greater than base delay")
        if not 0 <= self.jitter_factor <= 1:
            raise ValueError("Jitter factor must be between 0 and 1")
        if self.slow_multiplier < 1:
            raise ValueError("Slow multiplier must be at least 1")
        if self.aggressive_multiplier < self.slow_multiplier:
            raise ValueError("Aggressive multiplier must be greater than slow multiplier")


@dataclass
class EndpointStats:
    """
    Statistics for a specific API endpoint

    This tracks performance metrics for adaptive rate limiting.
    Like monitoring API performance in a React app.
    """

    endpoint: str
    call_count: int = 0
    total_response_time: float = 0.0
    average_response_time: float = 0.0
    last_call_time: float = 0.0
    consecutive_slow_calls: int = 0
    consecutive_fast_calls: int = 0
    state: RateLimitState = RateLimitState.NORMAL

    def update_response_time(self, response_time: float) -> None:
        """
        Update response time statistics

        Args:
            response_time: Time taken for the API call
        """
        self.call_count += 1
        self.total_response_time += response_time
        self.average_response_time = self.total_response_time / self.call_count
        self.last_call_time = time.time()

        # Update consecutive counters
        if response_time > 5.0:  # Slow call
            self.consecutive_slow_calls += 1
            self.consecutive_fast_calls = 0
        else:  # Fast call
            self.consecutive_fast_calls += 1
            self.consecutive_slow_calls = 0

        # Update state based on recent performance
        self._update_state()

    def _update_state(self) -> None:
        """Update rate limit state based on recent performance"""
        if self.consecutive_slow_calls >= 3:
            self.state = RateLimitState.AGGRESSIVE
        elif self.consecutive_slow_calls >= 1 or self.average_response_time > 5.0:
            self.state = RateLimitState.SLOW
        elif self.consecutive_fast_calls >= 2:
            self.state = RateLimitState.NORMAL


class IntelligentRateLimiter:
    """
    Intelligent Rate Limiter with adaptive behavior

    This class implements sophisticated rate limiting that adapts to API performance.
    Similar to implementing smart retry logic in a React app.
    """

    def __init__(self, config: Optional[RateLimitConfig] = None):
        """
        Initialize the rate limiter

        Args:
            config: Rate limiting configuration
        """
        self.config = config or RateLimitConfig()
        self._lock = threading.Lock()  # Thread safety
        self._endpoints: Dict[str, EndpointStats] = {}
        self._attempt_counts: Dict[str, int] = {}
        self._last_call_times: Dict[str, float] = {}

        logger.info(f"Intelligent rate limiter initialized with base_delay={self.config.base_delay}s")

    def get_endpoint_stats(self, endpoint: str) -> EndpointStats:
        """
        Get or create statistics for an endpoint

        Args:
            endpoint: API endpoint identifier

        Returns:
            EndpointStats: Statistics for the endpoint
        """
        with self._lock:
            if endpoint not in self._endpoints:
                self._endpoints[endpoint] = EndpointStats(endpoint)
            return self._endpoints[endpoint]

    def calculate_delay(self, endpoint: str, attempt: int = 1) -> float:
        """
        Calculate delay for an endpoint based on current state and attempt count

        Args:
            endpoint: API endpoint identifier
            attempt: Current attempt number (for exponential backoff)

        Returns:
            float: Delay in seconds
        """
        stats = self.get_endpoint_stats(endpoint)

        # Base delay with exponential backoff
        base_delay = self.config.base_delay * (2 ** (attempt - 1))
        base_delay = min(base_delay, self.config.max_delay)

        # Apply state-based multiplier
        if stats.state == RateLimitState.AGGRESSIVE:
            base_delay *= self.config.aggressive_multiplier
        elif stats.state == RateLimitState.SLOW:
            base_delay *= self.config.slow_multiplier

        # Apply jitter (random variation to prevent thundering herd)
        jitter_range = base_delay * self.config.jitter_factor
        jitter = random.uniform(-jitter_range, jitter_range)
        final_delay = max(0, base_delay + jitter)

        logger.debug(
            f"Rate limit delay for {endpoint}: "
            f"base={base_delay:.2f}s, state={stats.state.value}, "
            f"jitter={jitter:.2f}s, final={final_delay:.2f}s"
        )

        return float(final_delay)

    def wait_if_needed(self, endpoint: str, attempt: int = 1) -> None:
        """
        Wait if rate limiting is needed for an endpoint

        Args:
            endpoint: API endpoint identifier
            attempt: Current attempt number
        """
        current_time = time.time()
        last_call_time = self._last_call_times.get(endpoint, 0)

        # Calculate required delay
        required_delay = self.calculate_delay(endpoint, attempt)

        # Check if we need to wait
        time_since_last = current_time - last_call_time
        if time_since_last < required_delay:
            sleep_time = required_delay - time_since_last
            logger.info(f"Rate limiting {endpoint}: waiting {sleep_time:.2f}s")
            time.sleep(sleep_time)

        # Update last call time
        self._last_call_times[endpoint] = time.time()

    def record_response_time(self, endpoint: str, response_time: float) -> None:
        """
        Record response time for an endpoint to update adaptive behavior

        Args:
            endpoint: API endpoint identifier
            response_time: Time taken for the API call
        """
        stats = self.get_endpoint_stats(endpoint)
        old_state = stats.state

        stats.update_response_time(response_time)

        # Log state changes
        if stats.state != old_state:
            logger.info(
                f"Rate limiter state change for {endpoint}: "
                f"{old_state.value} → {stats.state.value} "
                f"(avg_response_time={stats.average_response_time:.2f}s)"
            )

    def call_with_rate_limiting(self, func: Callable[..., Any], endpoint: str, *args: Any, **kwargs: Any) -> Any:
        """
        Call a function with intelligent rate limiting and user feedback

        Args:
            func: Function to call
            endpoint: API endpoint identifier
            *args: Function arguments
            **kwargs: Function keyword arguments (may include progress_callback)

        Returns:
            Any: Function result
        """
        # Extract progress callback for UX feedback
        progress_callback = kwargs.pop("progress_callback", None)

        # Get attempt count
        with self._lock:
            attempt = self._attempt_counts.get(endpoint, 0) + 1
            self._attempt_counts[endpoint] = attempt

        # Calculate delay and provide user feedback
        delay = self.calculate_delay(endpoint, attempt)
        stats = self.get_endpoint_stats(endpoint)

        if progress_callback and delay > 0:
            # Provide user feedback about rate limiting
            if stats.state == RateLimitState.AGGRESSIVE:
                progress_callback(f"API is responding slowly, waiting {delay:.1f}s...")
            elif stats.state == RateLimitState.SLOW:
                progress_callback(f"API is a bit slow, waiting {delay:.1f}s...")
            else:
                progress_callback(f"Rate limiting: waiting {delay:.1f}s...")

        # Wait if rate limiting is needed
        self.wait_if_needed(endpoint, attempt)

        # Make the API call and measure response time
        start_time = time.time()
        try:
            if progress_callback:
                progress_callback("Making API request...")

            result = func(*args, **kwargs)
            response_time = time.time() - start_time

            # Record successful response time
            self.record_response_time(endpoint, response_time)

            # Reset attempt count on success
            with self._lock:
                self._attempt_counts[endpoint] = 0

            if progress_callback:
                progress_callback(f"Request completed in {response_time:.1f}s")

            return result

        except Exception as e:
            response_time = time.time() - start_time

            # Record failed response time (still useful for rate limiting)
            self.record_response_time(endpoint, response_time)

            # Don't reset attempt count on failure (for exponential backoff)
            logger.warning(f"API call failed for {endpoint}: {type(e).__name__}: {e}")

            if progress_callback:
                progress_callback(f"Request failed: {type(e).__name__}")

            raise

    def get_endpoint_status(self, endpoint: str) -> Dict[str, Any]:
        """
        Get status information for an endpoint

        Args:
            endpoint: API endpoint identifier

        Returns:
            dict: Status information
        """
        stats = self.get_endpoint_stats(endpoint)
        attempt = self._attempt_counts.get(endpoint, 0)

        return {
            "endpoint": endpoint,
            "state": stats.state.value,
            "call_count": stats.call_count,
            "average_response_time": stats.average_response_time,
            "consecutive_slow_calls": stats.consecutive_slow_calls,
            "consecutive_fast_calls": stats.consecutive_fast_calls,
            "current_attempt": attempt,
            "last_call_time": stats.last_call_time,
            "time_since_last_call": time.time() - stats.last_call_time if stats.last_call_time else None,
        }

    def get_all_endpoints_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get status for all endpoints

        Returns:
            dict: Status for all endpoints
        """
        return {endpoint: self.get_endpoint_status(endpoint) for endpoint in self._endpoints.keys()}

    def reset_endpoint(self, endpoint: str) -> None:
        """
        Reset statistics for an endpoint

        Args:
            endpoint: API endpoint identifier
        """
        with self._lock:
            if endpoint in self._endpoints:
                self._endpoints[endpoint] = EndpointStats(endpoint)
            if endpoint in self._attempt_counts:
                self._attempt_counts[endpoint] = 0
            if endpoint in self._last_call_times:
                del self._last_call_times[endpoint]

        logger.info(f"Reset rate limiter statistics for {endpoint}")

    def reset_all(self) -> None:
        """Reset all rate limiter statistics"""
        with self._lock:
            self._endpoints.clear()
            self._attempt_counts.clear()
            self._last_call_times.clear()

        logger.info("Reset all rate limiter statistics")


# Global rate limiter instance
_rate_limiter: Optional[IntelligentRateLimiter] = None


def get_rate_limiter(config: Optional[RateLimitConfig] = None) -> IntelligentRateLimiter:
    """
    Get the global rate limiter instance

    Args:
        config: Optional configuration override

    Returns:
        IntelligentRateLimiter: Global rate limiter instance
    """
    global _rate_limiter
    if _rate_limiter is None or config is not None:
        if config is None:
            # Import here to avoid circular imports
            try:
                from config.environment_manager import get_rate_limit_config

                config = get_rate_limit_config()
            except ImportError:
                # Fallback to default if config module not available
                config = RateLimitConfig()
        _rate_limiter = IntelligentRateLimiter(config)
    return _rate_limiter


def reset_rate_limiter() -> None:
    """Reset the global rate limiter instance (useful for testing different configs)."""
    global _rate_limiter
    _rate_limiter = None


def rate_limit(endpoint: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to apply rate limiting to functions

    This is like a higher-order component in React that adds rate limiting.

    Args:
        endpoint: API endpoint identifier

    Returns:
        Callable: Decorated function
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            rate_limiter = get_rate_limiter()
            return rate_limiter.call_with_rate_limiting(func, endpoint, *args, **kwargs)

        return wrapper

    return decorator
