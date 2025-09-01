"""
Circuit Breaker Pattern Implementation

This module implements the Circuit Breaker pattern to prevent cascading failures
when external APIs are unavailable or slow.
"""

import logging
import threading
import time
from enum import Enum
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

from config.environment import get_circuit_breaker_config

# Set up logging
logger = logging.getLogger(__name__)

# Type variable for generic return types
T = TypeVar("T")


class CircuitState(Enum):
    """
    Circuit Breaker States

    Think of this like a React component's state or a Redux state enum:
    - CLOSED: Normal operation (like "idle" state)
    - OPEN: Circuit is broken, failing fast (like "error" state)
    - HALF_OPEN: Testing if service is back (like "loading" state)
    """

    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreaker:
    """
    Circuit Breaker Implementation

    This class implements the circuit breaker pattern to protect against
    cascading failures when external services are unavailable.

    Similar to implementing a retry mechanism with state management in React.
    """

    def __init__(self, name: str, config: Optional[dict] = None):
        """
        Initialize the circuit breaker

        Args:
            name: Name of the circuit breaker (for logging)
            config: Optional configuration override
        """
        self.name = name
        self._lock = threading.Lock()  # Thread safety (like a mutex)

        # Load configuration
        if config:
            self.threshold = config.get("threshold", 5)
            self.timeout = config.get("timeout", 300)
        else:
            cb_config = get_circuit_breaker_config()
            self.threshold = cb_config.threshold
            self.timeout = cb_config.timeout

        # Circuit state
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None

        logger.info(f"Circuit breaker '{name}' initialized with threshold={self.threshold}, timeout={self.timeout}s")

    @property
    def state(self) -> CircuitState:
        """Get current circuit state (thread-safe)"""
        with self._lock:
            return self._state

    @property
    def failure_count(self) -> int:
        """Get current failure count (thread-safe)"""
        with self._lock:
            return self._failure_count

    def _should_attempt_reset(self) -> bool:
        """
        Check if circuit should attempt to reset (move to HALF_OPEN)

        Returns:
            bool: True if timeout has elapsed
        """
        if self._last_failure_time is None:
            return False

        return bool(time.time() - self._last_failure_time >= self.timeout)

    def _on_success(self) -> None:
        """Handle successful operation"""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                # Success in half-open state, close the circuit
                logger.info(f"Circuit breaker '{self.name}': HALF_OPEN → CLOSED (success)")
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._last_failure_time = None
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed operation"""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()

            if self._state == CircuitState.CLOSED and self._failure_count >= self.threshold:
                # Open the circuit
                logger.warning(
                    f"Circuit breaker '{self.name}': CLOSED → OPEN " f"(threshold reached: {self.failure_count})"
                )
                self._state = CircuitState.OPEN
            elif self._state == CircuitState.HALF_OPEN:
                # Failure in half-open state, open the circuit again
                logger.warning(f"Circuit breaker '{self.name}': HALF_OPEN → OPEN (failure in half-open)")
                self._state = CircuitState.OPEN

    def _can_execute(self) -> bool:
        """
        Check if operation can be executed

        Returns:
            bool: True if operation should be attempted
        """
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    # Move to half-open state
                    logger.info(f"Circuit breaker '{self.name}': OPEN → HALF_OPEN (timeout elapsed)")
                    self._state = CircuitState.HALF_OPEN
                    return True
                return False

            # HALF_OPEN state - allow one attempt
            return True

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Execute function with circuit breaker protection

        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            T: Function result

        Raises:
            Exception: Original exception if circuit is closed or half-open
            CircuitOpenException: If circuit is open
        """
        if not self._can_execute():
            raise CircuitOpenException(f"Circuit breaker '{self.name}' is OPEN")

        try:
            # Execute the function
            result = func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            logger.debug(f"Circuit breaker '{self.name}': Function failed: {type(e).__name__}: {e}")
            raise

    def reset(self) -> None:
        """Manually reset the circuit breaker to CLOSED state"""
        with self._lock:
            logger.info(f"Circuit breaker '{self.name}': Manual reset to CLOSED")
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._last_failure_time = None

    def get_status(self) -> dict:
        """
        Get current circuit breaker status

        Returns:
            dict: Status information
        """
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "threshold": self.threshold,
                "timeout": self.timeout,
                "last_failure_time": self._last_failure_time,
                "time_since_last_failure": time.time() - self._last_failure_time if self._last_failure_time else None,
            }


class CircuitOpenException(Exception):
    """Exception raised when circuit breaker is open"""

    pass


def circuit_breaker(name: str, config: Optional[dict] = None) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to apply circuit breaker pattern to functions

    This is like a higher-order component in React or a decorator in TypeScript.

    Args:
        name: Circuit breaker name
        config: Optional configuration override

    Returns:
        Callable: Decorated function
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        # Create circuit breaker instance for this function
        cb = CircuitBreaker(name, config)

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return cb.call(func, *args, **kwargs)

        # Attach circuit breaker to wrapper for access
        setattr(wrapper, "circuit_breaker", cb)
        return wrapper

    return decorator


# Global circuit breaker registry
# This is like a service registry in Angular or a context in React
_circuit_breakers: dict[str, CircuitBreaker] = {}


def get_circuit_breaker(name: str, config: Optional[dict] = None) -> CircuitBreaker:
    """
    Get or create a circuit breaker instance

    Args:
        name: Circuit breaker name
        config: Optional configuration override

    Returns:
        CircuitBreaker: Circuit breaker instance
    """
    if name not in _circuit_breakers:
        _circuit_breakers[name] = CircuitBreaker(name, config)

    return _circuit_breakers[name]


def get_all_circuit_breakers() -> dict[str, CircuitBreaker]:
    """
    Get all circuit breaker instances

    Returns:
        dict: All circuit breakers by name
    """
    return _circuit_breakers.copy()
