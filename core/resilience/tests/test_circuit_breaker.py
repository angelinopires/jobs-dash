"""
Unit Tests for Circuit Breaker Implementation

This module tests the circuit breaker pattern implementation to ensure
proper state transitions, configuration handling, and thread safety.
"""

import threading
import time
import unittest
from typing import Any, List
from unittest.mock import patch

from config.environment import CircuitBreakerConfig

from ..circuit_breaker import (
    CircuitBreaker,
    CircuitOpenException,
    CircuitState,
    get_all_circuit_breakers,
    get_circuit_breaker,
)


class TestCircuitBreaker(unittest.TestCase):
    """Test cases for CircuitBreaker class"""

    def setUp(self) -> None:
        """Set up test fixtures before each test method"""
        # Reset global circuit breakers
        get_all_circuit_breakers().clear()

        # Create a test circuit breaker with custom config
        self.config = {"threshold": 3, "timeout": 1}
        self.cb = CircuitBreaker("test_circuit", self.config)

    def tearDown(self) -> None:
        """Clean up after each test method"""
        # Reset global circuit breakers
        get_all_circuit_breakers().clear()

    def test_initial_state(self) -> None:
        """Test that circuit breaker starts in CLOSED state"""
        self.assertEqual(self.cb.state, CircuitState.CLOSED)
        self.assertEqual(self.cb.failure_count, 0)
        self.assertIsNone(self.cb._last_failure_time)

    def test_successful_call(self) -> None:
        """Test successful function call"""

        def success_func() -> str:
            return "success"

        result = self.cb.call(success_func)
        self.assertEqual(result, "success")
        self.assertEqual(self.cb.state, CircuitState.CLOSED)
        self.assertEqual(self.cb.failure_count, 0)

    def test_failed_call_closed_state(self) -> None:
        """Test failed call in CLOSED state"""

        def fail_func() -> None:
            raise ValueError("Test error")

        # First few failures should keep circuit closed
        for i in range(self.config["threshold"] - 1):
            with self.assertRaises(ValueError):
                self.cb.call(fail_func)
            self.assertEqual(self.cb.state, CircuitState.CLOSED)
            self.assertEqual(self.cb.failure_count, i + 1)

        # Threshold failure should open circuit
        with self.assertRaises(ValueError):
            self.cb.call(fail_func)
        self.assertEqual(self.cb.state, CircuitState.OPEN)
        self.assertEqual(self.cb.failure_count, self.config["threshold"])

    def test_open_circuit_blocks_calls(self) -> None:
        """Test that OPEN circuit blocks function calls"""
        # Open the circuit
        for _ in range(self.config["threshold"]):
            with self.assertRaises(ValueError):
                self.cb.call(lambda: (_ for _ in ()).throw(ValueError("Test")))

        # Circuit should be open
        self.assertEqual(self.cb.state, CircuitState.OPEN)

        # Calls should be blocked
        with self.assertRaises(CircuitOpenException):
            self.cb.call(lambda: "should not execute")

    def test_half_open_state(self) -> None:
        """Test HALF_OPEN state behavior"""
        # Open the circuit
        for _ in range(self.config["threshold"]):
            with self.assertRaises(ValueError):
                self.cb.call(lambda: (_ for _ in ()).throw(ValueError("Test")))

        # Wait for timeout
        time.sleep(self.config["timeout"] + 0.1)

        # Circuit should be half-open
        self.assertEqual(self.cb.state, CircuitState.HALF_OPEN)

        # Success should close circuit
        result = self.cb.call(lambda: "success")
        self.assertEqual(result, "success")
        self.assertEqual(self.cb.state, CircuitState.CLOSED)
        self.assertEqual(self.cb.failure_count, 0)

    def test_half_open_failure(self) -> None:
        """Test failure in HALF_OPEN state"""
        # Open the circuit
        for _ in range(self.config["threshold"]):
            with self.assertRaises(ValueError):
                self.cb.call(lambda: (_ for _ in ()).throw(ValueError("Test")))

        # Wait for timeout
        time.sleep(self.config["timeout"] + 0.1)

        # Failure in half-open should open circuit again
        with self.assertRaises(ValueError):
            self.cb.call(lambda: (_ for _ in ()).throw(ValueError("Test")))

        self.assertEqual(self.cb.state, CircuitState.OPEN)

    def test_manual_reset(self) -> None:
        """Test manual circuit reset"""
        # Open the circuit
        for _ in range(self.config["threshold"]):
            with self.assertRaises(ValueError):
                self.cb.call(lambda: (_ for _ in ()).throw(ValueError("Test")))

        # Manual reset
        self.cb.reset()
        self.assertEqual(self.cb.state, CircuitState.CLOSED)
        self.assertEqual(self.cb.failure_count, 0)
        self.assertIsNone(self.cb._last_failure_time)

    def test_get_status(self) -> None:
        """Test status information retrieval"""
        status = self.cb.get_status()

        expected_keys = {
            "name",
            "state",
            "failure_count",
            "threshold",
            "timeout",
            "last_failure_time",
            "time_since_last_failure",
        }
        self.assertEqual(set(status.keys()), expected_keys)
        self.assertEqual(status["name"], "test_circuit")
        self.assertEqual(status["state"], CircuitState.CLOSED.value)
        self.assertEqual(status["threshold"], self.config["threshold"])
        self.assertEqual(status["timeout"], self.config["timeout"])

    def test_thread_safety(self) -> None:
        """Test thread safety of circuit breaker"""
        results: List[str] = []
        errors: List[Exception] = []

        def worker() -> None:
            """Worker function for thread safety test"""
            try:
                # Simulate some work
                time.sleep(0.01)
                result = self.cb.call(lambda: "success")
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Create multiple threads
        threads = [threading.Thread(target=worker) for _ in range(10)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # All should succeed
        self.assertEqual(len(results), 10)
        self.assertEqual(len(errors), 0)
        self.assertEqual(self.cb.state, CircuitState.CLOSED)


class TestCircuitBreakerRegistry(unittest.TestCase):
    """Test cases for circuit breaker registry functions"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        get_all_circuit_breakers().clear()

    def tearDown(self) -> None:
        """Clean up after each test"""
        get_all_circuit_breakers().clear()

    def test_get_circuit_breaker_creation(self) -> None:
        """Test creating new circuit breaker via registry"""
        cb = get_circuit_breaker("new_circuit")
        self.assertIsInstance(cb, CircuitBreaker)
        self.assertEqual(cb.name, "new_circuit")

    def test_get_circuit_breaker_existing(self) -> None:
        """Test getting existing circuit breaker"""
        cb1 = get_circuit_breaker("existing_circuit")
        cb2 = get_circuit_breaker("existing_circuit")
        self.assertIs(cb1, cb2)  # Same instance

    def test_get_circuit_breaker_with_config(self) -> None:
        """Test creating circuit breaker with custom config"""
        config = {"threshold": 10, "timeout": 5}
        cb = get_circuit_breaker("config_circuit", config)
        self.assertEqual(cb.threshold, 10)
        self.assertEqual(cb.timeout, 5)

    def test_get_all_circuit_breakers(self) -> None:
        """Test getting all circuit breakers"""
        # Create multiple circuit breakers
        get_circuit_breaker("circuit1")
        get_circuit_breaker("circuit2")

        all_cbs = get_all_circuit_breakers()
        self.assertEqual(len(all_cbs), 2)
        self.assertIn("circuit1", all_cbs)
        self.assertIn("circuit2", all_cbs)


class TestCircuitBreakerConfiguration(unittest.TestCase):
    """Test cases for circuit breaker configuration"""

    @patch("core.circuit_breaker.get_circuit_breaker_config")
    def test_default_configuration(self, mock_get_config: Any) -> None:
        """Test default configuration loading"""
        # Mock the config
        mock_config = CircuitBreakerConfig(threshold=5, timeout=300)
        mock_get_config.return_value = mock_config

        cb = CircuitBreaker("default_circuit")
        self.assertEqual(cb.threshold, 5)
        self.assertEqual(cb.timeout, 300)

    def test_custom_configuration(self) -> None:
        """Test custom configuration override"""
        config = {"threshold": 7, "timeout": 60}
        cb = CircuitBreaker("custom_circuit", config)
        self.assertEqual(cb.threshold, 7)
        self.assertEqual(cb.timeout, 60)


if __name__ == "__main__":
    unittest.main()
