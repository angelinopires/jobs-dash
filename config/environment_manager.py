"""
Simplified Rate Limiting Configuration

This module provides a unified rate limiting configuration for all environments.
No more complex environment detection - one config for all cases.
"""

import logging

from core.resilience.rate_limiter import RateLimitConfig

logger = logging.getLogger(__name__)


def get_rate_limit_config() -> RateLimitConfig:
    """
    Get unified rate limiting configuration for all environments.

    Uses a balanced configuration suitable for both production and testing:
    - 2s base delay (respectful to APIs)
    - 10s max delay (prevents excessive waits)
    - Moderate jitter and multipliers

    Returns:
        RateLimitConfig: Unified configuration for all environments
    """
    config = RateLimitConfig(
        base_delay=2.0,
        max_delay=10.0,
        jitter_factor=0.2,  # ±20% jitter
        slow_response_threshold=5.0,  # 5s threshold for "slow" state
        aggressive_response_threshold=10.0,  # 10s threshold for "aggressive" state
        slow_multiplier=1.5,  # 1.5x multiplier when slow
        aggressive_multiplier=2.0,  # 2x multiplier when very slow
    )

    logger.info(f"Rate limiting configured: base_delay={config.base_delay}s, max_delay={config.max_delay}s")
    return config


def set_environment_for_testing(profile: str) -> None:
    """
    Legacy function for backwards compatibility with tests.

    This function now does nothing since we use unified configuration,
    but is kept to avoid breaking existing test code.

    Args:
        profile: Profile name (ignored)
    """
    logger.info(f"Legacy function called with profile '{profile}' - using unified config")


def get_config_summary() -> dict:
    """
    Get summary of current configuration for debugging.

    Returns:
        dict: Summary of unified rate limiting settings
    """
    config = get_rate_limit_config()

    return {
        "environment": "unified",
        "rate_limiting": {
            "base_delay": config.base_delay,
            "max_delay": config.max_delay,
            "jitter_factor": config.jitter_factor,
            "slow_multiplier": config.slow_multiplier,
            "aggressive_multiplier": config.aggressive_multiplier,
            "slow_response_threshold": config.slow_response_threshold,
            "aggressive_response_threshold": config.aggressive_response_threshold,
        },
        "note": "Using unified configuration for all environments",
    }
