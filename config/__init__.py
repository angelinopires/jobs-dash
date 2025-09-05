"""
Configuration package for Jobs Dashboard.

This package contains simplified unified configuration:
- Rate limiting configuration for all environments
- Configuration utilities
"""

from .environment_manager import get_config_summary, get_rate_limit_config, set_environment_for_testing

__all__ = [
    # Unified configuration
    "get_rate_limit_config",
    "set_environment_for_testing",
    "get_config_summary",
]
