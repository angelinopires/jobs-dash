"""
Core filtering module for job data processing.

This module provides filtering capabilities for job data, including:
- Remote job filtering to exclude false remote positions
- Pattern matching for hybrid/on-site job detection
- Validation utilities for filter testing

Key Components:
- RemoteJobFilter: Main filtering class for remote job validation
- Pattern definitions: Readable regex patterns for job description analysis
- Filter validator: Testing and validation utilities
"""

from .pattern_definitions import HIGH_CONFIDENCE_DISQUALIFIERS, compile_patterns, get_pattern_names_by_category
from .remote_filter import RemoteJobFilter

__all__ = [
    "RemoteJobFilter",
    "HIGH_CONFIDENCE_DISQUALIFIERS",
    "compile_patterns",
    "get_pattern_names_by_category",
]
