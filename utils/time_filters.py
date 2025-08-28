"""
Time filter utilities for job search.
Converts user-friendly time options to hours for API calls.
"""

from typing import Dict, Optional

# Time filter options mapping
TIME_FILTERS: Dict[str, Optional[int]] = {
    "Last 24h": 24,
    "Last 72h": 72,
    "Past Week": 168,  # 7 days
    "Past Month": 720,  # 30 days (24 * 30)
}


def get_time_filter_options() -> list:
    """Get list of available time filter options."""
    return list(TIME_FILTERS.keys())


def get_hours_from_filter(time_filter: str) -> Optional[int]:
    """Convert time filter option to hours for API."""
    return TIME_FILTERS.get(time_filter)


def get_filter_from_hours(hours: Optional[int]) -> str:
    """Convert hours back to filter option for display."""
    if hours is None:
        return "Past Month"  # Default to longest period

    for option, hour_value in TIME_FILTERS.items():
        if hour_value == hours:
            return option

    return "Past Month"  # Default


def is_time_filter_enabled(time_filter: str) -> bool:
    """Check if a time filter will actually filter results."""
    return TIME_FILTERS.get(time_filter) is not None
