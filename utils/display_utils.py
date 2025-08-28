"""
Display utility functions for cleaning and formatting job data.
"""

from typing import Any

import pandas as pd

from utils.constants import INVALID_VALUES


def clean_display_value(value: str, default: str = "Not available") -> str:
    """
    Clean a value for display, handling nan, None, empty strings, etc.

    Args:
        value: The value to clean
        default: Default value to return if the value is invalid

    Returns:
        Cleaned string value
    """
    if value is None or pd.isna(value):
        return default

    # Convert to string and check for common "invalid" values
    str_value = str(value).strip()

    # Check for empty string after stripping
    if not str_value:
        return default

    # Check for common "invalid" values (case-insensitive)
    if str_value.lower() in [v.lower() for v in INVALID_VALUES]:
        return default

    # Handle specific cases like "n.a.", "N.A.", etc.
    if str_value.lower() in ["n.a.", "n/a", "na", "null", "none", "undefined", "<na>"]:
        return default

    return str_value


def clean_company_info(company_info_str: str) -> str:
    """
    Clean company info string that may contain nan values.

    Args:
        company_info_str: String like "Industry: nan | Size: nan | Revenue: nan"

    Returns:
        Cleaned string or "Not available" if all parts are invalid
    """
    if (
        pd.isna(company_info_str)
        or (company_info_str is None)
        or (isinstance(company_info_str, str) and not company_info_str.strip())
    ):
        return "Not available"

    str_value = str(company_info_str).strip()

    if str_value.lower() in [v.lower() for v in INVALID_VALUES]:
        return "Not available"

    # Split by pipes and clean each part
    parts = str_value.split("|")
    cleaned_parts = []

    for part in parts:
        part = part.strip()
        if ":" in part:
            label, value = part.split(":", 1)
            value = value.strip()
            # Check if the value part is valid
            if value.lower() not in [v.lower() for v in INVALID_VALUES]:
                cleaned_parts.append(f"{label.strip()}: {value}")

    if cleaned_parts:
        return " | ".join(cleaned_parts)
    else:
        return "Not available"


def format_posted_date_enhanced(date_value: Any) -> str:
    """
    Enhanced date formatting that handles various input formats and invalid values.

    Args:
        date_value: Date value in various formats

    Returns:
        Formatted date string like "Aug 23, 2025 16:47"
    """
    if pd.isna(date_value) or (date_value is None) or (isinstance(date_value, str) and not date_value.strip()):
        return "N/A"

    try:
        import datetime as dt

        # Handle different input formats
        if isinstance(date_value, str):
            # If it's already in our target format, return as-is
            if ":" in date_value and any(
                month in date_value
                for month in ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
            ):
                return date_value

            # Parse various string formats
            if date_value.lower() in [v.lower() for v in INVALID_VALUES]:
                return "N/A"

            # Try different parsing approaches
            try:
                # Handle numeric timestamp strings (Unix timestamps)
                if date_value.isdigit():
                    timestamp = float(date_value)
                    if timestamp > 1e10:  # Milliseconds
                        timestamp = timestamp / 1000.0
                    date_obj = dt.datetime.fromtimestamp(timestamp)
                    return str(date_obj.strftime("%b %d, %Y"))  # Date only, no time

                # Handle ISO format dates like "2025-08-23"
                if "-" in date_value and len(date_value) == 10:
                    parsed_date = dt.datetime.strptime(date_value, "%Y-%m-%d")
                    return parsed_date.strftime("%b %d, %Y")  # Date only, no time

                # Handle other formats
                parsed_date = pd.to_datetime(date_value)
                return str(parsed_date.strftime("%b %d, %Y"))  # Always date only, no time
            except Exception:
                # Check if the original value is invalid
                if str(date_value).lower() in [v.lower() for v in INVALID_VALUES]:
                    return "N/A"
                return str(date_value)  # Return as-is if can't parse

        elif isinstance(date_value, (int, float)):
            # Handle timestamps
            timestamp = float(date_value)
            if timestamp > 1e10:  # Milliseconds
                timestamp = timestamp / 1000.0
            date_obj = dt.datetime.fromtimestamp(timestamp)
            return str(date_obj.strftime("%b %d, %Y"))  # Date only, no time

        elif hasattr(date_value, "strftime"):
            # Already a datetime object
            return str(date_value.strftime("%b %d, %Y"))  # Date only, no time

        else:
            # Check if the original value is invalid
            if str(date_value).lower() in [v.lower() for v in INVALID_VALUES]:
                return "N/A"
            return str(date_value)

    except Exception:
        # Check if the original value is invalid
        if str(date_value).lower() in [v.lower() for v in INVALID_VALUES]:
            return "N/A"
        return str(date_value) if date_value else "N/A"
