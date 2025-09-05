"""
Data module for the job dashboard.

Contains static reference data including country mappings and job filter options.
"""

from .countries import get_country_info, get_country_options, get_indeed_country_name
from .job_filters import (
    enhance_search_term_with_remote_keywords,
    get_country_flag_and_name,
    get_currency_code,
    get_currency_options,
    get_global_countries,
    get_global_countries_display,
    get_job_type_code,
    get_job_type_options,
)

__all__ = [
    # Countries
    "get_country_options",
    "get_indeed_country_name",
    "get_country_info",
    # Job filters
    "get_currency_options",
    "get_currency_code",
    "get_job_type_options",
    "get_job_type_code",
    "get_global_countries",
    "get_global_countries_display",
    "enhance_search_term_with_remote_keywords",
    "get_country_flag_and_name",
]
