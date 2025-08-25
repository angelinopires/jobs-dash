"""
Configuration module for the job dashboard.
Contains country mappings and remote filter configuration data.
"""

from .countries import get_country_options, get_indeed_country_name, get_country_info

from .remote_filters import (
    get_currency_options,
    get_currency_code,
    get_job_type_options,
    get_job_type_code,
    get_global_countries,
    enhance_search_term_with_remote_keywords,
    get_country_flag_and_name
)

__all__ = [
    # Countries
    'get_country_options', 
    'get_indeed_country_name', 
    'get_country_info',
    # Remote filters
    'get_currency_options',
    'get_currency_code',
    'get_job_type_options', 
    'get_job_type_code',
    'get_global_countries',
    'enhance_search_term_with_remote_keywords',
    'get_country_flag_and_name'
]
