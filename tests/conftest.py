"""
Pytest configuration and custom optimizations for the jobs dashboard.

This file contains pytest fixtures, configuration, and optimizations
to make testing faster and more reliable.
"""

import os
import sys
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Set test environment variables
os.environ["STREAMLIT_SERVER_HEADLESS"] = "true"
os.environ["STREAMLIT_SERVER_RUN_ON_SAVE"] = "false"
os.environ["STREAMLIT_BROWSER_GATHER_USAGE_STATS"] = "false"
os.environ["STREAMLIT_SERVER_ENABLE_STATIC_SERVING"] = "false"
os.environ["STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION"] = "false"
os.environ["STREAMLIT_SERVER_ENABLE_CORS"] = "false"

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def pytest_configure(config: Any) -> None:
    """Configure pytest with custom optimizations."""
    # Mock heavy dependencies before any tests run
    mock_modules = {
        "streamlit": MagicMock(),
        "selenium": MagicMock(),
        # Don't mock redis globally as it breaks import chains
        # Don't mock requests globally as it breaks jobspy imports
    }

    for module_name, mock_module in mock_modules.items():
        sys.modules[module_name] = mock_module


def pytest_collection_modifyitems(config: Any, items: Any) -> None:
    """Automatically categorize tests based on their names."""
    for item in items:
        # Mark scraper tests
        if any(keyword in item.name.lower() for keyword in ["scraper", "indeed", "base_scraper"]):
            item.add_marker("scraper")

        # Mark display tests
        if any(keyword in item.name.lower() for keyword in ["display", "formatting", "dashboard"]):
            item.add_marker("display")

        # Mark cache tests
        if any(keyword in item.name.lower() for keyword in ["cache", "redis"]):
            item.add_marker("cache")

        # Mark rate limiting tests
        if any(keyword in item.name.lower() for keyword in ["rate", "circuit", "breaker"]):
            item.add_marker("rate_limit")

        # Mark as unit tests by default
        if not any(item.iter_markers()):
            item.add_marker("unit")


@pytest.fixture
def sample_jobs_df() -> pd.DataFrame:
    """Sample job data for testing."""
    return pd.DataFrame(
        {
            "title": ["Senior Python Developer", "Data Scientist", "Frontend Engineer"],
            "company": ["TechCorp", "DataLab", "WebDev Inc"],
            "location": ["Remote", "New York", "San Francisco"],
            "salary_formatted": ["$80,000 - $120,000", "$90,000 - $130,000", "$70,000 - $100,000"],
            "job_url": ["https://example.com/job/1", "https://example.com/job/2", "https://example.com/job/3"],
            "date_posted_formatted": ["2 days ago", "1 day ago", "3 days ago"],
            "job_type": ["Full-time", "Contract", "Full-time"],
            "remote_status": ["Remote", "Hybrid", "On-site"],
        }
    )


@pytest.fixture
def empty_jobs_df() -> pd.DataFrame:
    """Empty DataFrame for edge case testing."""
    return pd.DataFrame()


@pytest.fixture
def incomplete_jobs_df() -> pd.DataFrame:
    """DataFrame with missing columns for error testing."""
    return pd.DataFrame(
        {
            "title": ["Test Job"],
            "company": ["Test Company"],
            # Missing other required columns
        }
    )
