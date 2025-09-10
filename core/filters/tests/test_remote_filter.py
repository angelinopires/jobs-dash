"""
Unit tests for remote job filtering functionality.

These tests validate the RemoteJobFilter class and its ability to correctly
identify and filter out false remote positions using pattern matching.

Test Categories:
- Pattern matching for on-site requirements
- Pattern matching for hybrid work arrangements
- Pattern matching for location restrictions
- Edge cases (empty descriptions, None values)
- DataFrame operations and integration
- Performance testing
"""

import numpy as np
import pandas as pd
import pytest

from core.filters.remote_filter import RemoteJobFilter


@pytest.fixture
def remote_filter() -> RemoteJobFilter:
    """Fixture to create a RemoteJobFilter instance with debug mode disabled."""
    return RemoteJobFilter(debug_mode=False)


@pytest.fixture
def sample_jobs_df() -> pd.DataFrame:
    """Fixture to create a sample DataFrame with job data."""
    return pd.DataFrame(
        {
            "id": [1, 2, 3, 4, 5],
            "title": ["Software Engineer", "Data Scientist", "Product Manager", "Designer", "Developer"],
            "company": ["TechCorp", "DataInc", "ProductCo", "DesignStudio", "DevShop"],
            "location": ["Remote", "Remote", "Remote", "Remote", "Remote"],
            "description": [
                "Fully remote software engineering position",  # Should pass
                "This is a hybrid role with 3 days in office",  # Should be filtered
                "Work from anywhere, 100% remote",  # Should pass
                "Must be a U.S. citizen",  # Should be filtered
                "Security clearance required",  # Should be filtered
            ],
        }
    )


class TestRemoteFilterPatternMatching:
    """Test pattern matching functionality of RemoteJobFilter."""

    @pytest.mark.parametrize(
        ("description", "expected", "reason"),
        [
            # Real examples from job listings that should be rejected
            ("U.S. citizenship required", False, "Citizenship requirement"),
            ("Security clearance required", False, "Security clearance"),
            ("Must reside in the United States", False, "Location restriction"),
            ("Must be authorized to work in the U.S.", False, "Work authorization"),
            ("This is a hybrid role with 2 days in office", False, "Hybrid requirement"),
            ("Must be within commuting distance", False, "Location restriction"),
            ("Sponsorship is not available", False, "Visa/sponsorship"),
            ("Travel commitment of 25%", False, "Travel requirement"),
            ("Must live in the United States", False, "Location restriction"),
        ],
    )
    def test_negative_signals_always_reject(
        self,
        remote_filter: RemoteJobFilter,
        description: str,
        expected: bool,
        reason: str,
    ) -> None:
        """Test that negative signals always reject, regardless of positive signals."""
        result = remote_filter.is_legitimate_remote(str(description))
        assert result == expected, f"Failed on: {description} ({reason})"

    @pytest.mark.parametrize(
        ("description", "expected", "reason"),
        [
            ("", True, "Empty string should be approved"),
            (None, True, "None should be approved"),
            ("Developer", True, "Short description should be approved"),
            ("Engineer needed", True, "Simple description should be approved"),
        ],
    )
    def test_edge_cases(
        self,
        remote_filter: RemoteJobFilter,
        description: str,
        expected: bool,
        reason: str,
    ) -> None:
        """Test edge cases and special scenarios."""
        result = remote_filter.is_legitimate_remote(str(description))
        assert result == expected, f"Failed on: {description} ({reason})"


class TestRemoteFilterDataFrameOperations:
    """Test DataFrame operations and integration."""

    def test_filter_dataframe_basic(
        self,
        remote_filter: RemoteJobFilter,
        sample_jobs_df: pd.DataFrame,
    ) -> None:
        """Test basic DataFrame filtering functionality."""
        result = remote_filter.filter_false_remote_jobs(sample_jobs_df)

        # Should keep jobs 1, 3 (legitimate remote) and filter out 2, 4, 5
        expected_legitimate_count = 2
        assert len(result) == expected_legitimate_count

        # Check that the right jobs remain
        remaining_ids = set(result["id"].tolist())
        expected_ids = {1, 3}
        assert remaining_ids == expected_ids

    def test_filter_empty_dataframe(
        self,
        remote_filter: RemoteJobFilter,
    ) -> None:
        """Test filtering empty DataFrame."""
        empty_df = pd.DataFrame()
        result = remote_filter.filter_false_remote_jobs(empty_df)

        assert result.empty
        assert isinstance(result, pd.DataFrame)

    def test_filter_missing_description_column(
        self,
        remote_filter: RemoteJobFilter,
    ) -> None:
        """Test error handling when description column is missing."""
        df_no_description = pd.DataFrame(
            {"id": [1, 2], "title": ["Job 1", "Job 2"], "company": ["Company A", "Company B"]}
        )

        with pytest.raises(KeyError):
            remote_filter.filter_false_remote_jobs(df_no_description)

    def test_filter_with_nan_descriptions(
        self,
        remote_filter: RemoteJobFilter,
    ) -> None:
        """Test filtering with NaN values in descriptions."""
        df_with_nans = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "title": ["Job 1", "Job 2", "Job 3", "Job 4"],
                "description": [
                    "Fully remote position",  # Should pass
                    np.nan,  # Should pass (NaN treated as legitimate)
                    "This is a hybrid role",  # Should be filtered
                    None,  # Should pass (None treated as legitimate)
                ],
            }
        )

        result = remote_filter.filter_false_remote_jobs(df_with_nans)

        # Should keep jobs 1, 2, 4 and filter out job 3
        assert len(result) == 3
        filtered_ids = set(result["id"].tolist())
        expected_ids = {1, 2, 4}
        assert filtered_ids == expected_ids


class TestRemoteFilterPatternCategories:
    """Test each pattern category individually."""

    @pytest.mark.parametrize(
        ("description", "expected", "reason"),
        [
            # Office requirements
            ("Not a remote position", False, "Explicit non-remote"),
            ("In office work required", False, "Office requirement"),
            ("Required in-person work", False, "In-person requirement"),
            ("Must be in our office", False, "Office requirement"),
            ("No remote work option", False, "No remote option"),
            ("Office presence is mandatory", False, "Office requirement"),
            ("Work from our office", False, "Office requirement"),
            # Borderline cases that should pass
            ("Office equipment provided for remote work", True, "Not an office requirement"),
            ("Access to virtual office tools required", True, "Not an office requirement"),
            ("Remote office culture", True, "Not an office requirement"),
        ],
    )
    def test_office_patterns(
        self,
        remote_filter: RemoteJobFilter,
        description: str,
        expected: bool,
        reason: str,
    ) -> None:
        """Test patterns related to office requirements."""
        result = remote_filter.is_legitimate_remote(str(description))
        assert result == expected, f"Failed on: {description} ({reason})"

    @pytest.mark.parametrize(
        ("description", "expected", "reason"),
        [
            # Hybrid work arrangements
            ("3 days in office per week", False, "Days in office"),
            ("Two days a week at the office", False, "Days in office"),
            ("This is a hybrid role", False, "Explicit hybrid"),
            ("Hybrid position available", False, "Hybrid position"),
            ("Hybrid work model", False, "Hybrid model"),
            ("Split between home and office", False, "Split work"),
            ("Remote and in-person work required", False, "Mixed mode"),
            # Borderline cases that should pass
            ("Flexible work arrangements", True, "Not explicitly hybrid"),
            ("Remote with optional office access", True, "Optional office"),
            ("Choose your work location", True, "Location flexibility"),
        ],
    )
    def test_hybrid_patterns(
        self,
        remote_filter: RemoteJobFilter,
        description: str,
        expected: bool,
        reason: str,
    ) -> None:
        """Test patterns related to hybrid work arrangements."""
        result = remote_filter.is_legitimate_remote(str(description))
        assert result == expected, f"Failed on: {description} ({reason})"

    @pytest.mark.parametrize(
        ("description", "expected", "reason"),
        [
            # Complex combinations
            ("Hybrid role with U.S. citizenship required and weekly office visits", False, "Multiple disqualifiers"),
            (
                "Must be authorized to work in the U.S., security clearance required, local candidates only",
                False,
                "Multiple disqualifiers",
            ),
            (
                "Remote position with 3 days in office and required travel to client sites",
                False,
                "Multiple disqualifiers",
            ),
            # Borderline cases that should pass
            ("Remote position with optional office access and quarterly team meetups", True, "Optional arrangements"),
            ("Fully distributed team with annual in-person retreat", True, "Infrequent meetings"),
            ("Work from anywhere with flexible hours and virtual collaboration", True, "True remote setup"),
        ],
    )
    def test_complex_combinations(
        self,
        remote_filter: RemoteJobFilter,
        description: str,
        expected: bool,
        reason: str,
    ) -> None:
        """Test complex combinations of multiple patterns."""
        result = remote_filter.is_legitimate_remote(str(description))
        assert result == expected, f"Failed on: {description} ({reason})"
