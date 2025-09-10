"""
Unit tests for pattern definitions and compilation.

These tests validate the regex patterns used for remote job filtering,
ensuring they compile correctly and match expected text patterns using
the disqualifier-only filtering approach.
"""

import re
from typing import Dict

import pytest

from core.filters.pattern_definitions import (
    HIGH_CONFIDENCE_DISQUALIFIERS,
    compile_patterns,
    get_pattern_names_by_category,
)


@pytest.fixture
def compiled_patterns() -> Dict[str, Dict[str, re.Pattern]]:
    """Fixture to provide compiled patterns for testing."""
    return compile_patterns()


def test_pattern_compilation(compiled_patterns: Dict[str, Dict[str, re.Pattern]]) -> None:
    """Test that all patterns compile without errors."""
    # Check that the category is present
    assert "negative" in compiled_patterns

    # Check that the category has patterns
    patterns = compiled_patterns["negative"]
    assert len(patterns) > 0

    # Check that each pattern is a compiled regex
    for pattern_name, pattern in patterns.items():
        assert isinstance(pattern, re.Pattern), f"Pattern {pattern_name} should be compiled"


def test_pattern_structure_consistency() -> None:
    """Test that pattern dictionaries have consistent structure."""
    # Check that pattern dictionary has expected structure
    assert isinstance(HIGH_CONFIDENCE_DISQUALIFIERS, dict)

    # Check that dictionary has patterns
    assert len(HIGH_CONFIDENCE_DISQUALIFIERS) > 0


def test_pattern_names_utility() -> None:
    """Test the get_pattern_names_by_category utility function."""
    pattern_names = get_pattern_names_by_category()

    # Check structure
    assert "negative" in pattern_names

    # Check that category has pattern names
    names = pattern_names["negative"]
    assert isinstance(names, list)
    assert len(names) > 0

    # Check that names are strings
    for name in names:
        assert isinstance(name, str)
        assert len(name) > 0


@pytest.mark.parametrize(
    ("pattern_name", "test_text", "should_match"),
    [
        # Office patterns
        ("EXPLICITLY_NOT_REMOTE", "not a remote", True),
        ("EXPLICITLY_NOT_REMOTE", "non remote", True),
        ("EXPLICITLY_NOT_REMOTE", "remote work", False),
        ("WORK_FROM_OFFICE", "work from our office", True),
        ("WORK_FROM_OFFICE", "work from home", False),
        ("MUST_BE_IN_OFFICE", "must be in the office", True),
        ("MUST_BE_IN_OFFICE", "must be in office", True),
        ("MUST_BE_IN_OFFICE", "can be remote", False),
        ("OFFICE_REQUIRED", "office presence is mandatory", True),
        ("OFFICE_REQUIRED", "office attendance required", True),
        ("OFFICE_REQUIRED", "office work necessary", True),
        ("OFFICE_REQUIRED", "office equipment provided", False),
        # Hybrid patterns
        ("EXPLICIT_HYBRID_ROLE", "this is a hybrid role", True),
        ("EXPLICIT_HYBRID_ROLE", "hybrid work", False),
        ("DAYS_IN_OFFICE", "3 days a week in office", True),
        ("DAYS_IN_OFFICE", "two days per week in office", True),
        ("DAYS_IN_OFFICE", "3 days in office", True),
        ("DAYS_IN_OFFICE", "3 days remote", False),
        ("HYBRID_POSITION", "hybrid position available", True),
        ("HYBRID_POSITION", "hybrid arrangement", True),
        ("HYBRID_POSITION", "flexible arrangement", False),
        # Location patterns
        ("MUST_RESIDE_US", "must reside in the United States", True),
        ("MUST_RESIDE_US", "must live in the U.S.", True),
        ("MUST_RESIDE_US", "can live anywhere", False),
        ("BASED_REQUIRED_US", "US based required", True),
        ("BASED_REQUIRED_US", "U.S. based needed", True),
        ("BASED_REQUIRED_US", "based anywhere", False),
        ("WITHIN_COMMUTING_DISTANCE", "within commuting distance", True),
        ("WITHIN_COMMUTING_DISTANCE", "no commute required", False),
        # Citizenship patterns
        ("US_CITIZENSHIP_REQUIRED", "U.S. citizenship", True),
        ("US_CITIZENSHIP_REQUIRED", "United States citizenship", True),
        ("US_CITIZENSHIP_REQUIRED", "citizenship", False),
        ("CITIZENSHIP_REQUIRED", "citizenship required", True),
        ("CITIZENSHIP_REQUIRED", "citizenship preferred", True),
        ("CITIZENSHIP_REQUIRED", "no citizenship needed", False),
        # Work authorization patterns
        ("ELIGIBLE_TO_WORK", "eligible to work in the U.S.", True),
        ("ELIGIBLE_TO_WORK", "authorized to work in the U.S.", True),
        ("ELIGIBLE_TO_WORK", "can work anywhere", False),
        ("MUST_HAVE_US_AUTH", "must have U.S. work authorization", True),
        ("MUST_HAVE_US_AUTH", "work authorization optional", False),
        # Security clearance patterns
        ("CLEARANCE_REQUIRED", "clearance required", True),
        ("CLEARANCE_REQUIRED", "clearance needed", True),
        ("CLEARANCE_REQUIRED", "no clearance needed", False),
        ("SECURITY_CLEARANCE", "security clearance required", True),
        ("SECURITY_CLEARANCE", "security clearance not needed", False),
        # Travel patterns
        ("WEEKLY_TRAVEL", "weekly travel required", True),
        ("WEEKLY_TRAVEL", "occasional travel", False),
        ("TRAVEL_COMMITMENT", "travel commitment of 25%", True),
        ("TRAVEL_COMMITMENT", "minimal travel", False),
        ("TRAVEL_TO_OFFICE", "travel to office required", True),
        ("TRAVEL_TO_OFFICE", "no office travel", False),
        # Visa patterns
        ("SPONSORSHIP_NOT_AVAILABLE", "sponsorship is not available", True),
        ("SPONSORSHIP_NOT_AVAILABLE", "sponsorship available", False),
        ("RELOCATION_REQUIRED", "relocation required", True),
        ("RELOCATION_REQUIRED", "no relocation needed", False),
    ],
)
def test_pattern_matching(
    compiled_patterns: Dict[str, Dict[str, re.Pattern]], pattern_name: str, test_text: str, should_match: bool
) -> None:
    """Test pattern matching with various test cases."""
    pattern = compiled_patterns["negative"][pattern_name]
    match = pattern.search(test_text) is not None
    assert (
        match == should_match
    ), f"Pattern {pattern_name} {'should' if should_match else 'should not'} match '{test_text}'"


@pytest.mark.parametrize(
    ("pattern_name", "test_text"),
    [
        # Office patterns
        ("MUST_BE_IN_OFFICE", "MUST BE IN THE OFFICE"),
        ("MUST_BE_IN_OFFICE", "must be in the office"),
        ("MUST_BE_IN_OFFICE", "Must Be In The Office"),
        # Hybrid patterns
        ("EXPLICIT_HYBRID_ROLE", "THIS IS A HYBRID ROLE"),
        ("EXPLICIT_HYBRID_ROLE", "this is a hybrid role"),
        ("EXPLICIT_HYBRID_ROLE", "This Is A Hybrid Role"),
        # Citizenship patterns
        ("US_CITIZENSHIP_REQUIRED", "U.S. CITIZENSHIP"),
        ("US_CITIZENSHIP_REQUIRED", "united states citizenship"),
        ("US_CITIZENSHIP_REQUIRED", "United States Citizenship"),
    ],
)
def test_case_insensitive_patterns(
    compiled_patterns: Dict[str, Dict[str, re.Pattern]], pattern_name: str, test_text: str
) -> None:
    """Test that patterns are case insensitive."""
    pattern = compiled_patterns["negative"][pattern_name]
    match = pattern.search(test_text) is not None
    assert match, f"Pattern {pattern_name} should match '{test_text}' (case insensitive)"


@pytest.mark.parametrize(
    ("pattern_name", "test_text"),
    [
        ("MUST_BE_IN_OFFICE", "mustbeinoffice"),  # office is part of another word
        ("WORK_FROM_OFFICE", "workfromoffice"),  # office is part of another word
        ("US_CITIZENSHIP_REQUIRED", "businesscitizenship"),  # citizenship is part of another word
    ],
)
def test_word_boundary_patterns(
    compiled_patterns: Dict[str, Dict[str, re.Pattern]], pattern_name: str, test_text: str
) -> None:
    """Test that patterns use word boundaries to avoid false matches."""
    pattern = compiled_patterns["negative"][pattern_name]
    match = pattern.search(test_text) is not None
    assert not match, f"Pattern {pattern_name} should NOT match '{test_text}' due to word boundaries"


def test_pattern_count_expectations(compiled_patterns: Dict[str, Dict[str, re.Pattern]]) -> None:
    """Test that we have a reasonable number of patterns."""
    # These are minimum expectations - we can have more patterns
    min_count = 20  # Should have at least 20 negative disqualifier patterns
    actual_count = len(compiled_patterns["negative"])
    assert actual_count >= min_count, f"Should have at least {min_count} patterns, got {actual_count}"


@pytest.mark.parametrize(
    ("pattern_name", "variations"),
    [
        ("MUST_BE_IN_OFFICE", ["must-be-in-office", "must be in office", "must-be-in-office"]),
        ("WORK_FROM_OFFICE", ["work-from-office", "work from office", "work-from-office"]),
    ],
)
def test_hyphen_variations(
    compiled_patterns: Dict[str, Dict[str, re.Pattern]], pattern_name: str, variations: list
) -> None:
    """Test patterns handle different hyphen/dash styles."""
    pattern = compiled_patterns["negative"][pattern_name]
    for variation in variations:
        match = pattern.search(variation) is not None
        assert match, f"Pattern {pattern_name} should match variation '{variation}'"


@pytest.mark.parametrize(
    ("pattern_name", "variations"),
    [
        ("WORK_FROM_OFFICE", ["work from office", "work  from  office", "work   from   office"]),
        ("MUST_BE_IN_OFFICE", ["must be in office", "must  be  in  office", "must   be   in   office"]),
    ],
)
def test_spacing_variations(
    compiled_patterns: Dict[str, Dict[str, re.Pattern]], pattern_name: str, variations: list
) -> None:
    """Test patterns handle different spacing styles."""
    pattern = compiled_patterns["negative"][pattern_name]
    for variation in variations:
        match = pattern.search(variation) is not None
        assert match, f"Pattern {pattern_name} should match spacing variation '{variation}'"
