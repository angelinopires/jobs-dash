"""
Pattern definitions for remote job filtering.

This module contains regex patterns for identifying legitimate remote positions
using a disqualifier-only filtering approach focused on accessibility barriers.

CATEGORIES:
1. HIGH_CONFIDENCE_DISQUALIFIERS: Patterns that identify accessibility barriers for Latam workers
   - Citizenship requirements
   - Visa/work authorization barriers
   - Location restrictions
   - Travel requirements
   - Security clearances
   - Hybrid/office requirements

APPROACH:
- Check ALL disqualifiers first
- ANY disqualifier found = immediate rejection
- NO disqualifiers found = assume accessible from Latam
- No positive signals needed - absence of barriers = accessible
"""

import re
from typing import Dict, Pattern


# Helper function to create location-based patterns dynamically
def create_location_patterns(base_pattern: str, countries: list) -> Dict[str, Pattern[str]]:
    """Create location-based patterns for multiple countries using switch-case logic.

    Supported base patterns:
    - MUST_RESIDE: "must (reside|live|be located) in [country]"
    - BASED_REQUIRED: "[country] based (required|preferred|needed|essential)"
    - YOU_MUST_LIVE: "you must (live|reside) in [country]"

    Handles country name variations (e.g., US vs United States, UK vs United Kingdom).
    """
    pattern_templates = {
        "MUST_RESIDE": r"\bmust\s+(reside|live|be\s+located)\s+in\s+(the\s+)?{country}\b",
        "BASED_REQUIRED": r"\b{country}\s+based\s+(required|preferred|needed|essential)\b",
        "YOU_MUST_LIVE": r"\byou\s+must\s+(live|reside)\s+in\s+(the\s+)?{country}\b",
    }

    if base_pattern not in pattern_templates:
        raise ValueError(f"Unsupported base_pattern: {base_pattern}. " f"Supported: {list(pattern_templates.keys())}")

    patterns = {}
    template = pattern_templates[base_pattern]

    for country in countries:
        key = f"{base_pattern}_{country.upper()}"

        # Handle country name variations inline
        if country == "US":
            country_pattern = r"(U\.?S\.?|United\s+States)"
        elif country == "UK":
            country_pattern = r"(UK|United\s+Kingdom)"
        else:
            country_pattern = re.escape(country)

        # Apply the template with country pattern
        pattern = template.format(country=country_pattern)
        patterns[key] = re.compile(pattern, re.IGNORECASE)

    return patterns


# ============================================================================
# HIGH CONFIDENCE DISQUALIFIERS (Reject as Remote)
# ============================================================================
# These patterns indicate jobs that are NOT legitimate remote positions

MOST_COMMON_LOCATIONS = ["UK", "US", "Canada", "Europe", "France", "Germany", "Spain"]

BASED_REQUIRED_PATTERNS = create_location_patterns("BASED_REQUIRED", MOST_COMMON_LOCATIONS)

MUST_RESIDE_PATTERNS = create_location_patterns("MUST_RESIDE", MOST_COMMON_LOCATIONS)

YOU_MUST_LIVE_PATTERNS = create_location_patterns("YOU_MUST_LIVE", MOST_COMMON_LOCATIONS)

# Office-based requirements
OFFICE_PATTERNS = {
    "EXPLICITLY_NOT_REMOTE": re.compile(r"\b(not|non)\s+(a\s+)?remote\b", re.IGNORECASE),
    "IN_OFFICE_REQUIREMENT": re.compile(
        r"\bin\s+office\s+(work|presence|attendance)\s+(required|mandatory)\b", re.IGNORECASE
    ),
    "IN_PERSON_REQUIREMENT": re.compile(r"\b(required\s+in-person|in-person\s+work)\b", re.IGNORECASE),
    "MUST_BE_IN_OFFICE": re.compile(r"\bmust[-\s]+be[-\s]+in[-\s]+(our|the|a)?[-\s]*office\b", re.IGNORECASE),
    "NO_REMOTE_OPTION": re.compile(r"\bno\s+remote\s+(option|work)\b", re.IGNORECASE),
    "OFFICE_REQUIRED": re.compile(
        r"\b(office\s+(presence|attendance|work)\s+(required|mandatory|necessary)"
        r"|office\s+presence\s+is\s+mandatory)\b",
        re.IGNORECASE,
    ),
    "WORK_FROM_OFFICE": re.compile(r"\bwork[-\s]+from[-\s]+(our|the|a)?[-\s]*office\b", re.IGNORECASE),
}

# Hybrid work patterns
HYBRID_PATTERNS = {
    "DAYS_IN_OFFICE": re.compile(
        r"\b(\d+|one|two|three|four|five)\s+days?\s+(?:(?:a|per)\s+week\s+)?(?:in|at)\s+(?:the\s+)?office\b",
        re.IGNORECASE,
    ),
    "EXPLICIT_HYBRID_ROLE": re.compile(r"\bthis\s+is\s+a\s+hybrid\s+role\b", re.IGNORECASE),
    "HYBRID_POSITION": re.compile(r"\bhybrid\s+(position|role|job|arrangement)\b", re.IGNORECASE),
    "HYBRID_WORK_MODEL": re.compile(r"\bhybrid\s+work\s+(model|environment|setup)\b", re.IGNORECASE),
    "SPLIT_HOME_OFFICE": re.compile(r"\bsplit\s+between\s+home\s+and\s+(the\s+)?office\b", re.IGNORECASE),
    # Remote & In-person combinations
    "REMOTE_AND_IN_PERSON": re.compile(r"\bremote\s+(&|and)\s+in[\s\-]*person\b", re.IGNORECASE),
    "IN_PERSON_AND_REMOTE": re.compile(r"\bin[\s\-]*person\s+(&|and)\s+remote\b", re.IGNORECASE),
    "HYBRID_REMOTE_MODEL": re.compile(r"\bhybrid\s+remote\s+(model|environment|setup)\b", re.IGNORECASE),
}

# Citizenship and legal requirements
CITIZENSHIP_PATTERNS = {
    # Generic citizenship patterns
    "CITIZENSHIP_REQUIRED": re.compile(r"\b(?<!no\s)citizenship\s+(required|preferred|needed)\b", re.IGNORECASE),
    "CITIZEN_RESIDENT_REQUIRED": re.compile(
        r"\bmust\s+be\s+\w+\s+(citizen|permanent resident|resident)\b", re.IGNORECASE
    ),
    "CONTRACT_CITIZENSHIP": re.compile(r"\bdue\s+to\s+contract\s+requirements?.*\bcitizen\b", re.IGNORECASE),
    # Country-specific citizenship
    "CANADIAN_CITIZEN_REQUIRED": re.compile(
        r"\b(Canadian?|Canada)\s+(citizen|permanent resident)\s+(required|preferred|needed)\b", re.IGNORECASE
    ),
    "UK_CITIZENSHIP_REQUIRED": re.compile(
        r"\b(UK|United\s+Kingdom|British)\s+(citizens?|citizenship)\b", re.IGNORECASE
    ),
    "US_CITIZENSHIP_REQUIRED": re.compile(r"\b(U\.?S\.?|United\s+States)\s+(citizens?|citizenship)\b", re.IGNORECASE),
    "US_CITIZEN_REQUIRED": re.compile(
        r"\b(U\.?S\.?|United\s+States)\s+citizen\s+(required|preferred|needed)\b", re.IGNORECASE
    ),
}

# Work authorization patterns
AUTHORIZATION_PATTERNS = {
    "ELIGIBLE_TO_WORK": re.compile(r"\b(eligible|authorized)\s+to\s+work\s+in\s+(the\s+)?U\.?S\.?\b", re.IGNORECASE),
    "MUST_HAVE_US_AUTH": re.compile(r"\bmust\s+have\s+U\.?S\.?\s+work\s+authorization\b", re.IGNORECASE),
    "US_WORK_AUTHORIZATION": re.compile(
        r"\b(authorized|eligible)\s+to\s+work\s+in\s+(the\s+)?U\.?S\.?\b", re.IGNORECASE
    ),
    "WORK_AUTHORIZATION_REQUIRED": re.compile(r"\bwork\s+authorization\s+(required|needed|necessary)\b", re.IGNORECASE),
}

# Security clearance patterns
SECURITY_PATTERNS = {
    "CLEARANCE_REQUIRED": re.compile(r"\b(?<!no\s)clearance\s+(required|needed|necessary|preferred)\b", re.IGNORECASE),
    "EXPORT_CONTROL_ACCESS": re.compile(r"\baccess\s+to\s+U\.?S\.?\s+export[\s\-]*controlled\b", re.IGNORECASE),
    "EXPORT_CONTROL_REQUIREMENT": re.compile(r"\bexport\s+control\s+(requirements?|compliance)\b", re.IGNORECASE),
    "EXPORT_CONTROLLED_INFO": re.compile(r"\bexport[\s\-]*controlled\s+information\b", re.IGNORECASE),
    "GOVERNMENT_CLEARANCE": re.compile(r"\bgovernment\s+clearance\s+(required|needed|necessary)\b", re.IGNORECASE),
    "SECURITY_CLEARANCE": re.compile(r"\bsecurity\s+clearance\s+(required|needed|necessary)\b", re.IGNORECASE),
}

# Visa and sponsorship patterns
VISA_PATTERNS = {
    "ABLE_TO_RELOCATE": re.compile(r"\bable\s+to\s+relocate\s+(without\s+sponsorship)?\b", re.IGNORECASE),
    "RELOCATION_REQUIRED": re.compile(r"\b(?<!no\s)relocation\s+(required|needed|necessary|expected)\b", re.IGNORECASE),
    "RELOCATION_WITHOUT_SPONSORSHIP": re.compile(r"\brelocate\s+\(without\s+sponsorship\)\b", re.IGNORECASE),
    "SPONSORSHIP_NOT_AVAILABLE": re.compile(r"\bsponsorship\s+is\s+not\s+available\b", re.IGNORECASE),
    "VISA_ASSISTANCE": re.compile(
        r"\bvisa\s+(assistance|support|sponsorship)\s+(available|provided|offered)\b", re.IGNORECASE
    ),
}

# Travel requirement patterns
TRAVEL_PATTERNS = {
    # Deal-breaking frequent travel
    "WEEKLY_TRAVEL": re.compile(
        r"\b(weekly|(\d+|one|two|three|four|five)\s+(day|week)s?\s+"
        r"(a\s+|per\s+)?(month|week|weekend|weekends)?)\s+"
        r"(travel|in\s+(office|the\s+office))\s+"
        r"(required|mandatory|necessary|expected)?\b",
        re.IGNORECASE,
    ),
    "MULTIPLE_WEEKS_MONTH": re.compile(r"\b(\d+|one|two|three|four)\s+weeks?\s+(a|per)\s+month\b", re.IGNORECASE),
    # General travel patterns (context-dependent)
    "TRAVEL_COMMITMENT": re.compile(
        r"\b(anticipated\s+)?travel\s+commitment\s+(of\s+|up\s+to\s+)?(\d+|\w+)\s*%?\s*(%|days?|weeks?)?\b",
        re.IGNORECASE,
    ),
    "TRAVEL_TO_OFFICE": re.compile(r"\btravel\s+to\s+(and\s+work\s+in\s+)?(the\s+)?office\b", re.IGNORECASE),
}

# General location restrictions
LOCATION_PATTERNS = {
    "COUNTRY_SPECIFIC": re.compile(
        r"\bavailable\s+(only|exclusively)\s+(to|for)\s+\w+\s+(citizens?|residents?)\b", re.IGNORECASE
    ),
    "LOCAL_CANDIDATES_ONLY": re.compile(r"\blocal\s+candidates\s+(only|preferred)\b", re.IGNORECASE),
    "LOCATION_RESTRICTION": re.compile(
        r"\b(currently\s+living|must\s+(live|reside)|only\s+(apply|available)\s+if\s+you\s+(are|live))\s+in\s+\w+\b",
        re.IGNORECASE,
    ),
    "WITHIN_COMMUTING_DISTANCE": re.compile(r"\bwithin\s+commuting\s+distance\b", re.IGNORECASE),
}

# Combine all negative patterns
HIGH_CONFIDENCE_DISQUALIFIERS = {
    **HYBRID_PATTERNS,
    **OFFICE_PATTERNS,
    **YOU_MUST_LIVE_PATTERNS,
    **MUST_RESIDE_PATTERNS,
    **BASED_REQUIRED_PATTERNS,
    **LOCATION_PATTERNS,
    **CITIZENSHIP_PATTERNS,
    **AUTHORIZATION_PATTERNS,
    **VISA_PATTERNS,
    **SECURITY_PATTERNS,
    **TRAVEL_PATTERNS,
}


def compile_patterns() -> Dict[str, Dict[str, Pattern[str]]]:
    """
    Compile disqualifier regex patterns for efficient matching.

    Returns:
        Dict[str, Dict[str, Pattern[str]]]: Compiled disqualifier patterns
    """
    return {
        "negative": {name: pattern for name, pattern in HIGH_CONFIDENCE_DISQUALIFIERS.items()},
    }


def get_pattern_names_by_category() -> Dict[str, list]:
    """
    Get readable disqualifier pattern names.

    Returns:
        Dict[str, list]: Pattern names by category ("negative" only)
    """
    return {"negative": list(HIGH_CONFIDENCE_DISQUALIFIERS.keys())}
