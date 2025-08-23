"""
Remote job filters configuration for the job dashboard.
Contains currencies, job types, and remote levels for remote-first job searching.
"""

from typing import Dict, List, Tuple

# Currency options for salary filtering
CURRENCIES: Dict[str, Tuple[str, str]] = {
    "Any": ("Any Currency", "ANY"),
    "USD": ("US Dollar", "USD"),
    "EUR": ("Euro", "EUR"), 
    "GBP": ("British Pound", "GBP"),
    "CAD": ("Canadian Dollar", "CAD"),
    "AUD": ("Australian Dollar", "AUD"),
    "BRL": ("Brazilian Real", "BRL"),
}

# Job type options (using JobSpy's expected lowercase values)
JOB_TYPES: Dict[str, Tuple[str, str]] = {
    "Any": ("Any Type", "ANY"),
    "Full-time": ("Full-time", "fulltime"),
    "Contract": ("Contract/Freelance", "contract"),
    "Part-time": ("Part-time", "parttime"),
}

# Remote level options
REMOTE_LEVELS: Dict[str, Tuple[str, str]] = {
    "Fully Remote": ("Fully Remote", "FULLY_REMOTE"),
    "Hybrid": ("Hybrid/Flexible", "HYBRID"),
    "Any": ("Any Remote Level", "ANY"),
}

# Global search countries (in priority order)
GLOBAL_COUNTRIES: List[Tuple[str, str, str]] = [
    ("🇺🇸", "United States", "usa"),
    ("🇨🇦", "Canada", "canada"), 
    ("🇧🇷", "Brazil", "brazil"),
    ("🇩🇪", "Germany", "germany"),
    ("🇳🇱", "Netherlands", "netherlands"),
    ("🇬🇧", "United Kingdom", "uk"),
    ("🇦🇺", "Australia", "australia"),
]

# Remote search keywords to enhance search terms
REMOTE_KEYWORDS: List[str] = [
    "remote",
    "work from home", 
    "WFH",
    "distributed",
    "telecommute",
    "home office"
]

def get_currency_options() -> List[str]:
    """Get list of currency display names for dropdown."""
    return list(CURRENCIES.keys())

def get_currency_code(currency_name: str) -> str:
    """Get currency code from display name."""
    if currency_name in CURRENCIES:
        return CURRENCIES[currency_name][1]
    return "ANY"

def get_job_type_options() -> List[str]:
    """Get list of job type display names for dropdown.""" 
    return list(JOB_TYPES.keys())

def get_job_type_code(job_type_name: str) -> str:
    """Get job type code from display name."""
    if job_type_name in JOB_TYPES:
        return JOB_TYPES[job_type_name][1]
    return "ANY"

def get_remote_level_options() -> List[str]:
    """Get list of remote level display names for dropdown."""
    return list(REMOTE_LEVELS.keys())

def get_remote_level_code(remote_level_name: str) -> str:
    """Get remote level code from display name."""
    if remote_level_name in REMOTE_LEVELS:
        return REMOTE_LEVELS[remote_level_name][1]
    return "FULLY_REMOTE"

def get_global_countries() -> List[Tuple[str, str, str]]:
    """Get list of global countries for remote job search."""
    return GLOBAL_COUNTRIES.copy()

def get_global_countries_display() -> str:
    """Get formatted string of global countries for display."""
    countries = [f"{flag} {name}" for flag, name, _ in GLOBAL_COUNTRIES]
    return ", ".join(countries)

def enhance_search_term_with_remote_keywords(search_term: str) -> str:
    """Enhance search term with remote keywords for better remote job results."""
    if not search_term.strip():
        return search_term
    
    # Don't add keywords if already present
    search_lower = search_term.lower()
    has_remote_keyword = any(keyword in search_lower for keyword in ["remote", "wfh", "work from home"])
    
    if has_remote_keyword:
        return search_term
    
    # Add remote keywords to enhance results
    enhanced_term = f"{search_term} (remote OR \"work from home\" OR WFH)"
    return enhanced_term

def get_country_flag_and_name(country_code: str) -> Tuple[str, str]:
    """Get country flag and name from country code."""
    for flag, name, code in GLOBAL_COUNTRIES:
        if code == country_code:
            return flag, name
    return "🌍", "Unknown"
