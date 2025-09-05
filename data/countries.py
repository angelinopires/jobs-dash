"""
Country configuration for JobSpy scrapers.
Based on the official JobSpy repository: https://github.com/speedyapply/JobSpy
"""

from typing import Dict, List, Tuple

# Country mapping: (display_name, indeed_country_name, glassdoor_support)
# JobSpy expects lowercase country names, not country codes
COUNTRIES: Dict[str, Tuple[str, str, bool]] = {
    # Indeed + Glassdoor supported
    "Australia": ("Australia", "australia", True),
    "Belgium": ("Belgium", "belgium", True),
    "Brazil": ("Brazil", "brazil", True),
    "Canada": ("Canada", "canada", True),
    "France": ("France", "france", True),
    "Germany": ("Germany", "germany", True),
    "Hong Kong": ("Hong Kong", "hong kong", True),
    "India": ("India", "india", True),
    "Indonesia": ("Indonesia", "indonesia", True),
    "Ireland": ("Ireland", "ireland", True),
    "Italy": ("Italy", "italy", True),
    "Mexico": ("Mexico", "mexico", True),
    "Netherlands": ("Netherlands", "netherlands", True),
    "New Zealand": ("New Zealand", "new zealand", True),
    "Singapore": ("Singapore", "singapore", True),
    "Spain": ("Spain", "spain", True),
    "Switzerland": ("Switzerland", "switzerland", True),
    "United Kingdom": ("United Kingdom", "uk", True),
    "United States": ("United States", "usa", True),
    "Vietnam": ("Vietnam", "vietnam", True),
    # Indeed only
    "Argentina": ("Argentina", "argentina", False),
    "Austria": ("Austria", "austria", False),
    "Bahrain": ("Bahrain", "bahrain", False),
    "Chile": ("Chile", "chile", False),
    "China": ("China", "china", False),
    "Colombia": ("Colombia", "colombia", False),
    "Costa Rica": ("Costa Rica", "costa rica", False),
    "Czech Republic": ("Czech Republic", "czech republic", False),
    "Denmark": ("Denmark", "denmark", False),
    "Ecuador": ("Ecuador", "ecuador", False),
    "Egypt": ("Egypt", "egypt", False),
    "Finland": ("Finland", "finland", False),
    "Greece": ("Greece", "greece", False),
    "Hungary": ("Hungary", "hungary", False),
    "Israel": ("Israel", "israel", False),
    "Japan": ("Japan", "japan", False),
    "Kuwait": ("Kuwait", "kuwait", False),
    "Luxembourg": ("Luxembourg", "luxembourg", False),
    "Malaysia": ("Malaysia", "malaysia", False),
    "Morocco": ("Morocco", "morocco", False),
    "Nigeria": ("Nigeria", "nigeria", False),
    "Norway": ("Norway", "norway", False),
    "Oman": ("Oman", "oman", False),
    "Pakistan": ("Pakistan", "pakistan", False),
    "Panama": ("Panama", "panama", False),
    "Peru": ("Peru", "peru", False),
    "Philippines": ("Philippines", "philippines", False),
    "Poland": ("Poland", "poland", False),
    "Portugal": ("Portugal", "portugal", False),
    "Qatar": ("Qatar", "qatar", False),
    "Romania": ("Romania", "romania", False),
    "Saudi Arabia": ("Saudi Arabia", "saudi arabia", False),
    "South Africa": ("South Africa", "south africa", False),
    "South Korea": ("South Korea", "south korea", False),
    "Sweden": ("Sweden", "sweden", False),
    "Taiwan": ("Taiwan", "taiwan", False),
    "Thailand": ("Thailand", "thailand", False),
    "Turkey": ("Turkey", "turkey", False),
    "Ukraine": ("Ukraine", "ukraine", False),
    "United Arab Emirates": ("United Arab Emirates", "united arab emirates", False),
    "Uruguay": ("Uruguay", "uruguay", False),
    "Venezuela": ("Venezuela", "venezuela", False),
}


def get_country_options() -> List[str]:
    """Get list of country display names for dropdown."""
    # Put "Global" first, then sorted countries
    countries = ["Global"] + sorted(COUNTRIES.keys())
    return countries


def get_indeed_country_name(country_name: str) -> str:
    """Get Indeed country name from display name."""
    if country_name in COUNTRIES:
        return COUNTRIES[country_name][1]
    return "usa"  # Default to usa


def has_glassdoor_support(country_name: str) -> bool:
    """Check if country supports Glassdoor scraping."""
    if country_name in COUNTRIES:
        return COUNTRIES[country_name][2]
    return False


def get_country_info(country_name: str) -> Tuple[str, str, bool]:
    """Get full country information."""
    if country_name in COUNTRIES:
        return COUNTRIES[country_name]
    return ("United States", "US", False)


def get_glassdoor_countries() -> List[str]:
    """Get list of countries that support Glassdoor."""
    return [name for name, (_, _, glassdoor) in COUNTRIES.items() if glassdoor]


def get_indeed_only_countries() -> List[str]:
    """Get list of countries that only support Indeed."""
    return [name for name, (_, _, glassdoor) in COUNTRIES.items() if not glassdoor]
