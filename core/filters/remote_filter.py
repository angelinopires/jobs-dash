"""
Remote job filtering module for identifying legitimate remote positions.

This module provides the RemoteJobFilter class that can identify and filter out
false remote positions (hybrid/on-site jobs) from job search results. The filter
uses pattern matching on job descriptions to detect contradictory language.

Key Features:
- Pattern-based filtering using readable regex definitions
- Comprehensive logging of filter decisions
- Integration with pandas DataFrames for job processing
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from .pattern_definitions import HIGH_CONFIDENCE_DISQUALIFIERS


class RemoteJobFilter:
    """
    Filter for identifying legitimate remote job positions using disqualifier-only approach.

    This filter uses a simplified approach focused on accessibility barriers:
    1. Check for ALL disqualifiers that create barriers for Latam workers
    2. If ANY disqualifier found → reject immediately
    3. If NO disqualifiers found → approve (assume accessible)

    The filter works by:
    1. Checking for citizenship, visa, location, travel, and other barriers
    2. ANY barrier = immediate rejection (no exceptions)
    3. No barriers = assume accessible from Latam
    4. Optionally saving filtered jobs for manual validation (debug mode)

    Attributes:
        negative_patterns: High-confidence patterns that disqualify remote claims
        debug_mode: Whether to save filtered jobs for validation
        debug_output_dir: Directory for saving debug output files
    """

    def __init__(self, debug_mode: bool = True):
        """
        Initialize the RemoteJobFilter with disqualifier-only filtering approach.

        Args:
            debug_mode: If True, saves filtered jobs to JSON files for validation.
                       Set to False in production to disable debug output.
        """
        self.negative_patterns = HIGH_CONFIDENCE_DISQUALIFIERS
        self.debug_mode = debug_mode
        self.debug_output_dir = Path("job_positions")

        # Ensure debug directory exists if debug mode is enabled
        if self.debug_mode:
            self.debug_output_dir.mkdir(exist_ok=True)

    def filter_false_remote_jobs(self, jobs_df: pd.DataFrame, country: str = "unknown") -> pd.DataFrame:
        """
        Filter out jobs that claim to be remote but aren't legitimate remote positions.

        This method applies pattern matching to job descriptions to identify
        jobs with contradictory language indicating hybrid, on-site, or
        location-restricted work arrangements.

        Args:
            jobs_df: DataFrame containing job data with 'description' column

        Returns:
            pd.DataFrame: Filtered DataFrame containing only legitimate remote jobs

        Raises:
            KeyError: If 'description' column is missing from the DataFrame
        """
        if jobs_df.empty:
            return jobs_df

        if "description" not in jobs_df.columns:
            raise KeyError("DataFrame must contain 'description' column for remote filtering")

        # Apply filtering to job descriptions
        mask = jobs_df["description"].apply(self.is_legitimate_remote)
        legitimate_remote_jobs = jobs_df[mask]
        filtered_jobs = jobs_df[~mask]

        # Save both legitimate and filtered jobs for validation if debug mode is enabled
        if self.debug_mode and (not legitimate_remote_jobs.empty or not filtered_jobs.empty):
            self._save_jobs_for_validation(legitimate_remote_jobs, filtered_jobs, len(jobs_df), country)

        return legitimate_remote_jobs

    def is_legitimate_remote(self, description: str) -> bool:
        """
        Check if a job description indicates legitimate remote work using disqualifier-only approach.

        This simplified approach focuses purely on accessibility barriers:
        1. Check ALL disqualifiers that create barriers for Latam workers
        2. If ANY disqualifier found → reject immediately (no exceptions)
        3. If NO disqualifiers found → approve (assume accessible from Latam)

        The philosophy: ANY barrier (citizenship, visa, location, travel, etc.)
        makes the job inaccessible from Latam, so we reject immediately.

        Args:
            description: Job description text to analyze

        Returns:
            bool: True if the job appears accessible from Latam, False if barriers exist
        """
        if not description or pd.isna(description):
            return True  # Fail-safe for missing descriptions

        description_str = str(description)

        # Check ALL disqualifiers first (Latam perspective)
        # ANY disqualifier is a deal-breaker due to visa/work authorization barriers
        all_disqualifiers = list(HIGH_CONFIDENCE_DISQUALIFIERS.keys())
        for pattern_name in all_disqualifiers:
            if pattern_name in self.negative_patterns:
                pattern = self.negative_patterns[pattern_name]
                if pattern.search(description_str):
                    return False  # ANY disqualifier = immediate rejection

        # No disqualifiers found = assume remote (conservative approach)
        # If we've eliminated all barriers, the job is accessible from Latam
        return True

    def get_matched_patterns(self, description: str) -> List[str]:
        """
        Get list of disqualifier patterns that matched for a job description.

        Useful for debugging and understanding why a job was filtered out.

        Args:
            description: Job description text to analyze

        Returns:
            List[str]: List of matched disqualifier pattern names
        """
        if not description or pd.isna(description):
            return []

        description_str = str(description)
        matched = []

        # Check disqualifier patterns only
        for name, pattern in self.negative_patterns.items():
            if pattern.search(description_str):
                matched.append(name)

        return matched

    def get_matched_snippets(self, description: str) -> List[str]:
        """
        Get list of disqualifier patterns that matched with actual text snippets.

        This method extracts the actual text that matched disqualifier patterns,
        making it easier to understand why a job was filtered out.

        Args:
            description: Job description text to analyze

        Returns:
            List[str]: List of matched text snippets from disqualifier patterns
        """
        if not description or pd.isna(description):
            return []

        description_str = str(description)
        matched_snippets: List[str] = []

        # Check disqualifier patterns and extract matched text
        for name, pattern in self.negative_patterns.items():
            match = pattern.search(description_str)
            if match:
                # Get the matched text, clean it up for display
                matched_text = match.group(0).strip()
                if len(matched_text) > 100:  # Truncate very long matches
                    matched_text = matched_text[:97] + "..."
                matched_snippets.append(matched_text)

        return matched_snippets

    def get_filter_reason(self, description: str) -> str:
        """
        Get human-readable reason why a job was filtered using disqualifier-only logic.

        Args:
            description: Job description text to analyze

        Returns:
            str: Human-readable explanation of filtering decision
        """
        if not description or pd.isna(description):
            return "No description available"

        description_str = str(description)

        # Check for disqualifiers
        for name, pattern in self.negative_patterns.items():
            if pattern.search(description_str):
                return f"Filtered: {name} detected"

        # No disqualifiers found
        return "Approved: No accessibility barriers detected"

    def _save_jobs_for_validation(
        self, legitimate_jobs_df: pd.DataFrame, filtered_jobs_df: pd.DataFrame, original_count: int, country: str
    ) -> None:
        """
        Save both legitimate remote jobs and filtered false remote jobs to JSON
        for comparison and validation (debug mode only).

        This method creates a timestamped JSON file containing both types of jobs
        with detailed analysis including matched patterns and text snippets,
        allowing for manual review and comparison to validate the filtering
        effectiveness and identify any false positives or negatives.

        Args:
            legitimate_jobs_df: DataFrame of jobs that passed the remote filter
            filtered_jobs_df: DataFrame of jobs that were filtered out as false remote
            original_count: Total number of jobs before filtering
            country: Country name for the search (used in filename)

        Note:
            Only executes when debug_mode is True. Files are saved to the
            job_positions directory with country and timestamp in filename.
            Includes actual text snippets that triggered pattern matches for debugging.
        """
        if not self.debug_mode:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Sanitize country name for filename (remove spaces, special chars)
        country_safe = country.replace(" ", "_").replace("/", "_").replace("\\", "_")
        filename = f"remote_jobs_comparison_{country_safe}_{timestamp}.json"
        filepath = self.debug_output_dir / filename

        # Prepare data for JSON serialization
        debug_data: Dict[str, Any] = {
            "metadata": {
                "exported_at": datetime.now().isoformat(),
                "total_original_jobs": original_count,
                "total_legitimate_remote_jobs": len(legitimate_jobs_df),
                "total_filtered_false_remote_jobs": len(filtered_jobs_df),
                "filtering_stats": {
                    "legitimate_percentage": (
                        f"{len(legitimate_jobs_df)/original_count*100:.1f}%" if original_count > 0 else "0%"
                    ),
                    "filtered_percentage": (
                        f"{len(filtered_jobs_df)/original_count*100:.1f}%" if original_count > 0 else "0%"
                    ),
                    "retention_rate": (
                        f"{len(legitimate_jobs_df)/original_count*100:.1f}%" if original_count > 0 else "0%"
                    ),
                },
                "purpose": "Comparison of legitimate remote jobs vs filtered false remote jobs",
                "note": "Review both sections to validate filter accuracy and identify false positives/negatives",
            },
            "legitimate_remote_jobs": [],
            "filtered_false_remote_jobs": [],
        }

        # Convert legitimate remote jobs to job data with analysis
        for _, job_row in legitimate_jobs_df.iterrows():
            description = job_row.get("description", "")

            job_data = {
                "id": job_row.get("id", "unknown"),
                "title": job_row.get("title", "Unknown Title"),
                "company": job_row.get("company", "Unknown Company"),
                "location": job_row.get("location", "Unknown Location"),
                "description": description,
                "matched_patterns": self.get_matched_patterns(description),
                "matched_disqualifiers": self.get_matched_snippets(description),
                "filter_reason": self.get_filter_reason(description),
            }
            debug_data["legitimate_remote_jobs"].append(job_data)

        # Convert filtered false remote jobs to job data with analysis
        for _, job_row in filtered_jobs_df.iterrows():
            description = job_row.get("description", "")

            job_data = {
                "id": job_row.get("id", "unknown"),
                "title": job_row.get("title", "Unknown Title"),
                "company": job_row.get("company", "Unknown Company"),
                "location": job_row.get("location", "Unknown Location"),
                "description": description,
                "matched_patterns": self.get_matched_patterns(description),
                "matched_disqualifiers": self.get_matched_snippets(description),
                "filter_reason": self.get_filter_reason(description),
            }
            debug_data["filtered_false_remote_jobs"].append(job_data)

        # Save to file with proper encoding
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(debug_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"⚠️  WARNING: Failed to save debug file {filepath}: {e}")

    def validate_patterns(self) -> Dict[str, Any]:
        """
        Validate that all disqualifier regex patterns compile correctly.

        This method tests the compilation and basic functionality of disqualifier patterns.
        Useful for ensuring pattern integrity during development.

        Returns:
            Dict[str, Any]: Validation results including pattern counts and any errors
        """
        validation_results: Dict[str, Any] = {
            "total_patterns": len(self.negative_patterns),
            "compilation_errors": [],
            "test_results": {},
        }

        # Test disqualifier patterns with sample text
        test_cases = {
            "disqualifier_test": "US citizenship required for this role",
        }

        for test_name, test_text in test_cases.items():
            try:
                # Test if any disqualifier pattern matches the test text
                matched = any(pattern.search(test_text) for pattern in self.negative_patterns.values())
                validation_results["test_results"][test_name] = {
                    "test_text": test_text,
                    "matched": matched,
                    "patterns_tested": len(self.negative_patterns),
                }
            except Exception as e:
                validation_results["compilation_errors"].append(f"{test_name}: {str(e)}")

        return validation_results
