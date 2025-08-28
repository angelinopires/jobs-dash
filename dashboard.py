"""
Enhanced Job Dashboard - Indeed Focused
A comprehensive job search dashboard with enhanced Indeed scraper support.
"""

import pandas as pd
import streamlit as st

from config.countries import get_country_options
from config.remote_filters import enhance_search_term_with_remote_keywords, get_global_countries_display
from scrapers.optimized_indeed_scraper import get_indeed_scraper
from utils.display_utils import clean_company_info, clean_display_value, format_posted_date_enhanced
from utils.time_filters import get_time_filter_options
from utils.toast import error_toast, warning_toast

# Configure the Streamlit page
st.set_page_config(
    page_title="Jobs Dash - Your personal job search assistant",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize session state
if "jobs_df" not in st.session_state:
    st.session_state.jobs_df = None
if "search_metadata" not in st.session_state:
    st.session_state.search_metadata = None
if "indeed_scraper" not in st.session_state:
    st.session_state.indeed_scraper = get_indeed_scraper()


def main():
    """Main dashboard function."""

    # Header
    st.title("🌍 Jobs Dash")
    st.markdown(
        """
    **Remote-First Job Hunter** - Find the best remote opportunities across global markets.

    Search for remote jobs worldwide or target specific countries. Optimized for distributed teams and remote work.
    """
    )

    # Sidebar
    with st.sidebar:
        create_search_sidebar()

    # Toast notifications area (below button)
    display_toast_notifications()

    # Main content
    if st.session_state.jobs_df is not None:
        display_search_results()
    else:
        show_welcome_message()


def create_search_sidebar():
    """Create search sidebar."""
    st.header("🌍 Jobs Dash")

    # Search parameters
    search_term = st.text_input(
        "Search parameters",
        value="Software Engineer",
        help="""
            **Search Tips:**
            - "Exact Phrase": Searches for the complete phrase.
            - "-word": Removes results containing a given word.
            - Example: "Software Engineer" -manager
                -  It will find jobs for "Software Engineer" but exclude any that also mention "manager".
            - Extra tip: This search includes both job title and job description.
        """,
    )

    # Available platforms
    platforms = st.pills(
        "Plaforms",
        ["Indeed", "Glassdoor", "LinkedIn", "ZipRecruiter"],
        default=["Indeed"],
        selection_mode="multi",
    )

    # Remote Only
    include_remote = st.toggle("Remote Only", value=True, help="Include only remote positions")

    # Where
    country_options = get_country_options()
    selected_where = st.selectbox(
        "Where",
        options=country_options,
        index=0,  # "Global" is first
        help="Search globally across top remote-friendly countries or target a specific country",
    )

    # Show countries included in Global search
    if selected_where == "Global":
        st.info(f"**Global search includes:** {get_global_countries_display()}.")

    # Time filter
    time_options = get_time_filter_options()
    time_filter = st.selectbox(
        "Job Posting Age",
        options=time_options,
        index=0,  # Default to "Last 24h"
        help="Filter jobs by posting age",
    )

    st.info(
        "**Date Filter:** Filters jobs by when they were originally posted. "
        "Jobs may have been refreshed/reposted recently but show older creation dates."
    )

    # Search button
    search_clicked = st.button(
        ":material/travel_explore: Search Jobs",
        type="primary",
        use_container_width=True,
    )

    # Handle search
    if search_clicked:
        perform_remote_job_search(
            search_term=search_term,
            where=selected_where,
            include_remote=include_remote,
            time_filter=time_filter,
            platforms=platforms,
        )


def perform_remote_job_search(search_term, where, include_remote, time_filter, platforms):
    """Perform the job search with enhanced error handling."""

    # Validate inputs
    if not search_term.strip():
        error_toast("Please enter a search term")
        return

    # Show progress
    progress_bar = st.progress(0)
    status_text = st.empty()

    def update_progress(message, progress=None):
        if progress is not None:
            # Ensure progress is between 0 and 1
            progress_val = max(0.0, min(1.0, float(progress)))
            progress_bar.progress(progress_val)
        status_text.text(message)

    try:
        # Start search
        update_progress("Initializing job scraper...", 0.1)

        # Get scraper instance
        scraper = st.session_state.indeed_scraper

        # Enhance search term with remote keywords if checkbox is checked
        final_search_term = search_term
        if include_remote:
            final_search_term = enhance_search_term_with_remote_keywords(search_term)

        # Perform search
        if where == "Global":
            update_progress("Starting global job search...", 0.2)
        else:
            update_progress(f"Searching jobs in {where}...", 0.2)

        result = scraper.search_jobs(
            search_term=final_search_term,
            where=where,
            include_remote=include_remote,
            time_filter=time_filter,
            progress_callback=update_progress,
        )

        # Process results
        if result["success"]:
            if result["jobs"] is not None:
                # Store results and metadata
                st.session_state.jobs_df = result["jobs"]
                st.session_state.search_metadata = {
                    "search_term": search_term,
                    "where": where,
                    "count": result["count"],
                    "search_time": result["search_time"],
                    "time_filter": time_filter,
                    "metadata": result.get("metadata", {}),
                }

                # Show balloons for successful search
                st.balloons()

            else:
                warning_toast(result["message"])
                st.session_state.jobs_df = None
                st.session_state.search_metadata = None
        else:
            error_toast(result["message"])
            st.session_state.jobs_df = None
            st.session_state.search_metadata = None

    except Exception as e:
        error_toast(f"Unexpected error during remote job search: {str(e)}")
        st.session_state.jobs_df = None
        st.session_state.search_metadata = None

    finally:
        # Clean up progress indicators
        progress_bar.empty()
        status_text.empty()


def display_toast_notifications():
    """Display toast notifications below the button."""
    from utils.toast import display_toasts

    display_toasts()


def apply_display_formatting(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply display formatting and sorting to the jobs dataframe.

    This function:
    - Formats job type fields according to display rules
    - Formats posted dates to "Aug 23, 2025" format
    - Applies default sorting: Salary (DESC) then Job Title (ASC)

    Args:
        jobs_df: Input jobs DataFrame

    Returns:
        Formatted and sorted DataFrame
    """
    if jobs_df.empty:
        return jobs_df

    formatted_df = jobs_df.copy()

    # Format job type fields according to the mapping rules
    job_type_mapping = {
        "parttime": "Part-time",
        "fulltime": "Full-time",
        "internship": "Internship",
        "temporary": "Temporary",
        "contract": "Contract",
        "none": "Not specified",
        # Also handle display names that might already be formatted
        "Part-time": "Part-time",
        "Full-time": "Full-time",
        "Internship": "Internship",
        "Temporary": "Temporary",
        "Contract": "Contract",
        "Not specified": "Not specified",
    }

    if "job_type" in formatted_df.columns:
        formatted_df["job_type"] = formatted_df["job_type"].fillna("none").astype(str).str.lower()
        formatted_df["job_type"] = formatted_df["job_type"].map(job_type_mapping).fillna("Not specified")

    # Format posted dates to "Aug 23, 2025" format
    if "date_posted" in formatted_df.columns:
        formatted_df["date_posted_formatted"] = formatted_df["date_posted"].apply(format_posted_date_enhanced)
    elif "date_posted_formatted" in formatted_df.columns:
        # Re-format existing formatted dates
        formatted_df["date_posted_formatted"] = formatted_df["date_posted_formatted"].apply(format_posted_date_enhanced)

    # Apply default sorting: Salary (DESC) then Date Posted (DESC)
    try:
        # Prepare salary sorting column
        if "salary_formatted" in formatted_df.columns:
            formatted_df["_salary_sort_key"] = formatted_df["salary_formatted"].apply(_extract_salary_for_sorting)
        else:
            formatted_df["_salary_sort_key"] = 0

        # Prepare date sorting column
        if "date_posted" in formatted_df.columns:
            formatted_df["_date_sort_key"] = pd.to_datetime(formatted_df["date_posted"], errors="coerce")
        else:
            formatted_df["_date_sort_key"] = pd.Timestamp.min

        # Sorting: Salary DESC, then Date Posted DESC
        sort_columns = ["_salary_sort_key", "_date_sort_key"]
        sort_ascending = [
            False,
            False,
        ]  # Both descending (highest salary first, newest date first)

        formatted_df = formatted_df.sort_values(sort_columns, ascending=sort_ascending, na_position="last")

        # Remove temporary sorting columns
        formatted_df = formatted_df.drop(columns=["_salary_sort_key", "_date_sort_key"], errors="ignore")

    except Exception:
        # Fallback to date posted sorting if there are issues
        if "date_posted" in formatted_df.columns:
            formatted_df = formatted_df.sort_values("date_posted", ascending=False, na_position="last")

    return formatted_df


def _extract_salary_for_sorting(salary_str):
    """
    Extract numeric value from salary string for sorting purposes.

    Args:
        salary_str: Salary string like "$80,000 - $120,000" or "Not specified"

    Returns:
        Numeric value for sorting (highest salaries sort first)
    """
    if (
        pd.isna(salary_str)
        or (salary_str is None)
        or (isinstance(salary_str, str) and not salary_str.strip())
        or salary_str in ["N/A", "Not specified", ""]
    ):
        return 0

    try:
        import re

        # Remove currency symbols and extract all numbers
        clean_salary = re.sub(r"[^\d,\.\s-]", " ", str(salary_str))
        numbers = re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?", clean_salary)

        if numbers:
            # Convert to numeric values
            salary_values = []
            for num in numbers:
                clean_num = num.replace(",", "")
                try:
                    if "." in clean_num:
                        salary_values.append(float(clean_num))
                    else:
                        salary_values.append(int(clean_num))
                except Exception:
                    continue

            if salary_values:
                # Use the maximum value found for sorting (to prioritize higher salaries)
                max_salary = max(salary_values)

                # Handle hourly rates (convert to annual estimate)
                if max_salary < 1000:  # Likely hourly
                    return max_salary * 40 * 52  # Convert to annual
                else:
                    return max_salary
    except Exception:
        pass

    return 0


def apply_interactive_filters(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply interactive post-processing filters above the results table.
    These filters work on already scraped data and don't require re-scraping.

    Args:
        jobs_df: Original scraped jobs DataFrame (should already be formatted)

    Returns:
        Filtered DataFrame based on user selections
    """
    st.markdown("### 🎛️ Filter Results")
    st.markdown("*Adjust these filters to refine your search results without re-scraping*")

    filter_col1, filter_col2, filter_col3, filter_col4, filter_col5 = st.columns(5)

    # Initialize filtered dataframe
    filtered_df = jobs_df.copy()

    # Get available job types from the original formatted data (not from filtered_df)
    standard_job_types = [
        "Full-time",
        "Part-time",
        "Contract",
        "Internship",
        "Temporary",
        "Not specified",
    ]

    if "job_type" in jobs_df.columns:
        available_in_data = jobs_df["job_type"].dropna().unique().tolist()
        # Only show job types that actually exist in the data
        available_job_types = [jt for jt in standard_job_types if jt in available_in_data]
    else:
        available_job_types = standard_job_types

    with filter_col1:
        # Job Title Filter
        job_title_filter = st.text_input(
            "🔍 Position Title",
            value="",
            key="job_title_filter",
            help="Filter jobs by title keywords (case-insensitive)",
        )

        if job_title_filter.strip():
            title_keywords = [k.strip().lower() for k in job_title_filter.split() if k.strip()]
            for keyword in title_keywords:
                title_mask = filtered_df["title"].fillna("").str.lower().str.contains(keyword, na=False)
                filtered_df = filtered_df[title_mask]

    with filter_col2:
        # Exclude Keywords Filter
        exclude_keywords = st.text_input(
            "🚫 Exclude Keywords",
            value="",
            key="exclude_keywords_filter",
            help="Exclude jobs containing these keywords (comma-separated)",
        )

        if exclude_keywords.strip():
            keywords = [k.strip().lower() for k in exclude_keywords.split(",") if k.strip()]
            for keyword in keywords:
                # Check in title and description
                title_mask = ~filtered_df["title"].fillna("").str.lower().str.contains(keyword, na=False)
                desc_mask = (
                    ~filtered_df.get("description", pd.Series([""] * len(filtered_df)))
                    .fillna("")
                    .str.lower()
                    .str.contains(keyword, na=False)
                )
                filtered_df = filtered_df[title_mask & desc_mask]

    with filter_col3:
        # Salary Range Filter
        salary_range_options = ["Any", "$0-50k", "$50k-100k", "$100k-150k", "$150k+"]
        selected_salary_range = st.selectbox(
            "💵 Salary Range",
            options=salary_range_options,
            index=0,
            key="salary_range_filter",
            help="Filter jobs by salary range",
        )

        # Apply salary range filter
        if selected_salary_range != "Any":
            filtered_df = filter_by_salary_range(filtered_df, selected_salary_range)

    with filter_col4:
        # Location Filter
        if "location_formatted" in filtered_df.columns:
            available_locations = filtered_df["location_formatted"].dropna().unique().tolist()
            # Remove 'N/A' values and sort
            available_locations = sorted([loc for loc in available_locations if loc and loc != "N/A"])
        else:
            available_locations = []

        selected_locations = st.multiselect(
            "📍 Location",
            options=available_locations,
            default=[],
            key="location_filter",
            help="Filter jobs by location (select multiple)",
        )

        # Apply location filter
        if selected_locations and "location_formatted" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["location_formatted"].isin(selected_locations)]

    with filter_col5:
        # Job Type Filter - use pre-calculated available options
        selected_job_types = st.multiselect(
            "💼 Job Type",
            options=available_job_types,
            default=[],
            key="job_type_filter",
            help="Filter jobs by employment type (select multiple)",
        )

        # Apply job type filter
        if selected_job_types and "job_type" in filtered_df.columns:
            filtered_df = filtered_df[filtered_df["job_type"].isin(selected_job_types)]

    return filtered_df


def display_search_results():
    """Display the search results with enhanced formatting and interactive filters."""
    jobs_df = st.session_state.jobs_df

    if jobs_df is None or jobs_df.empty:
        st.info("No jobs to display.")
        return

    # Search Statistics and Parameters
    if st.session_state.search_metadata:
        metadata = st.session_state.search_metadata
        search_meta = metadata.get("metadata", {})

        st.divider()

    # Apply formatting and sorting improvements first
    formatted_jobs_df = apply_display_formatting(jobs_df)

    # Results table header - always show job count
    st.subheader(f"🎯 Search Results ({len(jobs_df)} jobs)")

    # Display search parameters below the subheader
    if st.session_state.search_metadata:
        metadata = st.session_state.search_metadata
        search_meta = metadata.get("metadata", {})

        # Create columns for better layout
        col1, col2, col3, col4 = st.columns(4)

        # Search term
        with col1:
            st.markdown("##### Search Query")
            st.success(f"**{metadata['search_term']}**")

        # Location/Countries
        with col2:
            st.markdown("##### Location")
            if metadata["where"] == "Global":
                countries_searched = search_meta.get("countries_searched", 0)
                st.success(f"**Global ({countries_searched} countries)**")
            else:
                st.success(f"**{metadata['where']}**")

        # Job posting age
        with col3:
            st.markdown("##### Job Posting Age")
            st.success(f"**{metadata['time_filter']}**")

        # Search time
        with col4:
            st.markdown("##### Search Time")
            st.success(f"**{metadata['search_time']:.1f}s**")

    # Post-processing filters
    with st.expander("🎛️ Filter Results", expanded=True):
        filtered_jobs_df = apply_interactive_filters(formatted_jobs_df)

    # Define display columns (added job_type after title)
    display_columns = [
        "title",
        "job_type",
        "company_name",
        "location_formatted",
        "salary_formatted",
        "date_posted_formatted",
        "job_url",
    ]

    # Ensure all columns exist
    for col in display_columns:
        if col not in filtered_jobs_df.columns:
            filtered_jobs_df[col] = "N/A"

    # Show filter summary
    if len(filtered_jobs_df) != len(jobs_df):
        st.info(f"🎯 Filters applied: {len(filtered_jobs_df)} of {len(jobs_df)} jobs are visible")

    # Display table
    st.dataframe(
        filtered_jobs_df[display_columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            "title": st.column_config.TextColumn("Job Title", width="medium"),
            "job_type": st.column_config.TextColumn("Job Type", width="small"),
            "company_name": st.column_config.TextColumn("Company", width="small"),  # Minimized width
            "location_formatted": st.column_config.TextColumn("Location", width="small"),  # Already minimal
            "salary_formatted": st.column_config.TextColumn("Salary", width="medium"),
            "date_posted_formatted": st.column_config.TextColumn("Posted", width="medium"),  # Increased for new format
            "job_url": st.column_config.LinkColumn("Link", width=80, display_text="Link"),
        },
    )

    # Job details panel
    st.subheader("🔍 Job Details")

    if "title" in filtered_jobs_df.columns and not filtered_jobs_df["title"].isna().all():
        # Get unique job titles
        job_titles = filtered_jobs_df["title"].dropna().unique()

        if len(job_titles) > 0:
            selected_job_title = st.selectbox(
                "Select a job to view details:",
                options=job_titles,
                help="Choose a job to see its full description and details",
            )

            # Get selected job
            selected_job = filtered_jobs_df[filtered_jobs_df["title"] == selected_job_title].iloc[0]

            # Display job details
            with st.expander("📄 Full Job Details", expanded=True):
                col1, col2 = st.columns(2)

                with col1:
                    st.markdown(f"**Job Title:** {clean_display_value(selected_job.get('title'))}")
                    st.markdown(f"**Company:** {clean_display_value(selected_job.get('company_name'))}")
                    st.markdown(f"**Location:** {clean_display_value(selected_job.get('location_formatted'))}")

                    # Format the posted date using the same function as the table
                    posted_date = selected_job.get("date_posted")
                    formatted_date = format_posted_date_enhanced(posted_date)
                    st.markdown(f"**Posted:** {clean_display_value(formatted_date)}")

                    # Remote status
                    is_remote = selected_job.get("is_remote", False)
                    remote_text = "Yes" if is_remote else "No"
                    st.markdown(f"**Remote:** {remote_text}")

                with col2:
                    # Salary info
                    salary = clean_display_value(selected_job.get("salary_formatted"))
                    if salary != "Not available":
                        st.markdown(f"**Salary:** {salary}")

                    # Company info (special handling for structured data)
                    company_info = clean_company_info(selected_job.get("company_info"))
                    if company_info != "Not available":
                        st.markdown(f"**Company Info:** {company_info}")

                    # Job type
                    job_type = clean_display_value(selected_job.get("job_type"))
                    if job_type != "Not available":
                        st.markdown(f"**Job Type:** {job_type}")

                    # Apply link
                    job_url = selected_job.get("job_url")
                    if job_url and clean_display_value(job_url) != "Not available":
                        st.markdown(f"**Apply Here:** [View on Indeed]({job_url})")

                # Job description
                st.markdown("---")
                st.markdown("**Job Description:**")

                description = clean_display_value(selected_job.get("description"), default="")
                if description and description != "Not available":
                    st.markdown(description, unsafe_allow_html=True)
                else:
                    st.info("No detailed description available for this job posting.")


def show_welcome_message():
    """Show welcome message and instructions."""
    st.info(
        "💡 Welcome! Use the sidebar to configure your job search parameters and click 'Start Indeed Search' to begin."
    )

    # Show scraper information
    with st.expander("ℹ️ About This Dashboard", expanded=False):
        st.markdown(
            """
        **Enhanced Indeed Job Scraper**

        This dashboard provides enhanced Indeed scraping with:
        - **60+ Country Support**: Search jobs worldwide
        - **Advanced Filtering**: Remote work, posting age, location radius
        - **Proxy Support**: Rotate IPs to avoid rate limiting
        - **Better Error Handling**: Clear messages and progress tracking
        - **Search History**: Save and reuse search parameters

        **Supported Parameters:**
        - Search term and location
        - Country selection with Indeed code mapping
        - Distance radius (5-100 miles)
        - Remote work filtering
        - Time-based filtering (24h, 72h, 1 week, no filter)
        - Results count (5-100 jobs)
        - Proxy rotation

        **Limitations:**
        - Maximum 1000 jobs per search
        - Rate limiting may apply without proxies
        - Time filters are based on original post date (not refresh/update date)
        - Job type filtering is done via post-processing for better accuracy
        """
        )


def filter_by_salary_range(jobs_df: pd.DataFrame, salary_range: str) -> pd.DataFrame:
    """
    Filter jobs by salary range.

    Args:
        jobs_df: Jobs DataFrame
        salary_range: Salary range like '$0-50k', '$50k-100k', etc.

    Returns:
        Filtered DataFrame
    """
    if salary_range == "Any" or jobs_df.empty:
        return jobs_df

    # Extract salary ranges
    range_mapping = {
        "$0-50k": (0, 50000),
        "$50k-100k": (50000, 100000),
        "$100k-150k": (100000, 150000),
        "$150k+": (150000, float("inf")),
    }

    if salary_range not in range_mapping:
        return jobs_df

    min_salary, max_salary = range_mapping[salary_range]

    filtered_df = jobs_df.copy()

    # Look for salary information in various columns
    salary_columns = ["min_amount", "max_amount", "salary_min", "salary_max"]

    # Try to filter using numeric salary columns
    for col in salary_columns:
        if col in filtered_df.columns:
            try:
                # Convert to numeric and filter
                numeric_values = pd.to_numeric(filtered_df[col], errors="coerce")
                if col.startswith("min") or col.endswith("min"):
                    # For minimum salary columns, check if min is within range
                    mask = (numeric_values >= min_salary) & (numeric_values <= max_salary)
                else:
                    # For maximum salary columns, check if max is within or above range
                    mask = numeric_values >= min_salary
                    if max_salary != float("inf"):
                        mask &= numeric_values <= max_salary

                filtered_df = filtered_df[mask.fillna(False)]
                break
            except Exception:
                continue

    # If no numeric filtering worked, try text-based filtering
    if len(filtered_df) == len(jobs_df) and "salary_formatted" in filtered_df.columns:
        salary_text = filtered_df["salary_formatted"].fillna("").astype(str)

        # Extract numbers from salary text
        import re

        mask = pd.Series([False] * len(filtered_df), index=filtered_df.index)

        for idx, salary_str in salary_text.items():
            # Remove currency symbols and extract all numbers
            clean_salary = re.sub(
                r"[^\d,\.\s-]", " ", salary_str
            )  # Remove all non-numeric chars except comma, dot, space, dash
            numbers = re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?", clean_salary)  # Find numbers with commas

            if numbers:
                try:
                    # Convert to integers/floats
                    salary_values = []
                    for num in numbers:
                        clean_num = num.replace(",", "")
                        if "." in clean_num:
                            salary_values.append(float(clean_num))
                        else:
                            salary_values.append(int(clean_num))

                    # Check if any salary value falls within our range
                    for salary_val in salary_values:
                        # Handle different salary formats (hourly, yearly, etc.)
                        if salary_val < 1000:  # Likely hourly rate
                            annual_salary = salary_val * 40 * 52  # Convert to annual
                        else:
                            annual_salary = salary_val

                        if min_salary <= annual_salary <= max_salary:
                            mask.iloc[mask.index.get_loc(idx)] = True
                            break
                except Exception:
                    continue

        if mask.any():
            filtered_df = filtered_df[mask]

    return filtered_df


if __name__ == "__main__":
    main()
# Test comment for pre-commit
# Test formatting consistency
