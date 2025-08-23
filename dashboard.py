"""
Enhanced Job Dashboard - Indeed Focused
A comprehensive job search dashboard with enhanced Indeed scraper support.
"""

import streamlit as st
import pandas as pd
import time
from datetime import datetime
import os

# Import our custom modules
from scrapers.indeed_scraper import IndeedScraper
from config.countries import get_country_options, get_country_info
from config.remote_filters import (
    get_currency_options, get_job_type_options, get_remote_level_options,
    get_global_countries_display
)
from utils.time_filters import get_time_filter_options
from utils.toast import success_toast, error_toast, warning_toast, info_toast

# Configure the Streamlit page
st.set_page_config(
    page_title="🌍 Remote Job Search Dashboard",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'jobs_df' not in st.session_state:
    st.session_state.jobs_df = None
if 'search_history' not in st.session_state:
    st.session_state.search_history = []
if 'indeed_scraper' not in st.session_state:
    st.session_state.indeed_scraper = IndeedScraper()

# Clean up old search history format (remove entries with old structure)
if 'search_history' in st.session_state:
    st.session_state.search_history = [
        item for item in st.session_state.search_history 
        if 'where' in item or 'timestamp' in item  # Keep new format or at least timestamped items
    ]

def main():
    """Main dashboard function."""
    
    # Header
    st.title("🌍 Remote Job Search Dashboard")
    st.markdown("""
    **Remote-First Job Hunter** - Find the best remote opportunities across global markets.
    
    Search for remote jobs worldwide or target specific countries. Optimized for distributed teams and remote work.
    """)
    
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
    """Create the remote-first search sidebar."""
    st.header("🌍 Remote Job Search")
    
    # Search term
    search_term = st.text_input(
        "Search Term",
        value="Software Engineer",
        help="Enter job title or keywords (remote keywords will be added automatically)"
    )
    
    # Where (Global or specific country)
    country_options = get_country_options()
    selected_where = st.selectbox(
        "Where",
        options=country_options,
        index=0,  # "Global" is first
        help="Search globally across top remote-friendly countries or target a specific country"
    )
    
    # Show countries included in Global search
    if selected_where == "Global":
        st.info(f"🌍 **Global search includes:** {get_global_countries_display()}")
    
    # Job Type Filter
    job_type_options = get_job_type_options()
    selected_job_type = st.selectbox(
        "Job Type",
        options=job_type_options,
        index=job_type_options.index("Full-time") if "Full-time" in job_type_options else 0,
        help="Filter by employment type"
    )
    
    # Remote Level
    remote_level_options = get_remote_level_options()
    selected_remote_level = st.selectbox(
        "Remote Level", 
        options=remote_level_options,
        index=0,  # "Fully Remote" is default
        help="Specify remote work requirements"
    )
    
    # Note: Salary Currency moved to post-processing filters above results table
    
    # Time filter
    time_options = get_time_filter_options()
    time_filter = st.selectbox(
        "Job Posting Age",
        options=time_options,
        index=2,  # Default to "Past Week"
        help="Filter jobs by when they were posted"
    )
    
    # Fixed results count (no more slider)
    results_count = 20  # Default fixed value
    
    # Proxy settings
    with st.expander("🔒 Proxy Settings", expanded=False):
        use_proxies = st.checkbox(
            "Use Proxies",
            value=False,
            help="Enable proxy rotation to avoid rate limiting"
        )
        
        if use_proxies:
            proxy_input = st.text_area(
                "Proxy List (one per line)",
                value="http://proxy1:8080\nhttp://proxy2:8080",
                help="Format: protocol://host:port or user:pass@host:port"
            )
            
            proxy_list = [p.strip() for p in proxy_input.split('\n') if p.strip()]
            
            if st.button("Test Proxies"):
                with st.spinner("Testing proxy health..."):
                    # Simple proxy test (can be enhanced later)
                    st.success(f"✅ {len(proxy_list)} proxies configured")
        else:
            proxy_list = []
    
    # Search button
    search_clicked = st.button(
        "🌍 Search Remote Jobs",
        type="primary",
        use_container_width=True,
        help="Begin searching for remote jobs with the selected parameters"
    )
    
    # Handle search
    if search_clicked:
        perform_remote_job_search(
            search_term=search_term,
            where=selected_where,
            job_type=selected_job_type,
            remote_level=selected_remote_level,
            time_filter=time_filter,
            results_count=results_count,
            proxies=proxy_list if proxy_list else None
        )
    
    # Search history
    with st.expander("📚 Search History", expanded=False):
        if st.session_state.search_history:
            for i, history_item in enumerate(reversed(st.session_state.search_history[-5:])):  # Show last 5
                # Handle both old and new history format
                location_text = history_item.get('where', history_item.get('location', 'Unknown'))
                if st.button(f"🔍 {history_item['search_term']} in {location_text}", key=f"history_{i}"):
                    restore_search_from_history(history_item)
        else:
            st.info("No search history yet. Your searches will appear here.")

def perform_remote_job_search(search_term, where, job_type, remote_level, time_filter, results_count, proxies):
    """Perform the remote job search with enhanced error handling."""
    
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
        update_progress("Initializing remote job scraper...", 0.1)
        
        # Get scraper instance
        scraper = st.session_state.indeed_scraper
        
        # Perform search
        if where == "Global":
            update_progress("Starting global remote job search...", 0.2)
        else:
            update_progress(f"Searching remote jobs in {where}...", 0.2)
        
        result = scraper.search_jobs(
            search_term=search_term,
            where=where,
            job_type=job_type,
            remote_level=remote_level,
            time_filter=time_filter,
            results_wanted=results_count,
            proxies=proxies if proxies else None,
            progress_callback=update_progress
        )
        
        # Process results
        if result["success"]:
            if result["jobs"] is not None:
                # Store results
                st.session_state.jobs_df = result["jobs"]
                
                # Add to search history
                history_item = {
                    "timestamp": datetime.now().isoformat(),
                    "search_term": search_term,
                    "where": where,
                    "job_type": job_type,
                    "remote_level": remote_level,
                    "time_filter": time_filter,
                    "results_count": results_count,
                    "jobs_found": result["count"],
                    "search_time": result["search_time"],
                    "proxies_used": bool(proxies),
                    "search_type": result.get("metadata", {}).get("search_type", "unknown")
                }
                st.session_state.search_history.append(history_item)
                
                # Success toast
                success_toast(result['message'])
                st.balloons()
                
                # Show metrics
                if where == "Global":
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Remote Jobs Found", result["count"])
                    with col2:
                        st.metric("Search Time", f"{result['search_time']:.1f}s")
                    with col3:
                        countries_searched = result.get("metadata", {}).get("countries_searched", 0)
                        st.metric("Countries Searched", countries_searched)
                    with col4:
                        st.metric("Search Type", "Global")
                else:
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Remote Jobs Found", result["count"])
                    with col2:
                        st.metric("Search Time", f"{result['search_time']:.1f}s")
                    with col3:
                        st.metric("Country", where)
                
            else:
                warning_toast(result['message'])
                st.session_state.jobs_df = None
        else:
            error_toast(result['message'])
            st.session_state.jobs_df = None
            
    except Exception as e:
        error_toast(f"Unexpected error during remote job search: {str(e)}")
        st.session_state.jobs_df = None
    
    finally:
        # Clean up progress indicators
        progress_bar.empty()
        status_text.empty()

def display_toast_notifications():
    """Display toast notifications below the button."""
    from utils.toast import display_toasts
    display_toasts()

def restore_search_from_history(history_item):
    """Restore search parameters from history."""
    # Handle both old and new history format
    location_text = history_item.get('where', history_item.get('location', 'Unknown'))
    st.info(f"🔍 Restored search: {history_item['search_term']} in {location_text}")
    
    # Note: In a real implementation, you'd restore the actual search parameters
    # For now, just show the history item
    st.json(history_item)

def apply_interactive_filters(jobs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply interactive post-processing filters above the results table.
    These filters work on already scraped data and don't require re-scraping.
    
    Args:
        jobs_df: Original scraped jobs DataFrame
        
    Returns:
        Filtered DataFrame based on user selections
    """
    st.markdown("### 🎛️ Filter Results")
    st.markdown("*Adjust these filters to refine your search results without re-scraping*")
    
    # Create filter columns
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    
    # Initialize filtered dataframe
    filtered_df = jobs_df.copy()
    
    with filter_col1:
        # Salary Currency Filter
        currency_options = ['Any'] + ['USD', 'EUR', 'GBP', 'CAD', 'AUD', 'BRL']
        selected_currency = st.selectbox(
            "💰 Salary Currency",
            options=currency_options,
            index=0,
            key="currency_filter",
            help="Filter jobs by salary currency"
        )
        
        if selected_currency != 'Any':
            filtered_df = filter_by_salary_currency(filtered_df, selected_currency)
    
    with filter_col2:
        # Company Size Filter (placeholder for now)
        company_size_options = ['Any', 'Startup (1-50)', 'Small (51-200)', 'Medium (201-1000)', 'Large (1000+)']
        selected_company_size = st.selectbox(
            "🏢 Company Size",
            options=company_size_options,
            index=0,
            key="company_size_filter",
            help="Filter jobs by company size (coming soon)"
        )
    
    with filter_col3:
        # Salary Range Filter (placeholder for now)
        salary_range_options = ['Any', '$0-50k', '$50k-100k', '$100k-150k', '$150k+']
        selected_salary_range = st.selectbox(
            "💵 Salary Range",
            options=salary_range_options,
            index=0,
            key="salary_range_filter",
            help="Filter jobs by salary range (coming soon)"
        )
    
    with filter_col4:
        # Keywords Filter
        exclude_keywords = st.text_input(
            "🚫 Exclude Keywords",
            value="",
            key="exclude_keywords_filter",
            help="Exclude jobs containing these keywords (comma-separated)"
        )
        
        if exclude_keywords.strip():
            keywords = [k.strip().lower() for k in exclude_keywords.split(',') if k.strip()]
            for keyword in keywords:
                # Check in title and description
                title_mask = ~filtered_df['title'].fillna('').str.lower().str.contains(keyword, na=False)
                desc_mask = ~filtered_df.get('description', pd.Series([''] * len(filtered_df))).fillna('').str.lower().str.contains(keyword, na=False)
                filtered_df = filtered_df[title_mask & desc_mask]
    
    # Show filter summary
    if len(filtered_df) != len(jobs_df):
        st.info(f"🎯 Filters applied: {len(jobs_df) - len(filtered_df)} jobs filtered out")
    
    st.markdown("---")  # Separator line
    
    return filtered_df

def filter_by_salary_currency(jobs_df: pd.DataFrame, target_currency: str) -> pd.DataFrame:
    """
    Filter jobs by salary currency (post-processing filter).
    
    Args:
        jobs_df: Jobs DataFrame
        target_currency: Target currency code (USD, EUR, etc.)
        
    Returns:
        Filtered DataFrame
    """
    if target_currency == "Any" or jobs_df.empty:
        return jobs_df
    
    # Currency patterns for different formats (escaped for regex)
    currency_patterns = {
        'USD': [r'\$', 'USD', r'US\$', 'dollar'],
        'EUR': ['€', 'EUR', 'euro'],
        'GBP': ['£', 'GBP', 'pound', 'sterling'],
        'CAD': ['CAD', r'C\$', 'canadian'],
        'AUD': ['AUD', r'A\$', 'australian'],
        'BRL': [r'R\$', 'BRL', 'real', 'reais']
    }
    
    if target_currency.upper() not in currency_patterns:
        return jobs_df
    
    patterns = currency_patterns[target_currency.upper()]
    currency_mask = pd.Series([False] * len(jobs_df), index=jobs_df.index)
    
    # Check multiple columns for currency information
    currency_columns = ['currency', 'salary_currency', 'salary_formatted', 'compensation']
    
    for col in currency_columns:
        if col in jobs_df.columns:
            for pattern in patterns:
                currency_mask |= jobs_df[col].fillna('').astype(str).str.contains(
                    pattern, case=False, na=False
                )
    
    # Return only jobs that match the currency (strict filtering)
    return jobs_df[currency_mask] if currency_mask.any() else pd.DataFrame(columns=jobs_df.columns)

def display_search_results():
    """Display the search results with enhanced formatting and interactive filters."""
    jobs_df = st.session_state.jobs_df
    
    if jobs_df is None or jobs_df.empty:
        st.info("No jobs to display.")
        return
    
    # Results header
    st.header(f"📋 Search Results ({len(jobs_df)} jobs)")
    
    # Post-processing filters (interactive - don't require re-scraping)
    filtered_jobs_df = apply_interactive_filters(jobs_df)
    
    # Update header with filtered count if different
    if len(filtered_jobs_df) != len(jobs_df):
        st.subheader(f"🎯 Showing {len(filtered_jobs_df)} of {len(jobs_df)} jobs (filtered)")
    
    # Metrics dashboard
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Jobs", len(filtered_jobs_df))
    
    with col2:
        # Show time range of jobs if available
        if 'date_posted_formatted' in filtered_jobs_df.columns:
            recent_jobs = filtered_jobs_df['date_posted_formatted'].str.contains('ago|Just now', na=False).sum()
            st.metric("Recent Jobs", f"{recent_jobs} posted today")
        else:
            st.metric("All Remote", "✅")
    
    with col3:
        # Count jobs with salary info
        salary_count = 0
        if 'compensation' in filtered_jobs_df.columns:
            salary_count = filtered_jobs_df['compensation'].notna().sum()
        elif 'salary_formatted' in filtered_jobs_df.columns:
            salary_count = (filtered_jobs_df['salary_formatted'] != "Not specified").sum()
        st.metric("With Salary", salary_count)
    
    with col4:
        # Country count or most common location
        if 'country_name' in filtered_jobs_df.columns:
            unique_countries = filtered_jobs_df['country_name'].nunique()
            if unique_countries > 1:
                st.metric("Countries", unique_countries)
            else:
                # Single country - show most common location
                if 'location_formatted' in filtered_jobs_df.columns:
                    most_common_location = filtered_jobs_df['location_formatted'].mode().iloc[0] if not filtered_jobs_df['location_formatted'].empty else "N/A"
                    st.metric("Top Location", most_common_location[:20] + "..." if len(most_common_location) > 20 else most_common_location)
        else:
            st.metric("Source", "Indeed")
    
    # Results table
    st.subheader("🎯 Job Listings")
    
    # Define display columns (removed remote column since all jobs are remote now, added link column)
    display_columns = [
        'title', 'company_name', 'location_formatted', 
        'salary_formatted', 'date_posted_formatted', 'job_url'
    ]
    
    # Ensure all columns exist
    for col in display_columns:
        if col not in filtered_jobs_df.columns:
            filtered_jobs_df[col] = "N/A"
    
    # Display table
    st.dataframe(
        filtered_jobs_df[display_columns],
        use_container_width=True,
        hide_index=True,
        column_config={
            "title": st.column_config.TextColumn("Job Title", width="medium"),
            "company_name": st.column_config.TextColumn("Company", width="medium"),
            "location_formatted": st.column_config.TextColumn("Location", width="medium"),
            "salary_formatted": st.column_config.TextColumn("Salary", width="medium"),
            "date_posted_formatted": st.column_config.TextColumn("Posted", width="small"),
            "job_url": st.column_config.LinkColumn("Apply", width="small", display_text="🔗 Apply")
        }
    )
    
    # Job details panel
    st.subheader("🔍 Job Details")
    
    if 'title' in filtered_jobs_df.columns and not filtered_jobs_df['title'].isna().all():
        # Get unique job titles
        job_titles = filtered_jobs_df['title'].dropna().unique()
        
        if len(job_titles) > 0:
            selected_job_title = st.selectbox(
                "Select a job to view details:",
                options=job_titles,
                help="Choose a job to see its full description and details"
            )
            
            # Get selected job
            selected_job = filtered_jobs_df[filtered_jobs_df['title'] == selected_job_title].iloc[0]
            
            # Display job details
            with st.expander("📄 Full Job Details", expanded=True):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"**Job Title:** {selected_job.get('title', 'N/A')}")
                    st.markdown(f"**Company:** {selected_job.get('company_name', 'N/A')}")
                    st.markdown(f"**Location:** {selected_job.get('location_formatted', 'N/A')}")
                    st.markdown(f"**Posted:** {selected_job.get('date_posted', 'N/A')}")
                    st.markdown(f"**Remote:** {'Yes' if selected_job.get('is_remote') else 'No'}")
                
                with col2:
                    if 'salary_formatted' in selected_job and selected_job['salary_formatted'] != 'N/A':
                        st.markdown(f"**Salary:** {selected_job['salary_formatted']}")
                    
                    if 'company_info' in selected_job and selected_job['company_info'] != 'N/A':
                        st.markdown(f"**Company Info:** {selected_job['company_info']}")
                    
                    if 'job_url' in selected_job and selected_job['job_url']:
                        st.markdown(f"**Apply Here:** [View on Indeed]({selected_job['job_url']})")
                
                # Job description
                st.markdown("---")
                st.markdown("**Job Description:**")
                
                if 'description' in selected_job and selected_job['description']:
                    st.markdown(selected_job['description'], unsafe_allow_html=True)
                else:
                    st.info("No detailed description available for this job posting.")

def show_welcome_message():
    """Show welcome message and instructions."""
    st.info("💡 Welcome! Use the sidebar to configure your job search parameters and click 'Start Indeed Search' to begin.")
    
    # Show scraper information
    with st.expander("ℹ️ About This Dashboard", expanded=False):
        st.markdown("""
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
        - Cannot use time filters with job type filters simultaneously
        - Maximum 1000 jobs per search
        - Rate limiting may apply without proxies
        """)

if __name__ == "__main__":
    main()
