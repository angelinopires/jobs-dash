"""
Integration tests for the job scraping dashboard.

Tests key integration points between components.
"""

import unittest
import pandas as pd
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.optimized_indeed_scraper import get_indeed_scraper
from dashboard import apply_display_formatting, filter_by_salary_range


class TestIntegration(unittest.TestCase):
    """Integration tests for component interactions."""

    def setUp(self):
        """Set up test fixtures."""
        self.scraper = get_indeed_scraper()

    def test_scraper_to_display_pipeline(self):
        """Test the complete pipeline from scraping to display."""
        # Create mock scraped data
        mock_jobs = pd.DataFrame({
            'title': ['Senior Python Developer'],
            'company': ['TechCorp'],
            'location': ['Remote'],
            'date_posted': ['2024-01-15'],
            'site': ['indeed'],
            'job_url': ['https://example.com/job1'],
            'company_industry': ['Technology'],
            'company_num_employees': ['100-500']
        })

        # Process through scraper
        processed = self.scraper._process_jobs(mock_jobs)

        # Apply display formatting
        formatted = apply_display_formatting(processed)

        # Verify the pipeline worked
        self.assertIsInstance(formatted, pd.DataFrame)
        self.assertEqual(len(formatted), 1)
        self.assertIn('company_info', formatted.columns)

    def test_salary_filtering_on_processed_data(self):
        """Test salary filtering works on processed data."""
        test_data = pd.DataFrame({
            'title': ['High Pay Job', 'Low Pay Job'],
            'salary_formatted': ['$150,000', '$50,000'],
            'company_name': ['A', 'B'],
            'location_formatted': ['Remote', 'NYC']
        })

        # Test high salary filter
        filtered = filter_by_salary_range(test_data, '$100k-150k')
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered.iloc[0]['title'], 'High Pay Job')


if __name__ == '__main__':
    unittest.main()
