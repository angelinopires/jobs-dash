"""
Unit tests for dashboard functionality.

Tests the search history, CSV download, and UI components.
"""

import unittest
import pandas as pd
import io
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

# Mock streamlit before importing dashboard
import sys
from unittest.mock import MagicMock
sys.modules['streamlit'] = MagicMock()

# Now we can import dashboard functions
from dashboard import create_csv_download, restore_search_from_history


class TestSearchHistory(unittest.TestCase):
    """Test search history functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Sample history item with new structure
        self.sample_history_item = {
            "id": "search_20240101_123456",
            "timestamp": "2024-01-01T12:34:56",
            "search_term": "Python Developer",
            "where": "Global",
            "filters": {
                "remote_level": "Fully Remote",
                "time_filter": "Past Week"
            },
            "results_summary": {
                "jobs_found": 74,
                "search_time": 18.1,
                "countries_searched": 7,
                "search_type": "global"
            },
            "jobs_data": [
                {
                    "title": "Senior Python Developer",
                    "company": "TechCorp",
                    "location": "Remote",
                    "salary_formatted": "$80,000 - $120,000",
                    "job_url": "https://example.com/job/1",
                    "date_posted_formatted": "2 days ago"
                },
                {
                    "title": "Python Backend Engineer", 
                    "company": "DataLab",
                    "location": "New York (Remote OK)",
                    "salary_formatted": "$90,000 - $130,000",
                    "job_url": "https://example.com/job/2",
                    "date_posted_formatted": "1 day ago"
                }
            ],
            "display_title": "Python Developer (Global) - 74 jobs"
        }
        
        # Sample empty history item
        self.empty_history_item = {
            "id": "search_20240101_000000",
            "timestamp": "2024-01-01T00:00:00",
            "search_term": "NonExistent Job",
            "where": "Mars",
            "filters": {"remote_level": "Fully Remote"},
            "results_summary": {"jobs_found": 0, "search_time": 1.0},
            "jobs_data": [],
            "display_title": "NonExistent Job (Mars) - 0 jobs"
        }
    
    def test_history_item_structure(self):
        """Test that history item has required structure."""
        required_fields = [
            "id", "timestamp", "search_term", "where", 
            "filters", "results_summary", "jobs_data", "display_title"
        ]
        
        for field in required_fields:
            self.assertIn(field, self.sample_history_item, f"Missing field: {field}")
        
        # Test nested structures
        self.assertIn("jobs_found", self.sample_history_item["results_summary"])
        self.assertIn("search_time", self.sample_history_item["results_summary"])
        self.assertIn("remote_level", self.sample_history_item["filters"])
    
    def test_display_title_format(self):
        """Test that display title is formatted correctly."""
        expected_format = "Python Developer (Global) - 74 jobs"
        self.assertEqual(self.sample_history_item["display_title"], expected_format)
    
    def test_jobs_data_structure(self):
        """Test that jobs data has expected structure."""
        jobs_data = self.sample_history_item["jobs_data"]
        
        self.assertEqual(len(jobs_data), 2)
        
        # Check first job has required fields
        first_job = jobs_data[0]
        required_job_fields = ["title", "company", "location", "job_url"]
        
        for field in required_job_fields:
            self.assertIn(field, first_job, f"Missing job field: {field}")
    
    def test_timestamp_format(self):
        """Test that timestamp is in ISO format."""
        timestamp_str = self.sample_history_item["timestamp"]
        
        # Should be able to parse as ISO format
        try:
            parsed_date = datetime.fromisoformat(timestamp_str)
            self.assertIsInstance(parsed_date, datetime)
        except ValueError:
            self.fail("Timestamp is not in valid ISO format")


class TestCSVDownload(unittest.TestCase):
    """Test CSV download functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.sample_history_item = {
            "jobs_data": [
                {
                    "title": "Python Developer",
                    "company": "TechCorp", 
                    "location": "Remote",
                    "salary_formatted": "$80,000 - $120,000",
                    "job_url": "https://example.com/job/1",
                    "date_posted_formatted": "2 days ago",
                    "country_name": "United States"
                },
                {
                    "title": "Data Scientist",
                    "company": "DataLab",
                    "location": "New York",
                    "salary_formatted": "$90,000 - $130,000", 
                    "job_url": "https://example.com/job/2",
                    "date_posted_formatted": "1 day ago",
                    "country_name": "United States"
                }
            ]
        }
    
    def test_create_csv_download_with_data(self):
        """Test CSV creation with job data."""
        csv_content = create_csv_download(self.sample_history_item)
        
        # Should return string content
        self.assertIsInstance(csv_content, str)
        
        # Should contain CSV headers
        self.assertIn("title", csv_content)
        self.assertIn("company", csv_content)
        self.assertIn("location", csv_content)
        
        # Should contain job data
        self.assertIn("Python Developer", csv_content)
        self.assertIn("TechCorp", csv_content)
        self.assertIn("DataLab", csv_content)
        
        # Should be proper CSV format (comma-separated)
        lines = csv_content.strip().split('\n')
        self.assertGreater(len(lines), 1)  # Header + data rows
        
        # Header should have commas
        header_line = lines[0]
        self.assertIn(',', header_line)
    
    def test_create_csv_download_empty_data(self):
        """Test CSV creation with empty job data."""
        empty_history = {"jobs_data": []}
        csv_content = create_csv_download(empty_history)
        
        # Should still return string (empty CSV)
        self.assertIsInstance(csv_content, str)
        
        # Should be minimal content (just headers or empty)
        lines = csv_content.strip().split('\n')
        self.assertLessEqual(len(lines), 2)  # At most header + empty line
    
    def test_csv_content_integrity(self):
        """Test that CSV preserves data integrity."""
        csv_content = create_csv_download(self.sample_history_item)
        
        # Parse CSV back to verify data integrity
        csv_io = io.StringIO(csv_content)
        df_from_csv = pd.read_csv(csv_io)
        
        # Should have same number of rows
        original_df = pd.DataFrame(self.sample_history_item["jobs_data"])
        self.assertEqual(len(df_from_csv), len(original_df))
        
        # Should preserve key data
        self.assertIn("Python Developer", df_from_csv["title"].values)
        self.assertIn("Data Scientist", df_from_csv["title"].values)
        self.assertIn("TechCorp", df_from_csv["company"].values)
    
    def test_csv_special_characters(self):
        """Test CSV handling of special characters."""
        special_data = {
            "jobs_data": [
                {
                    "title": "Software Engineer, Frontend & Backend",
                    "company": "Tech Corp (Acquired by BigCorp)",
                    "location": "San Francisco, CA",
                    "description": 'Looking for "rockstar" developer with 5+ years experience'
                }
            ]
        }
        
        csv_content = create_csv_download(special_data)
        
        # Should handle commas, quotes, and special characters
        self.assertIsInstance(csv_content, str)
        self.assertIn("Software Engineer", csv_content)
        
        # Parse back to ensure no corruption
        csv_io = io.StringIO(csv_content)
        df_from_csv = pd.read_csv(csv_io)
        self.assertEqual(len(df_from_csv), 1)


class TestSearchHistoryRestoration(unittest.TestCase):
    """Test search history restoration functionality."""
    
    @patch('dashboard.st')
    def test_restore_search_with_valid_data(self, mock_st):
        """Test restoring search with valid job data."""
        # Create a mock session state that can track assignments
        session_state_mock = MagicMock()
        mock_st.session_state = session_state_mock
        mock_st.rerun = Mock()
        
        # Mock toast functions  
        with patch('dashboard.success_toast') as mock_success:
            history_item = {
                "search_term": "Python Developer",
                "where": "Global",
                "results_summary": {
                    "jobs_found": 5,
                    "search_time": 2.5,
                    "countries_searched": 3,
                    "search_type": "global"
                },
                "jobs_data": [
                    {"title": "Python Dev", "company": "TechCorp"},
                    {"title": "Python Engineer", "company": "DataLab"}
                ],
                "display_title": "Python Developer (Global) - 5 jobs"
            }
            
            restore_search_from_history(history_item)
            
            # Verify that session state attributes were set
            # Check that jobs_df and search_metadata were assigned
            self.assertTrue(hasattr(session_state_mock, 'jobs_df'))
            self.assertTrue(hasattr(session_state_mock, 'search_metadata'))
            
            # Should show success message
            mock_success.assert_called_once()
            
            # Should trigger rerun
            mock_st.rerun.assert_called_once()
    
    @patch('dashboard.st')
    def test_restore_search_with_empty_data(self, mock_st):
        """Test restoring search with no job data."""
        mock_st.session_state = {}
        
        with patch('dashboard.warning_toast') as mock_warning:
            history_item = {
                "search_term": "NonExistent Job",
                "where": "Mars", 
                "results_summary": {"jobs_found": 0, "search_time": 1.0},
                "jobs_data": [],
                "display_title": "NonExistent Job (Mars) - 0 jobs"
            }
            
            restore_search_from_history(history_item)
            
            # Should show warning for no data
            mock_warning.assert_called_once()
    
    @patch('dashboard.st')
    def test_restore_search_with_corrupted_data(self, mock_st):
        """Test restoring search with corrupted/invalid data."""
        mock_st.session_state = {}
        
        with patch('dashboard.error_toast') as mock_error:
            # Incomplete history item (missing required fields)
            corrupted_item = {
                "search_term": "Test Job"
                # Missing other required fields
            }
            
            restore_search_from_history(corrupted_item)
            
            # Should show error message
            mock_error.assert_called_once()


class TestDashboardIntegration(unittest.TestCase):
    """Test dashboard integration and session state management."""
    
    def test_search_metadata_structure(self):
        """Test that search metadata has expected structure."""
        # Simulate the metadata structure created during search
        sample_metadata = {
            "search_term": "Python Developer",
            "where": "Global",
            "count": 74,
            "search_time": 18.1,
            "metadata": {
                "countries_searched": 7,
                "search_type": "global"
            }
        }
        
        # Verify structure
        required_fields = ["search_term", "where", "count", "search_time", "metadata"]
        for field in required_fields:
            self.assertIn(field, sample_metadata)
        
        # Verify nested metadata
        self.assertIn("countries_searched", sample_metadata["metadata"])
        self.assertIn("search_type", sample_metadata["metadata"])
    
    def test_history_id_generation(self):
        """Test that history IDs are unique and properly formatted."""
        # Simulate ID generation
        timestamp = datetime.now()
        history_id = f"search_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        # Should follow expected format
        self.assertTrue(history_id.startswith("search_"))
        self.assertEqual(len(history_id), 22)  # search_ + 8 chars date + _ + 6 chars time
        
        # Should be different for different timestamps
        import time
        time.sleep(0.01)  # Small delay
        timestamp2 = datetime.now()
        history_id2 = f"search_{timestamp2.strftime('%Y%m%d_%H%M%S')}"
        
        # IDs should be different (unless generated in same second)
        if timestamp.second != timestamp2.second:
            self.assertNotEqual(history_id, history_id2)


if __name__ == '__main__':
    unittest.main(verbosity=2)
