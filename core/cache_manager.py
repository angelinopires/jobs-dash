"""
Smart caching system for job search results.

Implements Option C strategy: Streamlit session state + file backup
- Fast access via session state
- Persistence via file cache
- Automatic expiration (15 minutes default)
- Cache key generation based on search parameters
"""

import hashlib
import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional, cast

import pandas as pd
import streamlit as st


class CacheManager:
    """
    Hybrid caching system optimized for Streamlit use.

    Features:
    - Session-based cache for fast access
    - File-based backup for persistence across restarts
    - Configurable TTL (default: 15 minutes)
    - Smart cache key generation
    - Automatic cleanup of expired entries
    """

    def __init__(self, cache_ttl_minutes: float = 15.0, cache_dir: str = "cache") -> None:
        self.cache_ttl_minutes = cache_ttl_minutes
        self.cache_dir = cache_dir
        self.cache_file = os.path.join(cache_dir, "search_results.json")

        # Ensure cache directory exists
        os.makedirs(cache_dir, exist_ok=True)

        # Initialize session cache if not exists (thread-safe)
        self._init_session_cache()

        # Load file cache into session on first run
        self._load_file_cache_to_session()

    def _init_session_cache(self) -> None:
        """Initialize session cache in a thread-safe way."""
        try:
            if "job_search_cache" not in st.session_state:
                st.session_state.job_search_cache = {}
        except Exception:
            # Fallback when running in threads or outside Streamlit context
            pass

    def generate_cache_key(
        self, scraper: str, search_term: str, country: str, include_remote: bool, **kwargs: Any
    ) -> str:
        """
        Generate a unique cache key based on search parameters.

        Args:
            scraper: Name of the scraper (indeed, linkedin, etc.)
            search_term: Job search keywords
            country: Target country or "Global"
            include_remote: Whether remote-only filter is enabled
            **kwargs: Other search parameters

        Returns:
            Unique cache key string
        """
        # Create a deterministic key from parameters
        key_data = {
            "scraper": scraper.lower().strip(),
            "search_term": search_term.lower().strip(),
            "country": country.lower().strip(),
            "include_remote": include_remote,
            **kwargs,
        }

        # Convert to sorted JSON string for consistency
        key_string = json.dumps(key_data, sort_keys=True)

        # Generate MD5 hash for compact key
        key_hash = hashlib.md5(key_string.encode()).hexdigest()

        return f"{scraper}_{key_hash}"

    def get_cached_result(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached result if it exists and hasn't expired.

        Args:
            cache_key: The cache key to look up

        Returns:
            Cached result dict or None if not found/expired
        """
        # Check session cache first (fastest) - thread-safe
        try:
            if cache_key in st.session_state.job_search_cache:
                cache_entry = st.session_state.job_search_cache[cache_key]

                if self._is_cache_valid(cache_entry):
                    # Convert jobs back to DataFrame if needed
                    if "jobs_data" in cache_entry["result"]:
                        cache_entry["result"]["jobs"] = pd.DataFrame(cache_entry["result"]["jobs_data"])

                    return cast(Dict[str, Any], cache_entry["result"])
                else:
                    # Expired - remove from session cache
                    del st.session_state.job_search_cache[cache_key]
        except Exception:
            # Fallback to file cache when session state is not available
            pass

        # Fallback to file cache
        return self._get_cached_result_from_file(cache_key)

    def get_cache_entry_info(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Get cache entry metadata (timestamp, etc.) without the full result.

        Args:
            cache_key: The cache key to look up

        Returns:
            Cache entry metadata or None if not found
        """
        # Check session cache first - thread-safe
        try:
            if cache_key in st.session_state.job_search_cache:
                cache_entry = st.session_state.job_search_cache[cache_key]

                if self._is_cache_valid(cache_entry):
                    return {"timestamp": cache_entry["timestamp"], "ttl_minutes": self.cache_ttl_minutes}
        except Exception:
            # Fallback to file cache when session state is not available
            pass

        # Fallback to file cache
        return self._get_cache_entry_info_from_file(cache_key)

    def cache_result(self, cache_key: str, result: Dict[str, Any]) -> None:
        """
        Store search result in both session and file cache.

        Args:
            cache_key: Unique key for this search
            result: Search result to cache
        """
        # Prepare cache entry
        cache_entry = {"timestamp": datetime.now().isoformat(), "result": result.copy()}

        # Convert DataFrame to serializable format for file cache
        result_dict = cast(Dict[str, Any], cache_entry["result"])
        if "jobs" in result_dict and isinstance(result_dict["jobs"], pd.DataFrame):
            # Convert DataFrame to JSON-serializable format
            jobs_df = result_dict["jobs"]

            # Convert datetime columns to strings to avoid JSON serialization issues
            jobs_data = jobs_df.copy()
            for col in jobs_data.columns:
                if pd.api.types.is_datetime64_any_dtype(jobs_data[col]):
                    jobs_data[col] = jobs_data[col].astype(str)

            result_dict["jobs_data"] = jobs_data.to_dict("records")
            # Keep DataFrame in session cache, remove for file cache
            result_dict["jobs"] = None

        # Store in session cache (keeps DataFrame) - thread-safe
        try:
            session_entry = {"timestamp": cache_entry["timestamp"], "result": result}  # Original result with DataFrame
            st.session_state.job_search_cache[cache_key] = session_entry
        except Exception:
            # Fallback when running in threads or outside Streamlit context
            pass

        # Store in file cache (serializable version)
        self._save_to_file_cache(cache_key, cache_entry)

        # Clean up expired entries periodically
        self._cleanup_expired_entries()

    def _is_cache_valid(self, cache_entry: Dict[str, Any]) -> bool:
        """Check if cache entry is still valid (not expired)."""
        timestamp = datetime.fromisoformat(cache_entry["timestamp"])
        expiry_time = timestamp + timedelta(minutes=self.cache_ttl_minutes)
        return datetime.now() < expiry_time

    def _load_file_cache_to_session(self) -> None:
        """Load valid entries from file cache into session cache."""
        if not os.path.exists(self.cache_file):
            return

        try:
            with open(self.cache_file, "r") as f:
                file_cache = json.load(f)

            # Load only valid (non-expired) entries - thread-safe
            try:
                for cache_key, cache_entry in file_cache.items():
                    if self._is_cache_valid(cache_entry):
                        # Convert jobs_data back to DataFrame for session
                        if "jobs_data" in cache_entry["result"]:
                            cache_entry["result"]["jobs"] = pd.DataFrame(cache_entry["result"]["jobs_data"])

                        st.session_state.job_search_cache[cache_key] = cache_entry
            except Exception:
                # Fallback when running in threads or outside Streamlit context
                pass

        except (json.JSONDecodeError, FileNotFoundError, KeyError):
            # If file is corrupted or missing, start fresh
            pass

    def _save_to_file_cache(self, cache_key: str, cache_entry: Dict[str, Any]) -> None:
        """Save cache entry to file for persistence."""
        try:
            # Load existing file cache
            file_cache = {}
            if os.path.exists(self.cache_file):
                try:
                    with open(self.cache_file, "r") as f:
                        file_cache = json.load(f)
                except json.JSONDecodeError:
                    # If file is corrupted, start fresh
                    print("Warning: Cache file corrupted, creating new cache")
                    file_cache = {}

            # Add new entry
            file_cache[cache_key] = cache_entry

            # Save back to file
            with open(self.cache_file, "w") as f:
                json.dump(file_cache, f, indent=2)

        except Exception as e:
            # Don't let file cache errors break the application
            print(f"Warning: Could not save to file cache: {e}")
            # If there's a persistent issue, clear the cache file
            if "JSON" in str(e) or "Expecting value" in str(e):
                self._clear_corrupted_cache()

    def _cleanup_expired_entries(self) -> None:
        """Remove expired entries from both session and file cache."""
        # Clean session cache - thread-safe
        try:
            expired_keys = []
            for cache_key, cache_entry in st.session_state.job_search_cache.items():
                if not self._is_cache_valid(cache_entry):
                    expired_keys.append(cache_key)

            for key in expired_keys:
                del st.session_state.job_search_cache[key]
        except Exception:
            # Fallback when running in threads or outside Streamlit context
            pass

        # Clean file cache
        self._cleanup_file_cache()

    def _cleanup_file_cache(self) -> None:
        """Remove expired entries from file cache."""
        if not os.path.exists(self.cache_file):
            return

        try:
            with open(self.cache_file, "r") as f:
                file_cache = json.load(f)

            # Keep only valid entries
            valid_cache = {key: entry for key, entry in file_cache.items() if self._is_cache_valid(entry)}

            # Save cleaned cache back to file
            with open(self.cache_file, "w") as f:
                json.dump(valid_cache, f, indent=2)

        except json.JSONDecodeError:
            print("Warning: Cache file corrupted during cleanup, clearing cache")
            self._clear_corrupted_cache()
        except Exception as e:
            print(f"Warning: Could not clean file cache: {e}")

    def _clear_corrupted_cache(self) -> None:
        """Clear corrupted cache file and start fresh."""
        try:
            if os.path.exists(self.cache_file):
                os.remove(self.cache_file)
            print("✅ Corrupted cache cleared, starting fresh")
        except Exception as e:
            print(f"Warning: Could not clear corrupted cache: {e}")

    def clear_scraper_cache(self, scraper_name: str) -> None:
        """Clear all cached results for a specific scraper."""
        # Clear from session cache - thread-safe
        try:
            keys_to_remove = [
                key for key in st.session_state.job_search_cache.keys() if key.startswith(f"{scraper_name}_")
            ]

            for key in keys_to_remove:
                del st.session_state.job_search_cache[key]
        except Exception:
            # Fallback when running in threads or outside Streamlit context
            pass

        # Clear from file cache
        self._clear_scraper_file_cache(scraper_name)

    def _clear_scraper_file_cache(self, scraper_name: str) -> None:
        """Clear scraper entries from file cache."""
        if not os.path.exists(self.cache_file):
            return

        try:
            with open(self.cache_file, "r") as f:
                file_cache = json.load(f)

            # Remove scraper entries
            cleaned_cache = {key: entry for key, entry in file_cache.items() if not key.startswith(f"{scraper_name}_")}

            # Save cleaned cache
            with open(self.cache_file, "w") as f:
                json.dump(cleaned_cache, f, indent=2)

        except Exception as e:
            print(f"Warning: Could not clear scraper cache: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get statistics about current cache usage."""
        # Get session count - thread-safe
        try:
            session_count = len(st.session_state.job_search_cache)
        except Exception:
            session_count = 0

        # Count file cache entries
        file_count = 0
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    file_cache = json.load(f)
                file_count = len(file_cache)
            except Exception:
                pass

        return {
            "session_entries": session_count,
            "file_entries": file_count,
            "cache_ttl_minutes": self.cache_ttl_minutes,
            "cache_file": self.cache_file,
        }

    def _get_cached_result_from_file(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Get cached result from file cache (fallback for threads).

        Args:
            cache_key: The cache key to look up

        Returns:
            Cached result dict or None if not found/expired
        """
        if not os.path.exists(self.cache_file):
            return None

        try:
            with open(self.cache_file, "r") as f:
                file_cache = json.load(f)

            if cache_key in file_cache:
                cache_entry = file_cache[cache_key]

                if self._is_cache_valid(cache_entry):
                    # Convert jobs back to DataFrame if needed
                    if "jobs_data" in cache_entry["result"]:
                        cache_entry["result"]["jobs"] = pd.DataFrame(cache_entry["result"]["jobs_data"])

                    return cast(Dict[str, Any], cache_entry["result"])
        except Exception:
            pass

        return None

    def _get_cache_entry_info_from_file(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """
        Get cache entry metadata from file cache (fallback for threads).

        Args:
            cache_key: The cache key to look up

        Returns:
            Cache entry metadata or None if not found
        """
        if not os.path.exists(self.cache_file):
            return None

        try:
            with open(self.cache_file, "r") as f:
                file_cache = json.load(f)

            if cache_key in file_cache:
                cache_entry = file_cache[cache_key]

                if self._is_cache_valid(cache_entry):
                    return {"timestamp": cache_entry["timestamp"], "ttl_minutes": self.cache_ttl_minutes}
        except Exception:
            pass

        return None
