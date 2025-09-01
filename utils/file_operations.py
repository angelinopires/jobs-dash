"""
Atomic File Operations Utility

This module provides atomic file operations for thread-safe caching.
It includes corruption detection, deadlock prevention, and graceful error handling.

Think of this like a file system wrapper that ensures data integrity.
"""

import gzip
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class AtomicFileOperations:
    """
    Atomic File Operations for Thread-Safe Caching

    This class provides atomic file operations using temporary files
    and os.replace() for safe concurrent access.
    Similar to a file system transaction manager.
    """

    def __init__(
        self, cache_dir: str = "cache", max_retries: int = 3, retry_delay: float = 0.1, use_compression: bool = True
    ):
        """
        Initialize atomic file operations

        Args:
            cache_dir: Directory for cache files
            max_retries: Maximum retry attempts for file operations
            retry_delay: Delay between retry attempts in seconds
            use_compression: Whether to compress cache files
        """
        self.cache_dir = Path(cache_dir)
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.use_compression = use_compression

        # Thread safety
        self._file_locks: Dict[str, threading.Lock] = {}
        self._locks_lock = threading.Lock()

        # Ensure cache directory exists
        self._ensure_cache_dir()

    def _ensure_cache_dir(self) -> None:
        """Ensure cache directory exists"""
        try:
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Cache directory ensured: {self.cache_dir}")
        except Exception as e:
            logger.error(f"Failed to create cache directory {self.cache_dir}: {e}")
            raise

    def _get_file_lock(self, filename: str) -> threading.Lock:
        """
        Get or create a file-specific lock for thread safety

        Args:
            filename: Name of the file to lock

        Returns:
            threading.Lock: File-specific lock
        """
        with self._locks_lock:
            if filename not in self._file_locks:
                self._file_locks[filename] = threading.Lock()
            return self._file_locks[filename]

    def _get_cache_file_path(self, key: str) -> Path:
        """
        Get the full path for a cache file

        Args:
            key: Cache key

        Returns:
            Path: Full file path
        """
        # Sanitize key for filename
        safe_key = "".join(c for c in key if c.isalnum() or c in "._-")
        extension = ".json.gz" if self.use_compression else ".json"
        return self.cache_dir / f"{safe_key}{extension}"

    def atomic_write_json(self, key: str, data: Dict[str, Any]) -> bool:
        """
        Atomically write JSON data to file

        Args:
            key: Cache key
            data: Data to write

        Returns:
            bool: True if successful, False otherwise
        """
        file_path = self._get_cache_file_path(key)
        file_lock = self._get_file_lock(str(file_path))

        with file_lock:
            for attempt in range(self.max_retries):
                try:
                    # Create temporary file
                    suffix = ".tmp.gz" if self.use_compression else ".tmp"
                    with tempfile.NamedTemporaryFile(
                        mode="wb" if self.use_compression else "w", dir=self.cache_dir, delete=False, suffix=suffix
                    ) as temp_file:
                        # Write data to temporary file
                        if self.use_compression:
                            # Compress data
                            json_str = json.dumps(data, indent=2, default=str)
                            compressed_data = gzip.compress(json_str.encode("utf-8"))
                            temp_file.write(compressed_data)
                        else:
                            # Write uncompressed
                            json.dump(data, temp_file, indent=2, default=str)

                        temp_file.flush()
                        os.fsync(temp_file.fileno())  # Ensure data is written to disk

                        # Atomically replace the original file
                        os.replace(temp_file.name, str(file_path))

                        logger.debug(f"Successfully wrote cache file: {file_path}")
                        return True

                except Exception as e:
                    logger.warning(f"File write attempt {attempt + 1} failed for {key}: {e}")

                    # Clean up temporary file if it exists
                    try:
                        if "temp_file" in locals():
                            os.unlink(temp_file.name)
                    except OSError:
                        pass  # File might not exist

                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2**attempt))  # Exponential backoff
                    else:
                        logger.error(f"Failed to write cache file after {self.max_retries} attempts: {key}")
                        return False

        return False

    def atomic_read_json(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Atomically read JSON data from file

        Args:
            key: Cache key

        Returns:
            Dict: Cached data or None if not found/corrupted
        """
        file_path = self._get_cache_file_path(key)
        file_lock = self._get_file_lock(str(file_path))

        with file_lock:
            if not file_path.exists():
                logger.debug(f"Cache file not found: {file_path}")
                return None

            for attempt in range(self.max_retries):
                try:
                    # Read file content
                    if self.use_compression:
                        # Read compressed data
                        with open(file_path, "rb") as f:
                            compressed_data = f.read()
                            json_str = gzip.decompress(compressed_data).decode("utf-8")
                            data = json.loads(json_str)
                    else:
                        # Read uncompressed data
                        with open(file_path, "r", encoding="utf-8") as f:
                            data = json.load(f)

                    # Validate data structure
                    if not isinstance(data, dict):
                        logger.warning(f"Invalid data structure in cache file: {key}")
                        return None

                    logger.debug(f"Successfully read cache file: {file_path}")
                    return data

                except json.JSONDecodeError as e:
                    logger.warning(f"JSON decode error in cache file {key} (attempt {attempt + 1}): {e}")
                    if attempt == self.max_retries - 1:
                        logger.error(f"Cache file corrupted, removing: {key}")
                        self._remove_corrupted_file(file_path)
                    return None

                except Exception as e:
                    logger.warning(f"File read attempt {attempt + 1} failed for {key}: {e}")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay * (2**attempt))
                    else:
                        return None

        return None

    def atomic_delete(self, key: str) -> bool:
        """
        Atomically delete a cache file

        Args:
            key: Cache key

        Returns:
            bool: True if successful, False otherwise
        """
        file_path = self._get_cache_file_path(key)
        file_lock = self._get_file_lock(str(file_path))

        with file_lock:
            try:
                if file_path.exists():
                    file_path.unlink()
                    logger.debug(f"Successfully deleted cache file: {file_path}")
                return True

            except Exception as e:
                logger.error(f"Failed to delete cache file {key}: {e}")
                return False

    def exists(self, key: str) -> bool:
        """
        Check if a cache file exists

        Args:
            key: Cache key

        Returns:
            bool: True if file exists, False otherwise
        """
        file_path = self._get_cache_file_path(key)
        return file_path.exists()

    def _remove_corrupted_file(self, file_path: Path) -> None:
        """
        Remove a corrupted cache file

        Args:
            file_path: Path to the corrupted file
        """
        try:
            if file_path.exists():
                file_path.unlink()
                logger.info(f"Removed corrupted cache file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to remove corrupted file {file_path}: {e}")

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get cache directory statistics

        Returns:
            Dict: Cache statistics including file count and total size
        """
        try:
            if not self.cache_dir.exists():
                return {"file_count": 0, "total_size_bytes": 0, "cache_dir": str(self.cache_dir), "exists": False}

            # Handle both compressed and uncompressed files
            json_files = list(self.cache_dir.glob("*.json"))
            gz_files = list(self.cache_dir.glob("*.json.gz"))
            files = json_files + gz_files
            total_size = sum(f.stat().st_size for f in files if f.is_file())

            return {
                "file_count": len(files),
                "total_size_bytes": total_size,
                "cache_dir": str(self.cache_dir),
                "exists": True,
            }

        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {
                "file_count": 0,
                "total_size_bytes": 0,
                "cache_dir": str(self.cache_dir),
                "exists": False,
                "error": str(e),
            }

    def clear_cache(self) -> bool:
        """
        Clear all cache files

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.cache_dir.exists():
                return True

            # Remove all cache files in cache directory
            json_files = list(self.cache_dir.glob("*.json"))
            gz_files = list(self.cache_dir.glob("*.json.gz"))
            files = json_files + gz_files
            for file_path in files:
                try:
                    file_path.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete cache file {file_path}: {e}")

            logger.info(f"Cleared {len(files)} cache files")
            return True

        except Exception as e:
            logger.error(f"Failed to clear cache: {e}")
            return False

    def cleanup_old_files(self, max_age_hours: int = 24) -> int:
        """
        Clean up old cache files

        Args:
            max_age_hours: Maximum age of files in hours

        Returns:
            int: Number of files removed
        """
        try:
            if not self.cache_dir.exists():
                return 0

            current_time = time.time()
            max_age_seconds = max_age_hours * 3600
            removed_count = 0

            # Handle both compressed and uncompressed files
            json_files = list(self.cache_dir.glob("*.json"))
            gz_files = list(self.cache_dir.glob("*.json.gz"))
            files = json_files + gz_files
            for file_path in files:
                try:
                    file_age = current_time - file_path.stat().st_mtime
                    if file_age > max_age_seconds:
                        file_path.unlink()
                        removed_count += 1
                        logger.debug(f"Removed old cache file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to check/remove old file {file_path}: {e}")

            if removed_count > 0:
                logger.info(f"Cleaned up {removed_count} old cache files")

            return removed_count

        except Exception as e:
            logger.error(f"Failed to cleanup old files: {e}")
            return 0
