"""
File Cache Manager

Provides reliable file-based caching with atomic operations and hybrid cache keys.
Serves as the fallback layer when Redis is unavailable.
"""

import hashlib
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from utils.cache_serialization import create_cache_serializer

logger = logging.getLogger(__name__)


class FileCacheManager:
    """
    File-based cache manager with atomic operations and hybrid cache keys.

    Features:
    - Hybrid cache keys (human-readable + hash)
    - Atomic file operations using temporary files
    - Automatic TTL management
    - Corruption detection and recovery
    - Organized file structure by scraper/date
    """

    def __init__(
        self,
        cache_dir: str = "cache",
        ttl_hours: int = 24,
        max_cache_size_mb: int = 50,
        compression_enabled: bool = True,
    ):
        """
        Initialize the file cache manager.

        Args:
            cache_dir: Directory to store cache files
            ttl_hours: Time-to-live for cache entries (hours)
            max_cache_size_mb: Maximum cache size in MB
            compression_enabled: Whether to use gzip compression
        """
        self.cache_dir = Path(cache_dir)
        self.ttl_hours = ttl_hours
        self.max_cache_size_mb = max_cache_size_mb
        self.max_cache_size_bytes = max_cache_size_mb * 1024 * 1024

        # Initialize serializer
        self.serializer = create_cache_serializer(compression_enabled)

        # Ensure cache directory exists
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        self._create_cache_structure()

        # Cache statistics
        self._stats = {"hits": 0, "misses": 0, "writes": 0, "deletions": 0, "errors": 0}

    def _create_cache_structure(self) -> None:
        """Create the cache directory structure."""
        try:
            # Create main cache directory
            self.cache_dir.mkdir(parents=True, exist_ok=True)

            # Create subdirectories for different scrapers
            scrapers = ["indeed", "linkedin", "glassdoor"]
            for scraper in scrapers:
                (self.cache_dir / scraper).mkdir(exist_ok=True)

            # Create metadata file
            metadata_file = self.cache_dir / "cache_metadata.json"
            if not metadata_file.exists():
                self._save_metadata(
                    {
                        "created_at": datetime.now().isoformat(),
                        "total_entries": 0,
                        "total_size_bytes": 0,
                        "last_cleanup": datetime.now().isoformat(),
                    }
                )

            logger.info(f"File cache structure created in {self.cache_dir}")

        except Exception as e:
            logger.error(f"Failed to create cache structure: {e}")

    def _generate_hybrid_key(self, scraper: str, search_term: str, location: str, **kwargs: Any) -> str:
        """
        Generate a hybrid cache key (human-readable + hash).

        Format: {scraper}_{search_term}_{location}_{hash}
        Example: indeed_software_engineer_global_a1b2c3d4

        Args:
            scraper: Name of the scraper
            search_term: Job search term
            location: Search location
            **kwargs: Additional search parameters

        Returns:
            Hybrid cache key string
        """
        # Create human-readable part
        readable_parts = [
            scraper.lower().strip(),
            search_term.lower().strip().replace(" ", "_"),
            location.lower().strip().replace(" ", "_"),
        ]

        # Add additional parameters
        for key, value in sorted(kwargs.items()):
            if value is not None:
                readable_parts.append(f"{key}_{str(value).lower()}")

        # Create deterministic key data for hashing
        key_data = {
            "scraper": scraper.lower().strip(),
            "search_term": search_term.lower().strip(),
            "location": location.lower().strip(),
            **{k: str(v).lower() for k, v in kwargs.items() if v is not None},
        }

        # Generate hash
        key_string = json.dumps(key_data, sort_keys=True)
        key_hash = hashlib.md5(key_string.encode()).hexdigest()[:8]  # First 8 chars

        # Combine readable parts with hash
        hybrid_key = "_".join(readable_parts) + "_" + key_hash

        return hybrid_key

    def _get_cache_file_path(self, cache_key: str, scraper: str) -> Path:
        """Get the full file path for a cache key."""
        return self.cache_dir / scraper / f"{cache_key}{self.serializer.get_file_extension()}"

    def get(self, scraper: str, search_term: str, location: str, **kwargs: Any) -> Optional[Dict[str, Any]]:
        """
        Retrieve cached data from file cache.

        Args:
            scraper: Name of the scraper
            search_term: Job search term
            location: Search location
            **kwargs: Additional search parameters

        Returns:
            Cached data with metadata or None if not found/expired
        """
        try:
            cache_key = self._generate_hybrid_key(scraper, search_term, location, **kwargs)
            cache_file = self._get_cache_file_path(cache_key, scraper)

            if not cache_file.exists():
                self._stats["misses"] += 1
                return None

            # Check if file is expired
            if self._is_file_expired(cache_file):
                logger.debug(f"Cache file expired: {cache_file}")
                self.delete(scraper, search_term, location, **kwargs)
                self._stats["misses"] += 1
                return None

            # Read and deserialize file
            with open(cache_file, "rb") as f:
                serialized_data = f.read()

            # Deserialize data
            cache_data = self.serializer.deserialize(serialized_data)

            # Update access metadata
            self._update_access_metadata(cache_file, cache_data)

            self._stats["hits"] += 1
            logger.debug(f"File cache hit: {cache_key}")

            return cache_data

        except Exception as e:
            logger.error(f"Error reading from file cache: {e}")
            self._stats["errors"] += 1
            return None

    def set(
        self,
        scraper: str,
        search_term: str,
        location: str,
        data: Any,
        metadata: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> bool:
        """
        Store data in file cache using atomic operations.

        Args:
            scraper: Name of the scraper
            search_term: Job search term
            location: Search location
            data: Data to cache
            metadata: Optional metadata
            **kwargs: Additional search parameters

        Returns:
            True if successful, False otherwise
        """
        try:
            cache_key = self._generate_hybrid_key(scraper, search_term, location, **kwargs)
            cache_file = self._get_cache_file_path(cache_key, scraper)

            # Ensure scraper directory exists
            cache_file.parent.mkdir(parents=True, exist_ok=True)

            # Create temporary file
            temp_file = cache_file.with_suffix(".tmp")

            # Serialize data
            serialized_data = self.serializer.serialize(data, metadata)

            # Write to temporary file first
            with open(temp_file, "wb") as f:
                f.write(serialized_data)

            # Atomic move (this is the atomic operation)
            temp_file.replace(cache_file)

            # Update metadata
            self._update_cache_metadata()

            self._stats["writes"] += 1
            logger.debug(f"File cache write successful: {cache_key}")

            return True

        except Exception as e:
            logger.error(f"Error writing to file cache: {e}")
            self._stats["errors"] += 1

            # Clean up temporary file if it exists
            if "temp_file" in locals() and temp_file.exists():
                try:
                    temp_file.unlink()
                except Exception:
                    pass

            return False

    def delete(self, scraper: str, search_term: str, location: str, **kwargs: Any) -> bool:
        """
        Delete a cache entry.

        Args:
            scraper: Name of the scraper
            search_term: Job search term
            location: Search location
            **kwargs: Additional search parameters

        Returns:
            True if successful, False otherwise
        """
        try:
            cache_key = self._generate_hybrid_key(scraper, search_term, location, **kwargs)
            cache_file = self._get_cache_file_path(cache_key, scraper)

            if cache_file.exists():
                cache_file.unlink()
                self._stats["deletions"] += 1
                logger.debug(f"Deleted cache file: {cache_file}")

                # Update metadata
                self._update_cache_metadata()
                return True

            return False

        except Exception as e:
            logger.error(f"Error deleting cache file: {e}")
            self._stats["errors"] += 1
            return False

    def _is_file_expired(self, cache_file: Path) -> bool:
        """Check if a cache file has expired based on TTL."""
        try:
            file_age = datetime.now() - datetime.fromtimestamp(cache_file.stat().st_mtime)
            return file_age > timedelta(hours=self.ttl_hours)
        except Exception:
            return True  # Consider expired if we can't check

    def _update_access_metadata(self, cache_file: Path, cache_data: Dict[str, Any]) -> None:
        """Update access metadata for a cache file."""
        try:
            if "metadata" in cache_data:
                cache_data["metadata"]["last_accessed"] = datetime.now().isoformat()
                cache_data["metadata"]["access_count"] = cache_data["metadata"].get("access_count", 0) + 1

                # Rewrite file with updated metadata
                serialized_data = self.serializer.serialize(cache_data["data"], cache_data["metadata"])

                with open(cache_file, "wb") as f:
                    f.write(serialized_data)

        except Exception as e:
            logger.debug(f"Failed to update access metadata: {e}")

    def _update_cache_metadata(self) -> None:
        """Update the main cache metadata file."""
        try:
            metadata = self._load_metadata()

            # Count total entries and size
            total_entries = 0
            total_size_bytes = 0

            for scraper_dir in self.cache_dir.iterdir():
                if scraper_dir.is_dir() and scraper_dir.name != "__pycache__":
                    for cache_file in scraper_dir.glob(f"*{self.serializer.get_file_extension()}"):
                        if cache_file.is_file():
                            total_entries += 1
                            total_size_bytes += cache_file.stat().st_size

            metadata.update(
                {
                    "total_entries": total_entries,
                    "total_size_bytes": total_size_bytes,
                    "last_updated": datetime.now().isoformat(),
                }
            )

            self._save_metadata(metadata)

        except Exception as e:
            logger.error(f"Failed to update cache metadata: {e}")

    def _load_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from file."""
        try:
            metadata_file = self.cache_dir / "cache_metadata.json"
            if metadata_file.exists():
                with open(metadata_file, "r") as f:
                    data: Dict[str, Any] = json.load(f)
                    return data
        except Exception as e:
            logger.error(f"Failed to load cache metadata: {e}")

        return {
            "created_at": datetime.now().isoformat(),
            "total_entries": 0,
            "total_size_bytes": 0,
            "last_cleanup": datetime.now().isoformat(),
        }

    def _save_metadata(self, metadata: Dict[str, Any]) -> None:
        """Save cache metadata to file."""
        try:
            metadata_file = self.cache_dir / "cache_metadata.json"
            with open(metadata_file, "w") as f:
                json.dump(metadata, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save cache metadata: {e}")

    def cleanup_expired(self) -> int:
        """
        Clean up expired cache entries.

        Returns:
            Number of entries cleaned up
        """
        cleaned_count = 0

        try:
            for scraper_dir in self.cache_dir.iterdir():
                if scraper_dir.is_dir() and scraper_dir.name != "__pycache__":
                    for cache_file in scraper_dir.glob(f"*{self.serializer.get_file_extension()}"):
                        if cache_file.is_file() and self._is_file_expired(cache_file):
                            try:
                                cache_file.unlink()
                                cleaned_count += 1
                                logger.debug(f"Cleaned up expired cache: {cache_file}")
                            except Exception as e:
                                logger.warning(f"Failed to clean up expired cache {cache_file}: {e}")

            # Update metadata after cleanup
            self._update_cache_metadata()

            # Update last cleanup time
            metadata = self._load_metadata()
            metadata["last_cleanup"] = datetime.now().isoformat()
            self._save_metadata(metadata)

            logger.info(f"Cache cleanup completed: {cleaned_count} expired entries removed")

        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}")

        return cleaned_count

    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        metadata = self._load_metadata()

        return {
            "file_cache": {
                "hits": self._stats["hits"],
                "misses": self._stats["misses"],
                "writes": self._stats["writes"],
                "deletions": self._stats["deletions"],
                "errors": self._stats["errors"],
                "hit_rate": self._stats["hits"] / max(1, self._stats["hits"] + self._stats["misses"]),
            },
            "storage": {
                "total_entries": metadata.get("total_entries", 0),
                "total_size_mb": round(metadata.get("total_size_bytes", 0) / (1024 * 1024), 2),
                "max_size_mb": self.max_cache_size_mb,
                "compression_enabled": self.serializer.compression_enabled,
            },
            "performance": {
                "ttl_hours": self.ttl_hours,
                "last_cleanup": metadata.get("last_cleanup", "Never"),
                "cache_dir": str(self.cache_dir),
            },
        }

    def clear_all(self) -> bool:
        """Clear all cache entries."""
        try:
            # Remove all cache files
            for scraper_dir in self.cache_dir.iterdir():
                if scraper_dir.is_dir() and scraper_dir.name != "__pycache__":
                    for cache_file in scraper_dir.glob(f"*{self.serializer.get_file_extension()}"):
                        if cache_file.is_file():
                            cache_file.unlink()

            # Reset statistics
            self._stats = {key: 0 for key in self._stats}

            # Update metadata
            self._update_cache_metadata()

            logger.info("File cache cleared successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to clear file cache: {e}")
            return False
