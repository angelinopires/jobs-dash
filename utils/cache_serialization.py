"""
Cache Serialization Utility

Provides efficient JSON compression and decompression for file-based caching.
Uses gzip compression for 70-90% space savings while maintaining fast access.
"""

import gzip
import json
import logging
import zlib
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class CacheSerializer:
    """
    Efficient cache serialization with compression support.

    Features:
    - JSON serialization with compression (gzip)
    - Automatic compression detection
    - Metadata preservation
    - Error handling and corruption detection
    """

    def __init__(self, compression_enabled: bool = True):
        """
        Initialize the cache serializer.

        Args:
            compression_enabled: Whether to use gzip compression
        """
        self.compression_enabled = compression_enabled

    def serialize(self, data: Any, metadata: Optional[Dict[str, Any]] = None) -> bytes:
        """
        Serialize data to compressed JSON bytes.

        Args:
            data: Data to serialize (must be JSON-serializable)
            metadata: Optional metadata to include

        Returns:
            Compressed JSON bytes
        """
        try:
            # Prepare serialization data
            serialization_data = {
                "data": data,
                "metadata": metadata or {},
                "serialized_at": datetime.now().isoformat(),
                "compression": "gzip" if self.compression_enabled else "none",
            }

            # Convert to JSON string
            json_string = json.dumps(serialization_data, default=str)

            if self.compression_enabled:
                # Compress with gzip
                compressed_data = gzip.compress(json_string.encode("utf-8"))
                logger.debug(f"Serialized and compressed data: {len(json_string)} -> {len(compressed_data)} bytes")
                return compressed_data
            else:
                # Return uncompressed
                return json_string.encode("utf-8")

        except Exception as e:
            logger.error(f"Serialization failed: {e}")
            raise ValueError(f"Failed to serialize data: {e}")

    def deserialize(self, serialized_data: bytes) -> Dict[str, Any]:
        """
        Deserialize compressed JSON bytes back to data.

        Args:
            serialized_data: Compressed or uncompressed JSON bytes

        Returns:
            Dictionary containing data and metadata
        """
        try:
            # Try to decompress first (gzip)
            try:
                decompressed = gzip.decompress(serialized_data)
                compression_used = "gzip"
            except (OSError, zlib.error):
                # Not compressed, treat as plain JSON
                decompressed = serialized_data
                compression_used = "none"

            # Parse JSON
            json_data: Dict[str, Any] = json.loads(decompressed.decode("utf-8"))

            logger.debug(
                f"Deserialized data: {len(serialized_data)} -> {len(decompressed)} bytes "
                f"(compression: {compression_used})"
            )

            return json_data

        except Exception as e:
            logger.error(f"Deserialization failed: {e}")
            raise ValueError(f"Failed to deserialize data: {e}")

    def get_file_extension(self) -> str:
        """Get the appropriate file extension for this serializer."""
        return ".json.gz" if self.compression_enabled else ".json"

    def estimate_size(self, data: Any) -> int:
        """
        Estimate the serialized size of data.

        Args:
            data: Data to estimate size for

        Returns:
            Estimated size in bytes
        """
        try:
            json_string = json.dumps(data, default=str)
            estimated_size = len(json_string.encode("utf-8"))

            if self.compression_enabled:
                # Estimate compression ratio (typically 70-90% reduction)
                estimated_size = int(estimated_size * 0.3)  # 70% reduction

            return estimated_size

        except Exception:
            return 0


def create_cache_serializer(compression_enabled: bool = True) -> CacheSerializer:
    """
    Factory function to create a cache serializer.

    Args:
        compression_enabled: Whether to enable compression

    Returns:
        Configured CacheSerializer instance
    """
    return CacheSerializer(compression_enabled=compression_enabled)
