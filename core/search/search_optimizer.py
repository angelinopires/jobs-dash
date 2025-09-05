"""
Search optimization framework for job search infrastructure.

This module provides optimization patterns for job searches, including
result processing optimization, memory management, and performance
improvements for parallel search operations.
"""

import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List

import pandas as pd


class BaseSearchOptimizer(ABC):
    """
    Abstract base class for search optimizations.

    This class defines common optimization patterns:
    - Result processing optimization
    - Parallel processing preparation
    - Memory management
    - Performance monitoring integration
    """

    def __init__(self, scraper_name: str) -> None:
        self.scraper_name = scraper_name
        self.optimization_stats = {"optimizations_applied": 0, "time_saved": 0.0, "memory_optimizations": 0}

    @abstractmethod
    def optimize_search_params(self, **params: Any) -> Dict[str, Any]:
        """Optimize search parameters for better performance."""
        pass

    @abstractmethod
    def optimize_result_processing(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """Optimize job result processing."""
        pass

    def optimize_memory_usage(self, jobs_list: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Optimize memory usage when combining large result sets.

        Args:
            jobs_list: List of DataFrames to combine

        Returns:
            Optimized combined DataFrame
        """
        if not jobs_list:
            return pd.DataFrame()

        start_time = time.time()

        # Filter out empty DataFrames to avoid pandas warnings
        non_empty_dfs = [df for df in jobs_list if not df.empty]

        if not non_empty_dfs:
            return pd.DataFrame()

        # Optimize concatenation for large datasets
        if len(non_empty_dfs) == 1:
            combined_df = non_empty_dfs[0].copy()
        else:
            # Use efficient concatenation
            combined_df = pd.concat(non_empty_dfs, ignore_index=True, copy=False)

        # Memory optimization: convert object columns to category where appropriate
        combined_df = self._optimize_dataframe_dtypes(combined_df)

        optimization_time = time.time() - start_time
        self.optimization_stats["time_saved"] += optimization_time
        self.optimization_stats["memory_optimizations"] += 1

        return combined_df

    def _optimize_dataframe_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame data types to reduce memory usage."""
        if df.empty:
            return df

        optimized_df = df.copy()

        # Only convert very repetitive columns to category to avoid dashboard issues
        # Avoid job_type and company as the dashboard modifies these
        safe_categorical_columns = ["site", "source_country", "source_scraper"]

        for col in safe_categorical_columns:
            if col in optimized_df.columns:
                # Only convert if there are many repeated values and enough rows
                unique_count = len(optimized_df[col].unique())
                total_count = len(optimized_df)
                unique_ratio = unique_count / total_count if total_count > 0 else 1

                # Be more conservative: only convert if < 30% unique AND more than 10 rows
                if unique_ratio < 0.3 and total_count > 10:
                    optimized_df[col] = optimized_df[col].astype("category")

        return optimized_df

    def optimize_duplicate_removal(self, df: pd.DataFrame, key_columns: List[str]) -> pd.DataFrame:
        """
        Optimized duplicate removal with performance tracking.

        Args:
            df: DataFrame to deduplicate
            key_columns: Columns to use for duplicate detection

        Returns:
            DataFrame with duplicates removed
        """
        if df.empty:
            return df

        start_time = time.time()
        initial_count = len(df)

        # Use efficient duplicate removal
        available_columns = [col for col in key_columns if col in df.columns]

        if available_columns:
            deduped_df = df.drop_duplicates(subset=available_columns, keep="first")
        else:
            # Fallback: remove exact duplicates
            deduped_df = df.drop_duplicates(keep="first")

        final_count = len(deduped_df)
        duplicates_removed = initial_count - final_count

        optimization_time = time.time() - start_time
        self.optimization_stats["optimizations_applied"] += 1

        if duplicates_removed > 0:
            print(f"🔧 Optimization: Removed {duplicates_removed} duplicates in {optimization_time:.2f}s")

        return deduped_df


class SearchOptimizer(BaseSearchOptimizer):
    """
    Concrete implementation of search optimizations.

    This class implements the optimization methods for job searching,
    focusing on performance improvements that work across all search operations.
    """

    def optimize_search_params(self, **params: Any) -> Dict[str, Any]:
        """
        Optimize search parameters for better API performance.

        Args:
            **params: Raw search parameters

        Returns:
            Optimized parameters
        """
        optimized = params.copy()

        # Optimize results_wanted based on search type
        if params.get("where") == "Global":
            # For global searches, request fewer results per country
            # to get faster responses and reduce rate limiting
            optimized["results_wanted"] = min(params.get("results_wanted", 1000), 500)
        else:
            # For single country, can request more results
            optimized["results_wanted"] = params.get("results_wanted", 1000)

        # Optimize search term for better API responses
        search_term = params.get("search_term", "")
        if search_term:
            # Remove excessive whitespace and special characters that might cause issues
            optimized["search_term"] = " ".join(search_term.split())

        self.optimization_stats["optimizations_applied"] += 1

        return optimized

    def optimize_result_processing(self, jobs_df: pd.DataFrame) -> pd.DataFrame:
        """
        Optimize job result processing for faster display.

        Args:
            jobs_df: Raw jobs DataFrame from scraper

        Returns:
            Optimized DataFrame ready for display
        """
        if jobs_df.empty:
            return jobs_df

        start_time = time.time()

        # Create optimized copy
        optimized_df = jobs_df.copy()

        # Optimize data types
        optimized_df = self._optimize_dataframe_dtypes(optimized_df)

        # Pre-process common display columns for faster rendering
        if "date_posted" in optimized_df.columns:
            # Convert date columns to datetime if they aren't already
            optimized_df["date_posted"] = pd.to_datetime(optimized_df["date_posted"], errors="coerce")

        # Sort by date_posted (newest first) for better UX
        if "date_posted" in optimized_df.columns:
            optimized_df = optimized_df.sort_values("date_posted", ascending=False, na_position="last")

        processing_time = time.time() - start_time
        self.optimization_stats["time_saved"] += processing_time
        self.optimization_stats["optimizations_applied"] += 1

        return optimized_df
