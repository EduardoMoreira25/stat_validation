"""
Column Type Classifier Module

Infers column data types from sample data to enable type-specific statistical profiling.
Supports NUMERICAL, CATEGORICAL, and TEMPORAL classifications with automatic fallback.
"""

import re
from typing import List, Optional, Any
from enum import Enum


class ColumnType(Enum):
    """Column data type classifications."""
    NUMERICAL = "NUMERICAL"
    CATEGORICAL = "CATEGORICAL"
    TEMPORAL = "TEMPORAL"


class ColumnClassifier:
    """Classifies columns based on sample data patterns."""

    # Common date patterns to detect
    DATE_PATTERNS = [
        # YYYYMMDD
        (re.compile(r'^\d{8}$'), 'YYYYMMDD'),
        # YYYY-MM-DD or YYYY/MM/DD
        (re.compile(r'^\d{4}[-/]\d{2}[-/]\d{2}$'), 'YYYY-MM-DD'),
        # DD-MM-YYYY or DD/MM/YYYY
        (re.compile(r'^\d{2}[-/]\d{2}[-/]\d{4}$'), 'DD-MM-YYYY'),
        # YYYY-MM-DD HH:MM:SS
        (re.compile(r'^\d{4}[-/]\d{2}[-/]\d{2}\s+\d{2}:\d{2}:\d{2}'), 'YYYY-MM-DD HH:MM:SS'),
        # DD-MM-YYYY HH:MM:SS
        (re.compile(r'^\d{2}[-/]\d{2}[-/]\d{4}\s+\d{2}:\d{2}:\d{2}'), 'DD-MM-YYYY HH:MM:SS'),
        # ISO format with T
        (re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}'), 'ISO8601'),
    ]

    @staticmethod
    def is_numeric(value: Any) -> bool:
        """
        Check if a value can be interpreted as numeric.

        Args:
            value: Value to check

        Returns:
            True if value is numeric
        """
        if value is None:
            return False

        # Already numeric type
        if isinstance(value, (int, float)):
            return True

        # Try to convert string to number
        try:
            float(str(value))
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def matches_date_pattern(value: Any) -> Optional[str]:
        """
        Check if value matches any date pattern.

        Args:
            value: Value to check

        Returns:
            Date format string if matched, None otherwise
        """
        if value is None:
            return None

        value_str = str(value).strip()

        for pattern, format_name in ColumnClassifier.DATE_PATTERNS:
            if pattern.match(value_str):
                return format_name

        return None

    @classmethod
    def classify_column(
        cls,
        column_name: str,
        sample_values: List[Any],
        min_non_null_ratio: float = 0.5
    ) -> tuple[ColumnType, Optional[str]]:
        """
        Classify a column based on sample values.

        Args:
            column_name: Name of the column
            sample_values: List of sample values from the column
            min_non_null_ratio: Minimum ratio of non-null values required for classification

        Returns:
            Tuple of (ColumnType, format_hint)
            format_hint is the date format for TEMPORAL columns, None otherwise
        """
        # Filter out null values
        non_null_values = [v for v in sample_values if v is not None and str(v).strip() != '']

        # If too many nulls, default to categorical
        if len(non_null_values) < len(sample_values) * min_non_null_ratio:
            return ColumnType.CATEGORICAL, None

        # If no non-null values, default to categorical
        if not non_null_values:
            return ColumnType.CATEGORICAL, None

        # Sample first N values for pattern matching (performance)
        sample_check = non_null_values[:min(1000, len(non_null_values))]

        # Try TEMPORAL classification first (most specific)
        date_formats = set()
        temporal_matches = 0

        for value in sample_check:
            date_format = cls.matches_date_pattern(value)
            if date_format:
                date_formats.add(date_format)
                temporal_matches += 1

        # If >80% match date patterns and consistent format, classify as TEMPORAL
        if temporal_matches / len(sample_check) > 0.8:
            # Use most common format
            if len(date_formats) == 1:
                return ColumnType.TEMPORAL, date_formats.pop()
            elif date_formats:
                # Multiple formats detected, use first one found
                return ColumnType.TEMPORAL, list(date_formats)[0]

        # Try NUMERICAL classification
        numeric_matches = sum(1 for v in sample_check if cls.is_numeric(v))

        # Require 100% numeric values to classify as NUMERICAL (strict)
        # This prevents errors when trying to CAST columns with mixed content
        if numeric_matches == len(sample_check):
            return ColumnType.NUMERICAL, None

        # Default to CATEGORICAL
        return ColumnType.CATEGORICAL, None

    @classmethod
    def classify_columns(
        cls,
        columns: List[str],
        sample_data: List[List[Any]]
    ) -> dict[str, tuple[ColumnType, Optional[str]]]:
        """
        Classify multiple columns from sample data.

        Args:
            columns: List of column names
            sample_data: List of rows, where each row is a list of values

        Returns:
            Dictionary mapping column name to (ColumnType, format_hint)
        """
        classifications = {}

        # Transpose data to get column-wise values
        if not sample_data:
            # No sample data, default all to categorical
            return {col: (ColumnType.CATEGORICAL, None) for col in columns}

        for col_idx, col_name in enumerate(columns):
            # Extract values for this column
            col_values = [row[col_idx] if col_idx < len(row) else None for row in sample_data]

            # Classify the column
            col_type, format_hint = cls.classify_column(col_name, col_values)
            classifications[col_name] = (col_type, format_hint)

        return classifications

    @classmethod
    def safe_classify(
        cls,
        column_name: str,
        sample_values: List[Any],
        fallback: ColumnType = ColumnType.CATEGORICAL
    ) -> tuple[ColumnType, Optional[str]]:
        """
        Safely classify a column with exception handling.

        Args:
            column_name: Name of the column
            sample_values: List of sample values
            fallback: Type to fall back to on error

        Returns:
            Tuple of (ColumnType, format_hint)
        """
        try:
            return cls.classify_column(column_name, sample_values)
        except Exception:
            # On any error, fall back to categorical
            return fallback, None
