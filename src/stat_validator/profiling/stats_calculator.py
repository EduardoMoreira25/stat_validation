"""
Advanced Statistics Calculator Module

Calculates detailed column-level statistics with support for different data types.
Uses SQL pushdown for efficient computation on large datasets.
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pyarrow as pa

from .column_classifier import ColumnType


logger = logging.getLogger(__name__)


@dataclass
class NumericalStats:
    """Statistics for numerical columns."""
    null_count: int
    unique_count: int
    unique_percentage: float
    mean: Optional[float]
    median: Optional[float]
    min: Optional[float]
    max: Optional[float]
    mode1: Optional[float]
    mode2: Optional[float]


@dataclass
class CategoricalStats:
    """Statistics for categorical columns."""
    null_count: int
    unique_count: int
    unique_percentage: float
    min_length: Optional[int]
    max_length: Optional[int]
    avg_length: Optional[float]
    mode1: Optional[str]
    mode2: Optional[str]


@dataclass
class TemporalStats:
    """Statistics for temporal columns."""
    null_count: int
    unique_count: int
    unique_percentage: float
    format: Optional[str]
    min_date: Optional[str]
    max_date: Optional[str]
    most_common_date: Optional[str]


class StatsCalculator:
    """Calculates advanced statistics using SQL pushdown."""

    @staticmethod
    def compute_mode_from_sample(sample_data: List[Any], limit: int = 2) -> List[Any]:
        """
        Compute mode (most common values) from in-memory sample.

        Args:
            sample_data: List of values from sample
            limit: Number of mode values to return (default 2)

        Returns:
            List of most common values
        """
        from collections import Counter

        # Filter out None values
        non_null_data = [v for v in sample_data if v is not None]

        if not non_null_data:
            return [None] * limit

        # Count frequencies and get top N
        counter = Counter(non_null_data)
        most_common = counter.most_common(limit)

        # Extract just the values
        modes = [value for value, count in most_common]

        # Pad with None if we don't have enough
        while len(modes) < limit:
            modes.append(None)

        return modes[:limit]

    @staticmethod
    def compute_numerical_stats_from_sample(
        sample_data: List[Any],
        total_row_count: int
    ) -> NumericalStats:
        """
        Compute numerical statistics from in-memory sample.

        Args:
            sample_data: List of values from sample
            total_row_count: Total row count in filtered dataset

        Returns:
            NumericalStats object
        """
        import statistics

        # Count nulls
        null_count = sum(1 for v in sample_data if v is None)
        non_null_data = [v for v in sample_data if v is not None]

        if not non_null_data:
            return NumericalStats(
                null_count=null_count,
                unique_count=0,
                unique_percentage=0.0,
                mean=None,
                median=None,
                min=None,
                max=None,
                mode1=None,
                mode2=None
            )

        # Convert to float
        try:
            numeric_data = [float(v) for v in non_null_data]
        except (ValueError, TypeError):
            # If conversion fails, treat as categorical
            return None

        # Compute stats
        unique_count = len(set(numeric_data))
        unique_pct = (unique_count / len(sample_data) * 100) if len(sample_data) > 0 else 0.0

        mean = statistics.mean(numeric_data)
        median = statistics.median(numeric_data)
        min_val = min(numeric_data)
        max_val = max(numeric_data)

        # Get mode from sample
        modes = StatsCalculator.compute_mode_from_sample(numeric_data, limit=2)
        mode1 = modes[0] if modes else None
        mode2 = modes[1] if len(modes) > 1 else None

        return NumericalStats(
            null_count=null_count,
            unique_count=unique_count,
            unique_percentage=round(unique_pct, 2),
            mean=round(mean, 4) if mean is not None else None,
            median=round(median, 4) if median is not None else None,
            min=min_val,
            max=max_val,
            mode1=mode1,
            mode2=mode2
        )

    @staticmethod
    def compute_categorical_stats_from_sample(
        sample_data: List[Any],
        total_row_count: int
    ) -> CategoricalStats:
        """
        Compute categorical statistics from in-memory sample.

        Args:
            sample_data: List of values from sample
            total_row_count: Total row count in filtered dataset

        Returns:
            CategoricalStats object
        """
        # Count nulls
        null_count = sum(1 for v in sample_data if v is None)
        non_null_data = [v for v in sample_data if v is not None]

        if not non_null_data:
            return CategoricalStats(
                null_count=null_count,
                unique_count=0,
                unique_percentage=0.0,
                min_length=None,
                max_length=None,
                avg_length=None,
                mode1=None,
                mode2=None
            )

        # Convert to strings
        str_data = [str(v) for v in non_null_data]

        # Compute stats
        unique_count = len(set(str_data))
        unique_pct = (unique_count / len(sample_data) * 100) if len(sample_data) > 0 else 0.0

        lengths = [len(s) for s in str_data]
        min_length = min(lengths) if lengths else None
        max_length = max(lengths) if lengths else None
        avg_length = sum(lengths) / len(lengths) if lengths else None

        # Get mode from sample
        modes = StatsCalculator.compute_mode_from_sample(str_data, limit=2)
        mode1 = modes[0] if modes else None
        mode2 = modes[1] if len(modes) > 1 else None

        return CategoricalStats(
            null_count=null_count,
            unique_count=unique_count,
            unique_percentage=round(unique_pct, 2),
            min_length=min_length,
            max_length=max_length,
            avg_length=round(avg_length, 2) if avg_length is not None else None,
            mode1=mode1,
            mode2=mode2
        )

    @staticmethod
    def compute_temporal_stats_from_sample(
        sample_data: List[Any],
        total_row_count: int,
        date_format: Optional[str]
    ) -> TemporalStats:
        """
        Compute temporal statistics from in-memory sample.

        Args:
            sample_data: List of values from sample
            total_row_count: Total row count in filtered dataset
            date_format: Detected date format

        Returns:
            TemporalStats object
        """
        # Count nulls
        null_count = sum(1 for v in sample_data if v is None)
        non_null_data = [v for v in sample_data if v is not None]

        if not non_null_data:
            return TemporalStats(
                null_count=null_count,
                unique_count=0,
                unique_percentage=0.0,
                format=date_format,
                min_date=None,
                max_date=None,
                most_common_date=None
            )

        # Convert to strings
        str_data = [str(v) for v in non_null_data]

        # Compute stats
        unique_count = len(set(str_data))
        unique_pct = (unique_count / len(sample_data) * 100) if len(sample_data) > 0 else 0.0

        min_date = min(str_data)
        max_date = max(str_data)

        # Get most common date from sample
        modes = StatsCalculator.compute_mode_from_sample(str_data, limit=1)
        most_common_date = modes[0] if modes else None

        return TemporalStats(
            null_count=null_count,
            unique_count=unique_count,
            unique_percentage=round(unique_pct, 2),
            format=date_format,
            min_date=min_date,
            max_date=max_date,
            most_common_date=most_common_date
        )

    @staticmethod
    def determine_sample_size(row_count: int) -> int:
        """
        Determine adaptive sample size based on row count.

        Args:
            row_count: Total number of rows in filtered dataset

        Returns:
            Appropriate sample size
        """
        if row_count < 10000:
            return row_count  # 100% sample
        elif row_count < 100000:
            return 10000
        else:
            return 20000

    @staticmethod
    def build_mode_query(
        table_ref: str,
        column_name: str,
        col_prefix: str,
        where_clause: str,
        engine: str = 'dremio'
    ) -> str:
        """
        Build query to get MODE (top 2 most common values).

        Args:
            table_ref: Full table reference (schema.table)
            column_name: Column to analyze
            col_prefix: Column prefix (e.g., 'a.' for aliases)
            where_clause: WHERE clause for filtering
            engine: 'dremio' or 'hana'

        Returns:
            SQL query string
        """
        safe_col = f'{col_prefix}"{column_name}"'

        query = f"""
        SELECT {safe_col} as "value", COUNT(*) as "frequency"
        FROM {table_ref}
        WHERE {where_clause}
          AND {safe_col} IS NOT NULL
        GROUP BY {safe_col}
        ORDER BY "frequency" DESC
        LIMIT 2
        """

        return query

    @staticmethod
    def build_numerical_stats_query(
        table_ref: str,
        column_name: str,
        col_prefix: str,
        where_clause: str,
        row_count: int,
        engine: str = 'dremio'
    ) -> str:
        """
        Build query for numerical column statistics.

        Args:
            table_ref: Full table reference
            column_name: Column to analyze
            col_prefix: Column prefix for table alias
            where_clause: WHERE clause
            row_count: Total row count for sample
            engine: 'dremio' or 'hana'

        Returns:
            SQL query string
        """
        safe_col = f'{col_prefix}"{column_name}"'

        # Use CAST to ensure numeric type
        cast_col = f'CAST({safe_col} AS DOUBLE)'

        # Median calculation differs by engine
        if engine == 'dremio':
            median_expr = f'PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {cast_col}) as "median"'
        else:  # hana
            median_expr = f'MEDIAN({cast_col}) as "median"'

        query = f"""
        SELECT
            SUM(CASE WHEN {safe_col} IS NULL THEN 1 ELSE 0 END) as "null_count",
            COUNT(DISTINCT {safe_col}) as "unique_count",
            AVG({cast_col}) as "mean",
            {median_expr},
            MIN({cast_col}) as "min",
            MAX({cast_col}) as "max"
        FROM {table_ref}
        WHERE {where_clause}
        """

        return query

    @staticmethod
    def build_categorical_stats_query(
        table_ref: str,
        column_name: str,
        col_prefix: str,
        where_clause: str,
        row_count: int,
        engine: str = 'dremio'
    ) -> str:
        """
        Build query for categorical column statistics.

        Args:
            table_ref: Full table reference
            column_name: Column to analyze
            col_prefix: Column prefix for table alias
            where_clause: WHERE clause
            row_count: Total row count for sample
            engine: 'dremio' or 'hana'

        Returns:
            SQL query string
        """
        safe_col = f'{col_prefix}"{column_name}"'

        query = f"""
        SELECT
            SUM(CASE WHEN {safe_col} IS NULL THEN 1 ELSE 0 END) as "null_count",
            COUNT(DISTINCT {safe_col}) as "unique_count",
            MIN(LENGTH({safe_col})) as "min_length",
            MAX(LENGTH({safe_col})) as "max_length",
            AVG(LENGTH(CAST({safe_col} AS VARCHAR))) as "avg_length"
        FROM {table_ref}
        WHERE {where_clause}
        """

        return query

    @staticmethod
    def build_temporal_stats_query(
        table_ref: str,
        column_name: str,
        col_prefix: str,
        where_clause: str,
        row_count: int,
        engine: str = 'dremio'
    ) -> str:
        """
        Build query for temporal column statistics.

        Args:
            table_ref: Full table reference
            column_name: Column to analyze
            col_prefix: Column prefix for table alias
            where_clause: WHERE clause
            row_count: Total row count for sample
            engine: 'dremio' or 'hana'

        Returns:
            SQL query string
        """
        safe_col = f'{col_prefix}"{column_name}"'

        query = f"""
        SELECT
            SUM(CASE WHEN {safe_col} IS NULL THEN 1 ELSE 0 END) as "null_count",
            COUNT(DISTINCT {safe_col}) as "unique_count",
            MIN({safe_col}) as "min_date",
            MAX({safe_col}) as "max_date"
        FROM {table_ref}
        WHERE {where_clause}
        """

        return query

    @staticmethod
    def parse_numerical_stats(
        result: pa.Table,
        row_count: int,
        mode_result: Optional[pa.Table] = None
    ) -> NumericalStats:
        """
        Parse numerical statistics from query result.

        Args:
            result: PyArrow table with statistics
            row_count: Total row count for percentage calculation
            mode_result: Optional PyArrow table with mode values

        Returns:
            NumericalStats object
        """
        if len(result) == 0:
            return NumericalStats(
                null_count=0,
                unique_count=0,
                unique_percentage=0.0,
                mean=None,
                median=None,
                min=None,
                max=None,
                mode1=None,
                mode2=None
            )

        row = result.to_pydict()

        # Handle case-insensitive column names
        col_mapping = {k.lower(): k for k in row.keys()}

        null_count = int(row[col_mapping.get('null_count', 'null_count')][0] or 0)
        unique_count = int(row[col_mapping.get('unique_count', 'unique_count')][0] or 0)
        unique_pct = (unique_count / row_count * 100) if row_count > 0 else 0.0

        mean = float(row[col_mapping.get('mean', 'mean')][0]) if row[col_mapping.get('mean', 'mean')][0] is not None else None
        median = float(row[col_mapping.get('median', 'median')][0]) if row[col_mapping.get('median', 'median')][0] is not None else None
        min_val = float(row[col_mapping.get('min', 'min')][0]) if row[col_mapping.get('min', 'min')][0] is not None else None
        max_val = float(row[col_mapping.get('max', 'max')][0]) if row[col_mapping.get('max', 'max')][0] is not None else None

        # Parse mode from separate query
        mode1, mode2 = None, None
        if mode_result and len(mode_result) > 0:
            mode_data = mode_result.to_pydict()
            mode_col_mapping = {k.lower(): k for k in mode_data.keys()}
            value_key = mode_col_mapping.get('value', 'value')

            if value_key in mode_data:
                values = mode_data[value_key]
                if len(values) >= 1 and values[0] is not None:
                    try:
                        mode1 = float(values[0])
                    except (ValueError, TypeError):
                        pass
                if len(values) >= 2 and values[1] is not None:
                    try:
                        mode2 = float(values[1])
                    except (ValueError, TypeError):
                        pass

        return NumericalStats(
            null_count=null_count,
            unique_count=unique_count,
            unique_percentage=round(unique_pct, 2),
            mean=round(mean, 2) if mean is not None else None,
            median=round(median, 2) if median is not None else None,
            min=min_val,
            max=max_val,
            mode1=mode1,
            mode2=mode2
        )

    @staticmethod
    def parse_categorical_stats(
        result: pa.Table,
        row_count: int,
        mode_result: Optional[pa.Table] = None
    ) -> CategoricalStats:
        """
        Parse categorical statistics from query result.

        Args:
            result: PyArrow table with statistics
            row_count: Total row count for percentage calculation
            mode_result: Optional PyArrow table with mode values

        Returns:
            CategoricalStats object
        """
        if len(result) == 0:
            return CategoricalStats(
                null_count=0,
                unique_count=0,
                unique_percentage=0.0,
                min_length=None,
                max_length=None,
                avg_length=None,
                mode1=None,
                mode2=None
            )

        row = result.to_pydict()
        col_mapping = {k.lower(): k for k in row.keys()}

        null_count = int(row[col_mapping.get('null_count', 'null_count')][0] or 0)
        unique_count = int(row[col_mapping.get('unique_count', 'unique_count')][0] or 0)
        unique_pct = (unique_count / row_count * 100) if row_count > 0 else 0.0

        min_length = int(row[col_mapping.get('min_length', 'min_length')][0]) if row[col_mapping.get('min_length', 'min_length')][0] is not None else None
        max_length = int(row[col_mapping.get('max_length', 'max_length')][0]) if row[col_mapping.get('max_length', 'max_length')][0] is not None else None
        avg_length = float(row[col_mapping.get('avg_length', 'avg_length')][0]) if row[col_mapping.get('avg_length', 'avg_length')][0] is not None else None

        # Parse mode from separate query
        mode1, mode2 = None, None
        if mode_result and len(mode_result) > 0:
            mode_data = mode_result.to_pydict()
            mode_col_mapping = {k.lower(): k for k in mode_data.keys()}
            value_key = mode_col_mapping.get('value', 'value')

            if value_key in mode_data:
                values = mode_data[value_key]
                if len(values) >= 1 and values[0] is not None:
                    mode1 = str(values[0])
                if len(values) >= 2 and values[1] is not None:
                    mode2 = str(values[1])

        return CategoricalStats(
            null_count=null_count,
            unique_count=unique_count,
            unique_percentage=round(unique_pct, 2),
            min_length=min_length,
            max_length=max_length,
            avg_length=round(avg_length, 2) if avg_length is not None else None,
            mode1=mode1,
            mode2=mode2
        )

    @staticmethod
    def parse_temporal_stats(
        result: pa.Table,
        row_count: int,
        date_format: Optional[str],
        mode_result: Optional[pa.Table] = None
    ) -> TemporalStats:
        """
        Parse temporal statistics from query result.

        Args:
            result: PyArrow table with statistics
            row_count: Total row count for percentage calculation
            date_format: Detected date format
            mode_result: Optional PyArrow table with mode values

        Returns:
            TemporalStats object
        """
        if len(result) == 0:
            return TemporalStats(
                null_count=0,
                unique_count=0,
                unique_percentage=0.0,
                format=date_format,
                min_date=None,
                max_date=None,
                most_common_date=None
            )

        row = result.to_pydict()
        col_mapping = {k.lower(): k for k in row.keys()}

        null_count = int(row[col_mapping.get('null_count', 'null_count')][0] or 0)
        unique_count = int(row[col_mapping.get('unique_count', 'unique_count')][0] or 0)
        unique_pct = (unique_count / row_count * 100) if row_count > 0 else 0.0

        min_date = str(row[col_mapping.get('min_date', 'min_date')][0]) if row[col_mapping.get('min_date', 'min_date')][0] is not None else None
        max_date = str(row[col_mapping.get('max_date', 'max_date')][0]) if row[col_mapping.get('max_date', 'max_date')][0] is not None else None

        # Parse most common date from mode query
        most_common_date = None
        if mode_result and len(mode_result) > 0:
            mode_data = mode_result.to_pydict()
            mode_col_mapping = {k.lower(): k for k in mode_data.keys()}
            value_key = mode_col_mapping.get('value', 'value')

            if value_key in mode_data:
                values = mode_data[value_key]
                if len(values) >= 1 and values[0] is not None:
                    most_common_date = str(values[0])

        return TemporalStats(
            null_count=null_count,
            unique_count=unique_count,
            unique_percentage=round(unique_pct, 2),
            format=date_format,
            min_date=min_date,
            max_date=max_date,
            most_common_date=most_common_date
        )
