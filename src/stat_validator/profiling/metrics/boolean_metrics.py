"""
Boolean metrics calculator for statistical profiling.
"""
from typing import Dict, Any
import duckdb


class BooleanMetricsCalculator:
    """Calculate metrics for boolean columns."""

    def __init__(self, cache_conn: duckdb.DuckDBPyConnection):
        """
        Initialize calculator.

        Args:
            cache_conn: DuckDB connection with cached table
        """
        self.cache_conn = cache_conn

    def calculate_metrics(
        self,
        table_name: str,
        column_name: str,
        total_rows: int
    ) -> Dict[str, Any]:
        """
        Calculate boolean-specific metrics.

        Args:
            table_name: Name of the cached table (usually 'profiled_table')
            column_name: Name of the column
            total_rows: Total number of rows in the table

        Returns:
            Dictionary with basic stats and boolean stats
        """
        metrics = {}

        # Basic statistics
        basic_stats = self._calculate_basic_stats(table_name, column_name, total_rows)
        metrics['basic_stats'] = basic_stats

        # Boolean-specific statistics
        if basic_stats['count'] > 0:
            boolean_stats = self._calculate_boolean_stats(table_name, column_name, total_rows)
            metrics['boolean_stats'] = boolean_stats
        else:
            metrics['boolean_stats'] = self._empty_boolean_stats()

        return metrics

    def _calculate_basic_stats(
        self,
        table_name: str,
        column_name: str,
        total_rows: int
    ) -> Dict[str, Any]:
        """Calculate basic statistics."""
        query = f"""
        SELECT
            COUNT(*) as count,
            COUNT({column_name}) as non_null_count,
            COUNT(*) - COUNT({column_name}) as null_count,
            COUNT(DISTINCT {column_name}) as distinct_count
        FROM {table_name}
        """

        result = self.cache_conn.execute(query).fetchone()

        count = result[0]
        non_null_count = result[1]
        null_count = result[2]
        distinct_count = result[3]

        null_rate = round(null_count / count * 100, 2) if count > 0 else 0.0
        uniqueness = round(distinct_count / count, 6) if count > 0 else 0.0

        return {
            'count': count,
            'null_count': null_count,
            'null_rate': null_rate,
            'distinct_count': distinct_count,
            'uniqueness': uniqueness,
            'is_unique': distinct_count == non_null_count and null_count == 0 and count > 1
        }

    def _calculate_boolean_stats(
        self,
        table_name: str,
        column_name: str,
        total_rows: int
    ) -> Dict[str, Any]:
        """Calculate boolean-specific statistics."""

        # Count True, False, and NULL values
        query = f"""
        SELECT
            SUM(CASE WHEN {column_name} = TRUE THEN 1 ELSE 0 END) as true_count,
            SUM(CASE WHEN {column_name} = FALSE THEN 1 ELSE 0 END) as false_count,
            SUM(CASE WHEN {column_name} IS NULL THEN 1 ELSE 0 END) as null_count
        FROM {table_name}
        """

        result = self.cache_conn.execute(query).fetchone()

        true_count = int(result[0]) if result[0] is not None else 0
        false_count = int(result[1]) if result[1] is not None else 0
        null_count = int(result[2]) if result[2] is not None else 0

        # Calculate percentages
        true_percentage = round(true_count / total_rows * 100, 2) if total_rows > 0 else 0.0
        false_percentage = round(false_count / total_rows * 100, 2) if total_rows > 0 else 0.0
        null_percentage = round(null_count / total_rows * 100, 2) if total_rows > 0 else 0.0

        # Calculate True/False ratio (excluding NULLs)
        non_null_total = true_count + false_count
        if non_null_total > 0:
            true_ratio = round(true_count / non_null_total, 4)
            false_ratio = round(false_count / non_null_total, 4)
        else:
            true_ratio = 0.0
            false_ratio = 0.0

        return {
            'true_count': true_count,
            'true_percentage': true_percentage,
            'false_count': false_count,
            'false_percentage': false_percentage,
            'null_count': null_count,
            'null_percentage': null_percentage,
            'true_ratio': true_ratio,
            'false_ratio': false_ratio
        }

    def _empty_boolean_stats(self) -> Dict[str, Any]:
        """Return empty boolean stats for columns with no data."""
        return {
            'true_count': 0,
            'true_percentage': 0.0,
            'false_count': 0,
            'false_percentage': 0.0,
            'null_count': 0,
            'null_percentage': 0.0,
            'true_ratio': 0.0,
            'false_ratio': 0.0
        }
