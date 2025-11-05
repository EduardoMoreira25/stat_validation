"""Calculate statistical metrics for categorical columns."""

import duckdb
import math
from typing import Dict, Any, List
from ...utils.logger import get_logger

logger = get_logger('categorical_metrics')


class CategoricalMetricsCalculator:
    """Calculate comprehensive statistics for categorical columns."""

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        """
        Initialize calculator with DuckDB connection.

        Args:
            conn: DuckDB connection with cached data
        """
        self.conn = conn

    def calculate_metrics(
        self,
        table_name: str,
        column_name: str,
        total_rows: int,
        max_cardinality: int = 10000
    ) -> Dict[str, Any]:
        """
        Calculate all categorical metrics for a column.

        Args:
            table_name: Name of cached table in DuckDB
            column_name: Column to analyze
            total_rows: Total row count for percentage calculations
            max_cardinality: Skip detailed analysis if cardinality exceeds this

        Returns:
            Dictionary with all categorical statistics
        """
        logger.debug(f"Calculating categorical metrics for {column_name}")

        # Basic aggregations
        basic_query = f"""
        SELECT
            COUNT(*) as count,
            COUNT("{column_name}") as non_null_count,
            COUNT(DISTINCT "{column_name}") as distinct_count
        FROM {table_name}
        """

        result = self.conn.execute(basic_query).fetchone()
        count = result[0]
        non_null_count = result[1]
        null_count = count - non_null_count
        distinct_count = result[2]

        # Calculate uniqueness
        uniqueness = (distinct_count / non_null_count) if non_null_count > 0 else 0.0
        is_unique = (uniqueness == 1.0 and null_count == 0)

        # Get mode (most frequent value)
        mode = None
        mode_frequency = None
        mode_percentage = None

        if distinct_count > 0:
            mode_query = f"""
            SELECT "{column_name}", COUNT(*) as freq
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            GROUP BY "{column_name}"
            ORDER BY freq DESC
            LIMIT 1
            """

            mode_result = self.conn.execute(mode_query).fetchone()
            if mode_result:
                mode = str(mode_result[0]) if mode_result[0] is not None else None
                mode_frequency = mode_result[1]
                mode_percentage = round(mode_frequency / non_null_count * 100, 2) if non_null_count > 0 else 0.0

        # Get top values (if cardinality is reasonable)
        top_values = []
        entropy = None

        if distinct_count <= max_cardinality:
            # Get frequency distribution
            freq_query = f"""
            SELECT "{column_name}", COUNT(*) as freq
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            GROUP BY "{column_name}"
            ORDER BY freq DESC
            LIMIT 100
            """

            freq_results = self.conn.execute(freq_query).fetchall()

            # Build top values list (top 10)
            for value, freq in freq_results[:10]:
                value_str = str(value) if value is not None else None
                percentage = round(freq / non_null_count * 100, 2) if non_null_count > 0 else 0.0

                top_values.append({
                    'value': value_str,
                    'count': int(freq),
                    'percentage': percentage
                })

            # Calculate entropy (measure of randomness/uniformity)
            # H = -Σ(p_i * log2(p_i))
            if non_null_count > 0:
                entropy_sum = 0.0
                for value, freq in freq_results:
                    p = freq / non_null_count
                    if p > 0:
                        entropy_sum += p * math.log2(p)

                entropy = -entropy_sum
                entropy = round(entropy, 4)

        # Include null in top values if nulls exist
        if null_count > 0:
            null_percentage = round(null_count / count * 100, 2)
            top_values.append({
                'value': None,
                'count': int(null_count),
                'percentage': null_percentage
            })

        # Identify rare values (appear only once or very few times)
        rare_threshold = max(1, int(non_null_count * 0.001))  # 0.1% threshold
        rare_values_count = 0

        if distinct_count <= max_cardinality:
            rare_query = f"""
            SELECT COUNT(*) as rare_count
            FROM (
                SELECT "{column_name}", COUNT(*) as freq
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
                GROUP BY "{column_name}"
                HAVING COUNT(*) <= {rare_threshold}
            )
            """

            rare_result = self.conn.execute(rare_query).fetchone()
            rare_values_count = rare_result[0] if rare_result else 0

        # String-specific metrics (if values are strings)
        string_stats = None

        try:
            # Try to calculate string length metrics
            length_query = f"""
            SELECT
                MIN(LENGTH(CAST("{column_name}" AS VARCHAR))) as min_length,
                MAX(LENGTH(CAST("{column_name}" AS VARCHAR))) as max_length,
                AVG(LENGTH(CAST("{column_name}" AS VARCHAR))) as avg_length,
                SUM(CASE WHEN CAST("{column_name}" AS VARCHAR) = '' THEN 1 ELSE 0 END) as empty_count,
                SUM(CASE WHEN TRIM(CAST("{column_name}" AS VARCHAR)) = '' THEN 1 ELSE 0 END) as whitespace_only_count
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            """

            length_result = self.conn.execute(length_query).fetchone()

            if length_result:
                string_stats = {
                    'min_length': int(length_result[0]) if length_result[0] is not None else None,
                    'max_length': int(length_result[1]) if length_result[1] is not None else None,
                    'avg_length': round(float(length_result[2]), 2) if length_result[2] is not None else None,
                    'empty_string_count': int(length_result[3]),
                    'whitespace_only_count': int(length_result[4])
                }
        except Exception as e:
            logger.debug(f"Could not calculate string metrics for {column_name}: {e}")

        # Compile metrics
        metrics = {
            'basic_stats': {
                'count': int(count),
                'null_count': int(null_count),
                'null_rate': round(null_count / count * 100, 2) if count > 0 else 0.0,
                'distinct_count': int(distinct_count),
                'uniqueness': round(uniqueness, 6),
                'is_unique': is_unique
            },
            'categorical_stats': {
                'mode': mode,
                'mode_frequency': int(mode_frequency) if mode_frequency is not None else None,
                'mode_percentage': mode_percentage,
                'entropy': entropy,
                'top_values': top_values,
                'rare_values_count': int(rare_values_count),
                'rare_values_threshold': rare_threshold
            }
        }

        if string_stats:
            metrics['string_stats'] = string_stats

        logger.debug(f"Calculated categorical metrics for {column_name} (cardinality: {distinct_count})")
        return metrics
