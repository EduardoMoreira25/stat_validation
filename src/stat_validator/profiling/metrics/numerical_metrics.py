"""Calculate statistical metrics for numerical columns."""

import duckdb
import numpy as np
from typing import Dict, Any, Optional
from ...utils.logger import get_logger

logger = get_logger('numerical_metrics')


class NumericalMetricsCalculator:
    """Calculate comprehensive statistics for numerical columns."""

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
        total_rows: int
    ) -> Dict[str, Any]:
        """
        Calculate all numerical metrics for a column.

        Args:
            table_name: Name of cached table in DuckDB
            column_name: Column to analyze
            total_rows: Total row count for percentage calculations

        Returns:
            Dictionary with all numerical statistics
        """
        logger.debug(f"Calculating numerical metrics for {column_name}")

        # Basic aggregations (single query for efficiency)
        basic_query = f"""
        SELECT
            COUNT(*) as count,
            COUNT("{column_name}") as non_null_count,
            COUNT(DISTINCT "{column_name}") as distinct_count,
            MIN("{column_name}") as min_val,
            MAX("{column_name}") as max_val,
            AVG("{column_name}") as mean,
            STDDEV("{column_name}") as std_dev,
            VARIANCE("{column_name}") as variance,
            SUM("{column_name}") as sum_val,
            SUM(CASE WHEN "{column_name}" = 0 THEN 1 ELSE 0 END) as zero_count,
            SUM(CASE WHEN "{column_name}" < 0 THEN 1 ELSE 0 END) as negative_count
        FROM {table_name}
        """

        result = self.conn.execute(basic_query).fetchone()

        count = result[0]
        non_null_count = result[1]
        null_count = count - non_null_count
        distinct_count = result[2]
        min_val = result[3]
        max_val = result[4]
        mean = result[5]
        std_dev = result[6]
        variance = result[7]
        sum_val = result[8]
        zero_count = result[9]
        negative_count = result[10]

        # Calculate percentiles/quartiles (separate query)
        percentile_query = f"""
        SELECT
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{column_name}") as q1,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "{column_name}") as median,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{column_name}") as q3
        FROM {table_name}
        WHERE "{column_name}" IS NOT NULL
        """

        percentiles = self.conn.execute(percentile_query).fetchone()
        q1 = percentiles[0] if percentiles[0] is not None else None
        median = percentiles[1] if percentiles[1] is not None else None
        q3 = percentiles[2] if percentiles[2] is not None else None

        # Calculate IQR and outliers
        iqr = None
        outliers_count = 0
        outliers_percentage = 0.0

        if q1 is not None and q3 is not None:
            iqr = q3 - q1

            # Outliers: values outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR]
            if iqr > 0:
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr

                outlier_query = f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
                  AND ("{column_name}" < {lower_bound} OR "{column_name}" > {upper_bound})
                """

                outliers_count = self.conn.execute(outlier_query).fetchone()[0]
                outliers_percentage = (outliers_count / non_null_count * 100) if non_null_count > 0 else 0.0

        # Calculate skewness and kurtosis (if enough data)
        skewness = None
        kurtosis = None

        if non_null_count >= 3:
            # Fetch data for scipy calculations
            data_query = f'SELECT "{column_name}" FROM {table_name} WHERE "{column_name}" IS NOT NULL'
            data = self.conn.execute(data_query).fetchdf()[column_name].values

            if len(data) >= 3:
                try:
                    from scipy import stats
                    skewness = float(stats.skew(data))
                    kurtosis = float(stats.kurtosis(data))
                except ImportError:
                    logger.warning("scipy not installed, skipping skewness/kurtosis calculation")
                except Exception as e:
                    logger.warning(f"Error calculating skewness/kurtosis: {e}")

        # Determine mode (most frequent value)
        mode = None
        mode_frequency = None

        if distinct_count <= 1000:  # Only for low-medium cardinality
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
                mode = mode_result[0]
                mode_frequency = mode_result[1]

        # Calculate uniqueness ratio
        uniqueness = (distinct_count / non_null_count) if non_null_count > 0 else 0.0
        is_unique = (uniqueness == 1.0 and null_count == 0)

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
            'numerical_stats': {
                'min': float(min_val) if min_val is not None else None,
                'max': float(max_val) if max_val is not None else None,
                'mean': round(float(mean), 4) if mean is not None else None,
                'median': round(float(median), 4) if median is not None else None,
                'mode': float(mode) if mode is not None else None,
                'mode_frequency': int(mode_frequency) if mode_frequency is not None else None,
                'std_dev': round(float(std_dev), 4) if std_dev is not None else None,
                'variance': round(float(variance), 4) if variance is not None else None,
                'q1': round(float(q1), 4) if q1 is not None else None,
                'q3': round(float(q3), 4) if q3 is not None else None,
                'iqr': round(float(iqr), 4) if iqr is not None else None,
                'skewness': round(float(skewness), 4) if skewness is not None else None,
                'kurtosis': round(float(kurtosis), 4) if kurtosis is not None else None,
                'sum': float(sum_val) if sum_val is not None else None,
                'zero_count': int(zero_count),
                'negative_count': int(negative_count),
                'outliers_count': int(outliers_count),
                'outliers_percentage': round(outliers_percentage, 2)
            }
        }

        logger.debug(f"Calculated {len(metrics['numerical_stats'])} numerical metrics for {column_name}")
        return metrics
