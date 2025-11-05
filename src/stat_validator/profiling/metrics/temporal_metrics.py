"""Calculate statistical metrics for temporal columns."""

import duckdb
from datetime import datetime, timedelta
from typing import Dict, Any, List
from ...utils.logger import get_logger

logger = get_logger('temporal_metrics')


class TemporalMetricsCalculator:
    """Calculate comprehensive statistics for temporal columns (date, datetime, timestamp)."""

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
        Calculate all temporal metrics for a column.

        Args:
            table_name: Name of cached table in DuckDB
            column_name: Column to analyze
            total_rows: Total row count for percentage calculations

        Returns:
            Dictionary with all temporal statistics
        """
        logger.debug(f"Calculating temporal metrics for {column_name}")

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

        # Get min, max, and median dates
        range_query = f"""
        SELECT
            MIN("{column_name}") as min_date,
            MAX("{column_name}") as max_date
        FROM {table_name}
        WHERE "{column_name}" IS NOT NULL
        """

        range_result = self.conn.execute(range_query).fetchone()
        min_date = range_result[0] if range_result else None
        max_date = range_result[1] if range_result else None

        # Get median date
        median_query = f"""
        SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column_name}") as median_date
        FROM {table_name}
        WHERE "{column_name}" IS NOT NULL
        """

        median_result = self.conn.execute(median_query).fetchone()
        median_date = median_result[0] if median_result and median_result[0] is not None else None

        # Calculate date span in days
        span_days = None
        if min_date and max_date:
            try:
                # Convert to datetime if needed
                if isinstance(min_date, str):
                    min_dt = datetime.fromisoformat(min_date.replace('Z', '+00:00'))
                    max_dt = datetime.fromisoformat(max_date.replace('Z', '+00:00'))
                else:
                    min_dt = min_date
                    max_dt = max_date

                span_days = (max_dt - min_dt).days
            except Exception as e:
                logger.warning(f"Could not calculate date span: {e}")

        # Check for future dates (dates beyond today)
        future_dates_count = 0
        try:
            future_query = f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE "{column_name}" > CURRENT_DATE
            """

            future_result = self.conn.execute(future_query).fetchone()
            future_dates_count = future_result[0] if future_result else 0
        except Exception as e:
            logger.debug(f"Could not check for future dates: {e}")

        # Weekday distribution (if we have a reasonable number of distinct dates)
        weekday_distribution = None
        if distinct_count <= 10000 and non_null_count > 0:
            try:
                weekday_query = f"""
                SELECT
                    DAYNAME("{column_name}") as day_name,
                    COUNT(*) as count
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
                GROUP BY DAYNAME("{column_name}")
                ORDER BY
                    CASE DAYNAME("{column_name}")
                        WHEN 'Monday' THEN 1
                        WHEN 'Tuesday' THEN 2
                        WHEN 'Wednesday' THEN 3
                        WHEN 'Thursday' THEN 4
                        WHEN 'Friday' THEN 5
                        WHEN 'Saturday' THEN 6
                        WHEN 'Sunday' THEN 7
                    END
                """

                weekday_results = self.conn.execute(weekday_query).fetchall()
                weekday_distribution = {
                    day_name: int(count) for day_name, count in weekday_results
                }
            except Exception as e:
                logger.debug(f"Could not calculate weekday distribution: {e}")

        # Hour distribution (if timestamps have time component)
        hour_distribution = None
        try:
            # Check if column has time component
            hour_check_query = f"""
            SELECT COUNT(DISTINCT HOUR("{column_name}"))
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            LIMIT 1
            """

            hour_distinct = self.conn.execute(hour_check_query).fetchone()[0]

            if hour_distinct > 1:  # Has time component with variation
                hour_query = f"""
                SELECT
                    HOUR("{column_name}") as hour,
                    COUNT(*) as count
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
                GROUP BY HOUR("{column_name}")
                ORDER BY hour
                """

                hour_results = self.conn.execute(hour_query).fetchall()
                hour_distribution = {
                    int(hour): int(count) for hour, count in hour_results
                }
        except Exception as e:
            logger.debug(f"Could not calculate hour distribution: {e}")

        # Detect gaps in dates (periods with no data)
        gaps = []
        if distinct_count > 1 and distinct_count <= 5000:  # Only for reasonable date counts
            try:
                # Get all distinct dates ordered
                dates_query = f"""
                SELECT DISTINCT CAST("{column_name}" AS DATE) as date_val
                FROM {table_name}
                WHERE "{column_name}" IS NOT NULL
                ORDER BY date_val
                """

                dates = [row[0] for row in self.conn.execute(dates_query).fetchall()]

                # Find gaps > 1 day
                for i in range(len(dates) - 1):
                    date1 = dates[i]
                    date2 = dates[i + 1]

                    if isinstance(date1, str):
                        date1 = datetime.fromisoformat(date1).date()
                    if isinstance(date2, str):
                        date2 = datetime.fromisoformat(date2).date()

                    gap_days = (date2 - date1).days

                    if gap_days > 1:  # Gap detected
                        gaps.append({
                            'from': str(date1),
                            'to': str(date2),
                            'days': gap_days
                        })

                        # Limit to first 10 gaps
                        if len(gaps) >= 10:
                            break
            except Exception as e:
                logger.debug(f"Could not detect date gaps: {e}")

        # Compile metrics
        metrics = {
            'basic_stats': {
                'count': int(count),
                'null_count': int(null_count),
                'null_rate': round(null_count / count * 100, 2) if count > 0 else 0.0,
                'distinct_count': int(distinct_count),
                'uniqueness': round(uniqueness, 6)
            },
            'temporal_stats': {
                'min': str(min_date) if min_date is not None else None,
                'max': str(max_date) if max_date is not None else None,
                'median': str(median_date) if median_date is not None else None,
                'span_days': span_days,
                'future_dates_count': int(future_dates_count)
            }
        }

        if weekday_distribution:
            metrics['temporal_stats']['weekday_distribution'] = weekday_distribution

        if hour_distribution:
            metrics['temporal_stats']['hour_distribution'] = hour_distribution

        if gaps:
            metrics['temporal_stats']['gaps'] = gaps

        logger.debug(f"Calculated temporal metrics for {column_name} (span: {span_days} days)")
        return metrics
