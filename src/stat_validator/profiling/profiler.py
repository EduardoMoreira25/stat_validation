"""Main table profiling engine."""

import duckdb
from datetime import datetime
from typing import Dict, Any, Optional, List
import pyarrow as pa

from ..connectors.base_connector import BaseConnector
from ..comparison.schema_validator import SchemaValidator
from .metrics.numerical_metrics import NumericalMetricsCalculator
from .metrics.categorical_metrics import CategoricalMetricsCalculator
from .metrics.temporal_metrics import TemporalMetricsCalculator
from .metrics.boolean_metrics import BooleanMetricsCalculator
from ..utils.logger import get_logger

logger = get_logger('profiler')


class TableProfiler:
    """Profile tables and generate comprehensive statistical summaries."""

    def __init__(
        self,
        connector: BaseConnector,
        sample_size: int = 50000,
        max_categorical_cardinality: int = 10000
    ):
        """
        Initialize table profiler.

        Args:
            connector: Database connector (HANA, Dremio, etc.)
            sample_size: Number of rows to sample for profiling
            max_categorical_cardinality: Skip detailed categorical analysis above this threshold
        """
        self.connector = connector
        self.sample_size = sample_size
        self.max_categorical_cardinality = max_categorical_cardinality
        self.schema_validator = SchemaValidator()

        # Initialize metric calculators (will be set after caching)
        self.numerical_calc = None
        self.categorical_calc = None
        self.temporal_calc = None

    def profile_table(self, table_name: str) -> Dict[str, Any]:
        """
        Generate comprehensive profile for a table.

        Args:
            table_name: Fully qualified table name

        Returns:
            Dictionary with complete profile data
        """
        logger.info(f"Starting profile generation for table: {table_name}")
        print(f"\n{'='*60}")
        print(f"Profiling Table: {table_name}")
        print(f"{'='*60}\n")

        profile_start = datetime.now()

        # Step 1: Get table schema
        print("[1/5] Fetching table schema...")
        schema = self.connector.get_table_schema(table_name)
        column_count = len(schema)
        logger.info(f"Table has {column_count} columns")

        # Step 2: Get row count
        print("[2/5] Counting rows...")
        try:
            row_count = self.connector.get_row_count(table_name)
            logger.info(f"Table has {row_count:,} rows")
        except Exception as e:
            logger.warning(f"Could not get exact row count: {e}")
            row_count = None

        # Step 3: Sample and cache data
        print(f"[3/5] Sampling data ({self.sample_size:,} rows)...")
        cached_columns = self._cache_table_sample(table_name, schema)

        # Get actual sample size from cache
        cache_conn = self.connector.get_cache_connection()
        actual_sample_size = cache_conn.execute("SELECT COUNT(*) FROM profiled_table").fetchone()[0]
        logger.info(f"Cached {actual_sample_size:,} rows for analysis")

        # Initialize metric calculators
        self.numerical_calc = NumericalMetricsCalculator(cache_conn)
        self.categorical_calc = CategoricalMetricsCalculator(cache_conn)
        self.temporal_calc = TemporalMetricsCalculator(cache_conn)
        self.boolean_calc = BooleanMetricsCalculator(cache_conn)

        # Step 4: Calculate metrics for each column
        print(f"[4/5] Calculating metrics for {len(cached_columns)} columns...")
        column_profiles = []

        for idx, column_name in enumerate(cached_columns, 1):
            print(f"  [{idx}/{len(cached_columns)}] Analyzing {column_name}...", end='\r')

            # Get column from schema
            field = next((f for f in schema if f.name == column_name), None)
            if not field:
                logger.warning(f"Column {column_name} not found in schema, skipping")
                continue

            # Classify column type
            column_type = str(field.type)
            classification = self._classify_column(field)

            # Calculate metrics based on type
            try:
                if classification == 'NUMERICAL':
                    metrics = self.numerical_calc.calculate_metrics(
                        'profiled_table',
                        column_name,
                        actual_sample_size
                    )
                elif classification == 'TEMPORAL':
                    metrics = self.temporal_calc.calculate_metrics(
                        'profiled_table',
                        column_name,
                        actual_sample_size
                    )
                elif classification == 'BOOLEAN':
                    metrics = self.boolean_calc.calculate_metrics(
                        'profiled_table',
                        column_name,
                        actual_sample_size
                    )
                else:  # CATEGORICAL or OTHER
                    metrics = self.categorical_calc.calculate_metrics(
                        'profiled_table',
                        column_name,
                        actual_sample_size,
                        self.max_categorical_cardinality
                    )

                # Add column metadata
                column_profile = {
                    'name': column_name,
                    'type': column_type,
                    'classification': classification,
                    **metrics
                }

                column_profiles.append(column_profile)

            except Exception as e:
                logger.error(f"Error profiling column {column_name}: {e}")
                # Add basic error profile
                column_profiles.append({
                    'name': column_name,
                    'type': column_type,
                    'classification': classification,
                    'error': str(e)
                })

        print(f"  Completed analysis for {len(column_profiles)} columns" + " " * 20)

        # Step 5: Calculate table-level metrics
        print("[5/5] Calculating table-level metrics...")
        table_metrics = self._calculate_table_metrics(cache_conn, actual_sample_size, column_profiles)

        profile_end = datetime.now()
        duration = (profile_end - profile_start).total_seconds()

        # Compile final profile
        profile = {
            'metadata': {
                'table_name': table_name,
                'database': self.connector.__class__.__name__.replace('Connector', '').upper(),
                'profiled_at': profile_start.isoformat(),
                'duration_seconds': round(duration, 2),
                'row_count': row_count,
                'column_count': column_count,
                'sample_size': actual_sample_size,
                'profile_version': '1.0'
            },
            'table_metrics': table_metrics,
            'columns': column_profiles
        }

        print(f"\n✅ Profile generated successfully in {duration:.2f}s")
        logger.info(f"Profile generation completed for {table_name}")

        return profile

    def _cache_table_sample(self, table_name: str, schema: pa.Schema) -> List[str]:
        """
        Sample table data and cache to DuckDB.

        Args:
            table_name: Table to sample
            schema: Table schema

        Returns:
            List of cached column names
        """
        # Step 1: Filter out binary TYPE columns
        binary_type_cols = set([
            field.name.upper() for field in schema
            if self._is_binary_type(field)
        ])

        cacheable_columns = [
            field.name for field in schema
            if field.name.upper() not in binary_type_cols
        ]

        if binary_type_cols:
            logger.info(f"Excluding {len(binary_type_cols)} binary-type columns from profiling")

        # Step 2: Detect binary DATA in string columns (converted binary columns)
        # Sample a small subset to check for binary content
        logger.info("Detecting binary data in string columns...")
        binary_data_cols = self._detect_binary_data_columns(table_name, cacheable_columns)

        if binary_data_cols:
            logger.warning(f"Detected {len(binary_data_cols)} columns with binary data (likely converted from binary types)")
            logger.warning(f"Excluding columns with binary data: {binary_data_cols}")
            cacheable_columns = [col for col in cacheable_columns if col.upper() not in binary_data_cols]

        if not cacheable_columns:
            raise Exception("No cacheable columns found - all columns are binary or contain binary data")

        # Step 3: Try to cache all remaining columns
        column_list = ', '.join([f'"{col}"' for col in cacheable_columns])
        query = f"SELECT {column_list} FROM {table_name} LIMIT {self.sample_size}"

        try:
            self.connector.cache_query(query, "profiled_table")
            logger.info(f"Successfully cached {len(cacheable_columns)} columns")
            return cacheable_columns
        except Exception as e:
            logger.error(f"Failed to cache columns even after binary detection: {e}")
            logger.info("Falling back to column-by-column caching...")

            # Step 4: Column-by-column fallback
            return self._cache_columns_individually(table_name, cacheable_columns)

    def _detect_binary_data_columns(self, table_name: str, columns: List[str], sample_size: int = 10) -> set:
        """
        Detect columns that contain binary data (even if typed as VARCHAR/STRING).

        This handles cases where binary HANA columns are converted to VARCHAR in Dremio
        but still contain unprintable binary characters.

        Args:
            table_name: Table to sample
            columns: List of column names to check
            sample_size: Number of rows to sample for detection

        Returns:
            Set of column names (uppercase) that contain binary data
        """
        binary_data_cols = set()

        try:
            # Build query to check first few non-null values of each string column
            column_checks = []
            for col in columns:
                # Cast to VARCHAR and check if it contains non-printable characters
                # by comparing length with length after removing control characters
                column_checks.append(f'"{col}"')

            if not column_checks:
                return binary_data_cols

            # Get a small sample
            sample_query = f"SELECT {', '.join(column_checks)} FROM {table_name} LIMIT {sample_size}"
            result = self.connector.execute_query(sample_query)
            df = result.to_pandas()

            # Check each column for binary data
            for col in df.columns:
                try:
                    # Check if column contains non-UTF8 or unprintable characters
                    sample_values = df[col].dropna().head(sample_size)

                    if len(sample_values) == 0:
                        continue  # Skip columns with all nulls

                    for val in sample_values:
                        if val is None:
                            continue

                        # Convert to string if not already
                        val_str = str(val)

                        # Check for binary indicators:
                        # 1. Non-printable characters (control chars except whitespace)
                        # 2. Null bytes
                        # 3. High percentage of non-ASCII characters
                        has_null_bytes = '\x00' in val_str
                        non_printable_count = sum(1 for c in val_str if ord(c) < 32 and c not in '\t\n\r')
                        non_ascii_count = sum(1 for c in val_str if ord(c) > 127)

                        # If more than 20% non-printable or has null bytes, it's likely binary
                        if has_null_bytes or (len(val_str) > 0 and non_printable_count / len(val_str) > 0.2):
                            binary_data_cols.add(col.upper())
                            logger.debug(f"Column '{col}' contains binary data (null_bytes={has_null_bytes}, non_printable={non_printable_count}/{len(val_str)})")
                            break  # Found binary, no need to check more values

                        # Also check for very high non-ASCII ratio (>80%)
                        if len(val_str) > 10 and non_ascii_count / len(val_str) > 0.8:
                            binary_data_cols.add(col.upper())
                            logger.debug(f"Column '{col}' appears to be encoded binary (non_ascii={non_ascii_count}/{len(val_str)})")
                            break

                except Exception as col_err:
                    logger.debug(f"Could not check column '{col}' for binary data: {col_err}")
                    # If we can't even check it, it's probably binary
                    binary_data_cols.add(col.upper())

        except Exception as e:
            logger.warning(f"Could not perform binary data detection: {e}")
            # Return empty set - will try caching and fail later if needed

        return binary_data_cols

    def _cache_columns_individually(self, table_name: str, columns: List[str]) -> List[str]:
        """
        Fall back to caching columns one by one to identify problematic ones.

        Args:
            table_name: Table to cache from
            columns: List of columns to cache

        Returns:
            List of successfully cached column names
        """
        successfully_cached = []
        failed_columns = []

        for col in columns:
            try:
                query_single = f'SELECT "{col}" FROM {table_name} LIMIT {self.sample_size}'
                temp_table = f"temp_{len(successfully_cached)}"
                self.connector.cache_query(query_single, temp_table)
                successfully_cached.append(col)
            except Exception as col_error:
                logger.warning(f"Failed to cache column '{col}': {col_error}")
                failed_columns.append(col)

        if not successfully_cached:
            raise Exception("Could not cache any columns - all columns have encoding/binary issues")

        logger.info(f"Successfully cached {len(successfully_cached)}/{len(columns)} columns")
        if failed_columns:
            logger.warning(f"Failed columns: {failed_columns}")

        # Join all temp tables into profiled_table
        cache_conn = self.connector.get_cache_connection()

        # Drop existing profiled_table if it exists (from previous failed attempts)
        try:
            cache_conn.execute("DROP TABLE IF EXISTS profiled_table")
        except:
            pass

        if len(successfully_cached) == 1:
            cache_conn.execute(f"CREATE TABLE profiled_table AS SELECT * FROM temp_0")
        else:
            # Join all temp tables by row number
            select_parts = [f't0.*']
            join_parts = [f'(SELECT ROW_NUMBER() OVER () as rn, * FROM temp_0) t0']

            for idx in range(1, len(successfully_cached)):
                select_parts.append(f't{idx}."{successfully_cached[idx]}"')
                join_parts.append(f'LEFT JOIN (SELECT ROW_NUMBER() OVER () as rn, * FROM temp_{idx}) t{idx} ON t0.rn = t{idx}.rn')

            create_query = f"CREATE TABLE profiled_table AS SELECT {', '.join(select_parts)} FROM {' '.join(join_parts)}"
            cache_conn.execute(create_query)

        # Clean up temp tables
        for idx in range(len(successfully_cached)):
            try:
                cache_conn.execute(f"DROP TABLE IF EXISTS temp_{idx}")
            except:
                pass

        return successfully_cached

    def _is_binary_type(self, field: pa.Field) -> bool:
        """Check if a PyArrow field is a binary type."""
        if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
            return True

        type_str = str(field.type).lower()
        binary_keywords = ['binary', 'blob', 'varbinary']
        return any(keyword in type_str for keyword in binary_keywords)

    def _classify_column(self, field: pa.Field) -> str:
        """
        Classify column into NUMERICAL, CATEGORICAL, TEMPORAL, BOOLEAN, or OTHER.

        Args:
            field: PyArrow field

        Returns:
            Classification string
        """
        if self.schema_validator.is_numerical_type(field.type):
            return 'NUMERICAL'
        elif self.schema_validator.is_temporal_type(field.type):
            return 'TEMPORAL'
        elif pa.types.is_boolean(field.type):
            return 'BOOLEAN'
        elif self.schema_validator.is_categorical_type(field.type):
            return 'CATEGORICAL'
        else:
            return 'OTHER'

    def _calculate_table_metrics(
        self,
        conn: duckdb.DuckDBPyConnection,
        sample_size: int,
        column_profiles: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Calculate table-level aggregate metrics.

        Args:
            conn: DuckDB connection
            sample_size: Number of rows in sample
            column_profiles: List of column profile dictionaries

        Returns:
            Dictionary with table-level metrics
        """
        column_count = len(column_profiles)
        total_cells = sample_size * column_count

        # Count total null cells
        total_nulls = sum(
            col.get('basic_stats', {}).get('null_count', 0)
            for col in column_profiles
        )

        null_percentage = round(total_nulls / total_cells * 100, 2) if total_cells > 0 else 0.0

        # Count unique columns (potential IDs)
        unique_columns = sum(
            1 for col in column_profiles
            if col.get('basic_stats', {}).get('is_unique', False)
        )

        metrics = {
            'total_rows': sample_size,
            'total_columns': column_count,
            'total_cells': total_cells,
            'null_cells': total_nulls,
            'null_percentage': null_percentage,
            'completeness_percentage': round(100 - null_percentage, 2),
            'unique_columns_count': unique_columns
        }

        return metrics
