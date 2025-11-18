"""Main table comparison engine with statistical validation."""

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
from datetime import datetime
from typing import Dict, List, Any, Optional
from scipy.stats import false_discovery_control
from ..connectors.base_connector import BaseConnector
from ..connectors.hana_connector import HanaConnector
from ..connectors.dremio_connector import DremioConnector
from ..utils.logger import get_logger
from .statistical_tests import StatisticalTests, TestResult
from .schema_validator import SchemaValidator


logger = get_logger('comparator')


class TableComparator:
    """
    Compares source and destination tables using statistical tests.
    
    Supports both single-source and cross-source comparisons.
    
    Implements comprehensive validation including:
    - Row count validation
    - Schema validation
    - Null rate comparison
    - KS-test for numerical distributions
    - T-test for means
    - PSI for categorical distributions
    - Chi-square for categorical independence
    - Date range tests for temporal columns
    """
    
    def __init__(
        self,
        source_connector: BaseConnector,
        dest_connector: Optional[BaseConnector] = None,
        config: Dict[str, Any] = None
    ):
        """
        Initialize table comparator.
        
        Args:
            source_connector: Connector for source data (e.g., HANA)
            dest_connector: Optional separate connector for destination data (e.g., Dremio).
                           If None, uses source_connector for both.
            config: Configuration dictionary from ConfigLoader
        """
        self.source_connector = source_connector
        self.dest_connector = dest_connector if dest_connector else source_connector
        self.config = config or {}
        
        # Initialize validators
        thresholds = self.config.get('thresholds', {})
        sampling_config = self.config.get('sampling', {})
        categorical_config = self.config.get('categorical', {})
        
        self.statistical_tests = StatisticalTests(
            ks_test_pvalue=thresholds.get('ks_test_pvalue', 0.05),
            t_test_pvalue=thresholds.get('t_test_pvalue', 0.05),
            chi_square_pvalue=thresholds.get('chi_square_pvalue', 0.05),
            psi_threshold=thresholds.get('psi_threshold', 0.1),
            min_sample_size=sampling_config.get('min_sample_size', 30)
        )
        
        self.schema_validator = SchemaValidator()
        
        # Configuration
        self.row_count_threshold_pct = thresholds.get('row_count_tolerance_pct', 0.1)
        self.null_rate_threshold_pct = thresholds.get('null_rate_tolerance_pct', 2.0)
        self.sample_size = sampling_config.get('max_sample_size', 50000)
        self.sampling_enabled = sampling_config.get('enabled', True)
        self.sampling_strategy = sampling_config.get('strategy', 'random')
        self.sampling_pct = sampling_config.get('target_pct', None)
        self.sampling_min_size = sampling_config.get('min_size', 1000)
        self.sampling_max_size = sampling_config.get('max_size', 1000000)
        self.sampling_seed = sampling_config.get('seed', 42)
        self.sampling_hash_column = sampling_config.get('hash_column', None)
        self.max_cardinality_psi = categorical_config.get('max_cardinality_for_psi', 100)
        self.max_cardinality_chi_square = categorical_config.get('max_cardinality_for_chi_square', 50)

    def _is_binary_type(self, field: pa.Field) -> bool:
        """
        Check if a PyArrow field is a binary type.

        Args:
            field: PyArrow field to check

        Returns:
            True if field is binary/varbinary/blob type
        """
        # Check using PyArrow type functions
        if pa.types.is_binary(field.type) or pa.types.is_large_binary(field.type):
            return True

        # Also check string representation for database-specific types (VARBINARY, BLOB, etc.)
        type_str = str(field.type).lower()
        binary_keywords = ['binary', 'blob', 'varbinary']
        return any(keyword in type_str for keyword in binary_keywords)

    def _detect_hash_column(self, schema: pa.Schema, exclude_columns: set = None) -> Optional[str]:
        """
        Auto-detect a suitable column for hash-based sampling.

        Looks for columns like 'id', 'key', etc. Skips binary columns.

        Args:
            schema: PyArrow schema of the table
            exclude_columns: Set of column names to exclude (e.g., binary columns)

        Returns:
            Column name if found, None otherwise
        """
        if exclude_columns is None:
            exclude_columns = set()

        # Priority order for hash column candidates
        candidates = ['id', 'key', 'pk', 'primary_key', 'row_id', 'rowid']

        # First pass: exact matches (case-insensitive), skip binary and excluded
        for field in schema:
            if field.name.upper() in exclude_columns:
                continue
            if self._is_binary_type(field):
                logger.debug(f"Skipping binary column {field.name} for hash candidate")
                continue
            if field.name.lower() in candidates:
                logger.info(f"Auto-detected hash column: {field.name}")
                return field.name

        # Second pass: columns containing these patterns, skip binary and excluded
        for field in schema:
            if field.name.upper() in exclude_columns:
                continue
            if self._is_binary_type(field):
                continue
            for candidate in candidates:
                if candidate in field.name.lower():
                    logger.info(f"Auto-detected hash column: {field.name}")
                    return field.name

        # Fallback: use first non-binary column if numeric or string type
        for field in schema:
            if field.name.upper() in exclude_columns:
                continue
            if self._is_binary_type(field):
                continue
            if pa.types.is_integer(field.type) or pa.types.is_string(field.type):
                logger.info(f"Using first suitable column for hashing: {field.name}")
                return field.name

        logger.warning("No suitable hash column found (all candidates are binary or excluded) - falling back to random sampling")
        return None

    def _calculate_sample_size(self, total_rows: int) -> int:
        """
        Calculate optimal sample size based on table size and configuration.

        Args:
            total_rows: Total number of rows in table

        Returns:
            Calculated sample size
        """
        if self.sampling_pct:
            # Percentage-based sampling
            calculated_size = int(total_rows * self.sampling_pct / 100)
        else:
            # Fixed size (legacy behavior)
            calculated_size = self.sample_size

        # Apply min/max bounds
        sample_size = max(self.sampling_min_size, min(calculated_size, self.sampling_max_size))

        # Don't sample more than table size
        sample_size = min(sample_size, total_rows)

        logger.info(f"Calculated sample size: {sample_size:,} ({sample_size/total_rows*100:.2f}% of {total_rows:,} rows)")
        return sample_size

    def _build_column_list(
        self,
        columns: List[str],
        schema: pa.Schema,
        connector: BaseConnector
    ) -> str:
        """
        Build column list with null-equivalent transformations if applicable.

        Args:
            columns: List of column names to include
            schema: PyArrow schema with column types
            connector: Connector instance (to check if it supports transformations)

        Returns:
            Comma-separated column list, with NULLIF transformations if supported
        """
        # Create a mapping from column name to type
        schema_map = {field.name: field.type for field in schema}

        # Check if connector supports null-equivalent transformations
        has_transform_method = hasattr(connector, 'transform_column_for_null_equivalents')

        if has_transform_method and (isinstance(connector, (HanaConnector, DremioConnector))):
            transformed_cols = []
            for col in columns:
                quoted_col = f'"{col}"'
                if col in schema_map:
                    # Apply null-equivalent transformation
                    transformed_col = connector.transform_column_for_null_equivalents(
                        quoted_col,
                        schema_map[col]
                    )
                    transformed_cols.append(transformed_col)
                else:
                    # Column not in schema, use as-is
                    transformed_cols.append(quoted_col)
            return ', '.join(transformed_cols)
        else:
            # For connectors without transformations, just quote column names
            return ', '.join([f'"{col}"' for col in columns])

    def _build_sample_query(
        self,
        column_list: str,
        table_name: str,
        schema: pa.Schema,
        is_hana: bool,
        exclude_binary_cols: set = None,
        where_clause: str = None
    ) -> str:
        """
        Build sampling query based on configured strategy.

        Args:
            column_list: Comma-separated quoted column list
            table_name: Table name
            schema: Table schema for hash column detection
            is_hana: True if HANA connector, False for Dremio
            exclude_binary_cols: Set of binary column names to exclude from hash selection
            where_clause: Optional WHERE clause for filtering (e.g., "TO_DATE(REFRESH_DT) = TO_DATE('2025-11-04')")

        Returns:
            SQL query string with sampling
        """
        if exclude_binary_cols is None:
            exclude_binary_cols = set()

        # Build base WHERE clause
        base_where = f"WHERE {where_clause}" if where_clause else ""

        if not self.sampling_enabled:
            return f"SELECT {column_list} FROM {table_name} {base_where}".strip()

        # Determine sample size strategy
        # Only fetch row count if target_pct is explicitly set (to avoid slow COUNT(*) on large tables)
        if self.sampling_pct is not None:
            try:
                logger.info(f"Fetching row count for percentage-based sampling...")
                row_count = self.source_connector.get_row_count(table_name)
                sample_size = self._calculate_sample_size(row_count)

                # If sample size >= total rows, don't sample
                if sample_size >= row_count:
                    logger.info(f"Sample size ({sample_size}) >= table size ({row_count}), querying full table")
                    return f"SELECT {column_list} FROM {table_name} {base_where}".strip()
            except Exception as e:
                logger.warning(f"Could not get row count for {table_name}: {e}. Using fixed sample size.")
                sample_size = self.sample_size
                row_count = sample_size * 2  # Estimate
        else:
            # Use fixed sample size (no row count needed - fast!)
            sample_size = self.sample_size
            row_count = sample_size * 2  # Estimate for hash percentage calculation
            logger.info(f"Using fixed sample size: {sample_size:,} rows (no row count fetch needed)")

        if self.sampling_strategy == 'hash':
            # Hash-based deterministic sampling
            hash_col = self.sampling_hash_column or self._detect_hash_column(schema, exclude_binary_cols)

            if hash_col:
                sample_pct = int(sample_size / row_count * 100) + 1  # +1 to ensure we get enough rows

                # Combine hash WHERE with user WHERE clause using AND
                hash_where = f"MOD(ABS(HASH_SHA256(TO_VARCHAR(\"{hash_col}\"))), 100) < {sample_pct}" if is_hana else f"MOD(ABS(HASH(\"{hash_col}\")), 100) < {sample_pct}"
                combined_where = f"{where_clause} AND {hash_where}" if where_clause else hash_where

                if is_hana:
                    # HANA: MOD(ABS(HASH_SHA256(column)), 100)
                    query = f"""
                    SELECT {column_list} FROM {table_name}
                    WHERE {combined_where}
                    LIMIT {sample_size}
                    """
                else:
                    # Dremio: MOD(ABS(HASH(column)), 100)
                    query = f"""
                    SELECT {column_list} FROM {table_name}
                    WHERE {combined_where}
                    LIMIT {sample_size}
                    """

                logger.info(f"Using hash-based sampling on column '{hash_col}' (strategy: hash, seed: deterministic)")
                return query.strip()
            else:
                logger.warning("Hash-based sampling requested but no suitable column found - falling back to random")

        # Fallback: Random sampling (legacy behavior, but with LIMIT only - no ORDER BY)
        if is_hana:
            # HANA: Use RAND() for backward compatibility but warn about performance
            logger.warning("Using ORDER BY RAND() - this is slow on large tables. Consider hash-based sampling.")
            query = f"SELECT {column_list} FROM {table_name} {base_where} ORDER BY RAND() LIMIT {sample_size}".replace("  ", " ").strip()
        else:
            # Dremio: Use random() for backward compatibility
            logger.warning("Using ORDER BY random() - this is slow on large tables. Consider hash-based sampling.")
            query = f"SELECT {column_list} FROM {table_name} {base_where} ORDER BY random() LIMIT {sample_size}".replace("  ", " ").strip()

        return query

    def _build_fallback_query(
        self,
        column_list: str,
        table_name: str,
        is_hana: bool,
        where_clause: str = None
    ) -> str:
        """
        Build fallback query using ORDER BY RAND/random() sampling.

        This is used when hash-based sampling fails (e.g., due to binary columns).

        Args:
            column_list: Comma-separated quoted column list
            table_name: Table name
            is_hana: True if HANA connector, False for Dremio
            where_clause: Optional WHERE clause for filtering

        Returns:
            SQL query string with simple random sampling
        """
        base_where = f"WHERE {where_clause}" if where_clause else ""

        if not self.sampling_enabled:
            return f"SELECT {column_list} FROM {table_name} {base_where}".strip()

        sample_size = self.sample_size
        logger.info(f"Using fallback ORDER BY RAND() sampling: {sample_size:,} rows")

        if is_hana:
            query = f"SELECT {column_list} FROM {table_name} {base_where} ORDER BY RAND() LIMIT {sample_size}".replace("  ", " ").strip()
        else:
            query = f"SELECT {column_list} FROM {table_name} {base_where} ORDER BY random() LIMIT {sample_size}".replace("  ", " ").strip()

        return query

    def compare(
        self,
        source_table: str,
        dest_table: str,
        columns_to_test: Optional[List[str]] = None,
        source_where: Optional[str] = None,
        dest_where: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Main comparison function.

        Args:
            source_table: Fully qualified source table name
            dest_table: Fully qualified destination table name
            columns_to_test: Optional list of specific columns to test
            source_where: Optional WHERE clause for source table (e.g., "TO_DATE(REFRESH_DT) = TO_DATE('2025-11-04')")
            dest_where: Optional WHERE clause for destination table (e.g., "CAST(system_ts AS DATE) = DATE '2025-11-04'")

        Returns:
            Dictionary with comparison results
        """
        logger.info(f"Starting comparison: {source_table} → {dest_table}")
        print(f"\n{'='*60}")
        print(f"Comparing: {source_table} → {dest_table}")
        print(f"{'='*60}")
        
        result = {
            'source_table': source_table,
            'dest_table': dest_table,
            'timestamp': datetime.now().isoformat(),
            'overall_status': 'PASS',
            'summary': {},
            'tests': []
        }
        
        # Store table names for categorical distribution queries
        self.source_table_name = source_table
        self.dest_table_name = dest_table
        
        # Phase 1: Basic Validation
        logger.info("Phase 1: Basic validation")
        print("\n[Phase 1] Basic Validation...")

        row_test = self._test_row_count(source_table, dest_table, source_where, dest_where)
        result['tests'].append(row_test.to_dict())
        print(f"  Row Count: {row_test.status} ({row_test.details.get('difference', 0)} rows diff)")
        
        schema_test = self._test_schema(source_table, dest_table)
        result['tests'].append(schema_test.to_dict())
        print(f"  Schema: {schema_test.status}")
        
        # Get common columns from schema test
        common_column_names = None
        if schema_test.status == 'FAIL':
            logger.warning("Schema mismatch detected - will test common columns only")
            print("\n⚠️  Schema mismatch - testing common columns only")
            
            # Get common columns (case-insensitive)
            source_schema = self.source_connector.get_table_schema(source_table)
            dest_schema = self.dest_connector.get_table_schema(dest_table)
            common_column_names = self.schema_validator.get_common_columns(source_schema, dest_schema)
            
            if len(common_column_names) == 0:
                print("   No common columns to test - skipping column tests")
                self._finalize_result(result)
                return result
            
            print(f"   Found {len(common_column_names)} common columns to test")
        
        # Phase 2: Cache tables for efficient column testing
        logger.info("Phase 2: Caching tables")
        print("\n[Phase 2] Caching Tables...")

        # Use common columns if schema failed, otherwise use user-specified or all
        cols_to_cache = common_column_names if common_column_names else columns_to_test

        try:
            cached_source_cols, cached_dest_cols = self._cache_tables(
                source_table, dest_table, cols_to_cache, source_where, dest_where
            )
        except Exception as e:
            logger.error(f"Failed to cache tables: {str(e)}")
            print(f"\n❌ Error caching tables: {str(e)}")
            print("   Cannot proceed with column-level tests")
            self._finalize_result(result)
            return result
        
        # Only test columns that were successfully cached (case-insensitive match)
        cached_source_upper = [c.upper() for c in cached_source_cols]
        cached_dest_upper = [c.upper() for c in cached_dest_cols]
        
        if cols_to_cache:
            # Filter to only columns that exist in cache
            cols_to_test_filtered = [
                col for col in cols_to_cache 
                if col.upper() in cached_source_upper and col.upper() in cached_dest_upper
            ]
            
            dropped_cols = set(cols_to_cache) - set(cols_to_test_filtered)
            if dropped_cols:
                print(f"   ⚠️  {len(dropped_cols)} columns dropped during caching (binary/encoding issues)")
                logger.warning(f"Columns dropped during caching: {dropped_cols}")
        else:
            # Get common cached columns
            cols_to_test_filtered = [
                col for col in cached_source_cols 
                if col.upper() in cached_dest_upper
            ]
        
        # Phase 3: Column-level statistical tests
        logger.info("Phase 3: Statistical tests on columns")
        print("\n[Phase 3] Statistical Tests on Columns...")
        
        column_tests = self._test_columns(source_table, dest_table, cols_to_test_filtered)
        result['tests'].extend([test.to_dict() for test in column_tests])
        
        # Finalize results
        self._finalize_result(result)
        self._print_summary(result)
        
        return result
    
    def _test_row_count(
        self,
        source_table: str,
        dest_table: str,
        source_where: Optional[str] = None,
        dest_where: Optional[str] = None
    ) -> TestResult:
        """
        Test row count validation using ratio-based comparison.

        Args:
            source_table: Source table name
            dest_table: Destination table name
            source_where: Optional WHERE clause for source (for filtered counts)
            dest_where: Optional WHERE clause for destination (for filtered counts)

        Returns:
            TestResult with row count comparison
        """
        logger.debug(f"Testing row count for {source_table} vs {dest_table}")

        try:
            # If WHERE clauses provided, use filtered counts
            if source_where or dest_where:
                logger.info("Using filtered row counts (WHERE clause provided)")

                # Build COUNT queries with WHERE clauses
                source_query = f"SELECT COUNT(*) FROM {source_table}"
                if source_where:
                    source_query += f" WHERE {source_where}"

                dest_query = f"SELECT COUNT(*) FROM {dest_table}"
                if dest_where:
                    dest_query += f" WHERE {dest_where}"

                # Execute count queries
                source_result = self.source_connector.execute_query(source_query)
                dest_result = self.dest_connector.execute_query(dest_query)

                # Extract counts from results (handle PyArrow Table format)
                if hasattr(source_result, 'to_pylist'):
                    # PyArrow Table: to_pylist() returns list of dicts
                    source_count = list(source_result.to_pylist()[0].values())[0]
                    dest_count = list(dest_result.to_pylist()[0].values())[0]
                else:
                    # Fallback for other formats
                    source_count = source_result[0][0]
                    dest_count = dest_result[0][0]
            else:
                # No WHERE clause - use optimized get_row_count() (existing behavior)
                source_count = self.source_connector.get_row_count(source_table)
                dest_count = self.dest_connector.get_row_count(dest_table)
            
            diff = dest_count - source_count

            # Threshold as ratio (e.g., 0.1% tolerance = 0.999 ratio)
            threshold_ratio = 1.0 - (self.row_count_threshold_pct / 100)

            # Handle edge case: both tables are empty (0 rows)
            if source_count == 0 and dest_count == 0:
                diff_pct = 0
                ratio = 1.0  # Perfect match (both empty)
                status = 'PASS'
            elif source_count == 0 and dest_count > 0:
                # Destination has data but source is empty
                diff_pct = 100
                ratio = float('inf')  # Infinite ratio
                status = 'FAIL'
            else:
                # Normal case: source has data
                diff_pct = abs(diff / source_count * 100)
                ratio = dest_count / source_count

                # Determine status based on ratio
                if ratio == 1.0:
                    status = 'PASS'
                elif ratio >= threshold_ratio:
                    status = 'WARNING'
                else:
                    status = 'FAIL'

            return TestResult(
                test_name='row_count',
                column=None,
                status=status,
                details={
                    'source_count': source_count,
                    'dest_count': dest_count,
                    'difference': diff,
                    'difference_pct': round(diff_pct, 3),
                    'ratio': round(ratio, 6) if ratio != float('inf') else None,
                    'threshold_pct': self.row_count_threshold_pct,
                    'threshold_ratio': round(threshold_ratio, 6)
                }
            )
        except Exception as e:
            logger.error(f"Row count test failed: {str(e)}")
            return TestResult(
                test_name='row_count',
                column=None,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def _test_schema(self, source_table: str, dest_table: str) -> TestResult:
        """Test schema validation."""
        logger.debug(f"Testing schema for {source_table} vs {dest_table}")
        
        try:
            source_schema = self.source_connector.get_table_schema(source_table)
            dest_schema = self.dest_connector.get_table_schema(dest_table)
            
            return self.schema_validator.compare_schemas(
                source_schema,
                dest_schema,
                source_table,
                dest_table
            )
        except Exception as e:
            logger.error(f"Schema test failed: {str(e)}")
            return TestResult(
                test_name='schema_comparison',
                column=None,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def _normalize_cached_table_columns(self, conn: duckdb.DuckDBPyConnection, table_name: str):
        """
        Normalize all column names in a cached table to lowercase.

        This ensures case-insensitive comparison between SAP (uppercase) and Dremio (lowercase).

        Args:
            conn: DuckDB connection
            table_name: Name of the cached table to normalize
        """
        try:
            # Get current column names
            columns_info = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            columns = [col[1] for col in columns_info]  # col[1] is the column name

            # Check if any columns need normalization (have uppercase letters)
            needs_normalization = any(col != col.lower() for col in columns)
            if not needs_normalization:
                logger.debug(f"Table {table_name} already has lowercase column names")
                return

            # Build column list with lowercase aliases
            col_mappings = [f'"{col}" AS "{col.lower()}"' for col in columns]
            col_list = ', '.join(col_mappings)

            # Step 1: Create new table with normalized column names
            conn.execute(f"CREATE TABLE {table_name}_temp AS SELECT {col_list} FROM {table_name}")

            # Step 2: Drop original table
            conn.execute(f"DROP TABLE {table_name}")

            # Step 3: Rename new table to original name
            conn.execute(f"ALTER TABLE {table_name}_temp RENAME TO {table_name}")

            logger.debug(f"Normalized {len(columns)} column names to lowercase in {table_name}")
        except Exception as e:
            logger.warning(f"Failed to normalize column names in {table_name}: {str(e)}")
            # Continue anyway - the original table still exists

    def _cache_tables(
        self,
        source_table: str,
        dest_table: str,
        columns: Optional[List[str]] = None,
        source_where: Optional[str] = None,
        dest_where: Optional[str] = None
    ) -> tuple:
        """
        Cache source and destination tables to DuckDB.

        Args:
            source_table: Source table name
            dest_table: Destination table name
            columns: Optional list of columns to cache
            source_where: Optional WHERE clause for source table
            dest_where: Optional WHERE clause for destination table

        Returns:
            Tuple of (source_cached_columns, dest_cached_columns)
        """
        logger.info("Caching source and destination tables")
        
        # Get schemas to map column names correctly
        source_schema = self.source_connector.get_table_schema(source_table)
        dest_schema = self.dest_connector.get_table_schema(dest_table)
        
        # Filter out binary columns (they cause encoding issues)
        # Use _is_binary_type() for consistent detection
        # Debug: log all column types to identify binary columns
        for field in source_schema:
            logger.debug(f"Column {field.name}: type={field.type}, str={str(field.type)}, is_binary={self._is_binary_type(field)}")

        source_binary_cols = set([
            field.name.upper() for field in source_schema
            if self._is_binary_type(field)
        ])
        
        # Get cacheable columns (exclude binary from source)
        source_cacheable = [
            field.name for field in source_schema 
            if field.name.upper() not in source_binary_cols
        ]
        
        # For dest, exclude both:
        # 1. Columns that are binary type in dest
        # 2. Columns that are binary in source (even if string in dest - they contain binary data)
        dest_cacheable = [
            field.name for field in dest_schema
            if not self._is_binary_type(field) and field.name.upper() not in source_binary_cols
        ]
        
        if source_binary_cols:
            logger.warning(f"Excluding {len(source_binary_cols)} binary columns from caching: {source_binary_cols}")
        
        # Create mapping from source columns to dest columns (case-insensitive)
        source_col_map = {field.name.upper(): field.name for field in source_schema if field.name in source_cacheable}
        dest_col_map = {field.name.upper(): field.name for field in dest_schema if field.name in dest_cacheable}
        
        if columns:
            # Map source column names to their equivalents in dest (excluding binary)
            source_cols = []
            dest_cols = []

            for col in columns:
                col_upper = col.upper()
                if col_upper in source_col_map:
                    source_cols.append(source_col_map[col_upper])
                if col_upper in dest_col_map:
                    dest_cols.append(dest_col_map[col_upper])
        else:
            # Select all non-binary columns
            source_cols = source_cacheable
            dest_cols = dest_cacheable

        # Build column lists with SAP null transformations if needed
        source_col_list = self._build_column_list(source_cols, source_schema, self.source_connector)
        dest_col_list = self._build_column_list(dest_cols, dest_schema, self.dest_connector)

        # Build queries with optimized sampling (exclude binary columns from hash selection)
        source_query = self._build_sample_query(
            source_col_list,
            source_table,
            source_schema,
            isinstance(self.source_connector, HanaConnector),
            source_binary_cols,
            source_where
        )

        dest_query = self._build_sample_query(
            dest_col_list,
            dest_table,
            dest_schema,
            isinstance(self.dest_connector, HanaConnector),
            source_binary_cols,  # Use source binary cols for dest too (same columns)
            dest_where
        )
        
        print(f"  Caching source table (sample: {self.sampling_enabled})...")
        try:
            self.source_connector.cache_query(source_query, "cached_source")
        except Exception as e:
            logger.warning(f"Hash-based caching failed for source table: {str(e)}")
            logger.info("Falling back to ORDER BY RAND() sampling...")

            # Fall back to simple random sampling without hash column
            source_query_fallback = self._build_fallback_query(
                source_col_list,
                source_table,
                isinstance(self.source_connector, HanaConnector),
                source_where
            )
            self.source_connector.cache_query(source_query_fallback, "cached_source")

        print(f"  Caching destination table (sample: {self.sampling_enabled})...")
        try:
            self.dest_connector.cache_query(dest_query, "cached_dest")
        except Exception as e:
            logger.warning(f"Hash-based caching failed for destination table: {str(e)}")
            logger.info("Falling back to ORDER BY RAND() sampling...")

            # Fall back to simple random sampling without hash column
            dest_query_fallback = self._build_fallback_query(
                dest_col_list,
                dest_table,
                isinstance(self.dest_connector, HanaConnector),
                dest_where
            )
            self.dest_connector.cache_query(dest_query_fallback, "cached_dest")

        # Normalize column names to lowercase for case-insensitive comparison
        # Each table is in its respective connector's cache, so use separate connections
        source_conn = self.source_connector.get_cache_connection()
        dest_conn = self.dest_connector.get_cache_connection()
        self._normalize_cached_table_columns(source_conn, "cached_source")
        self._normalize_cached_table_columns(dest_conn, "cached_dest")

        # Get actual cached columns (some may have been dropped during caching)
        # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
        # We need index 1 for the column name
        cached_source_cols = [col[1] for col in source_conn.execute("PRAGMA table_info(cached_source)").fetchall()]
        cached_dest_cols = [col[1] for col in dest_conn.execute("PRAGMA table_info(cached_dest)").fetchall()]

        logger.info(f"Tables cached successfully. Source: {len(cached_source_cols)} cols, Dest: {len(cached_dest_cols)} cols")

        return cached_source_cols, cached_dest_cols
    
    def _test_columns(
        self,
        source_table: str,
        dest_table: str,
        columns_to_test: Optional[List[str]] = None
    ) -> List[TestResult]:
        """Run statistical tests on all columns."""
        results = []

        # Get schema and classify columns
        source_schema = self.source_connector.get_table_schema(source_table)
        column_classification = self.schema_validator.classify_columns(source_schema)

        # Get DuckDB connections for cached data (each table is in its respective connector's cache)
        source_conn = self.source_connector.get_cache_connection()
        dest_conn = self.dest_connector.get_cache_connection()

        # Determine which columns to test (excluding binary columns)
        if columns_to_test:
            # User specified or common columns from schema mismatch - still need to filter out binary
            # Create a map of column names (case-insensitive) to their PyArrow fields
            schema_map_lower = {field.name.upper(): field for field in source_schema}
            all_columns = []
            binary_excluded = []

            for col in columns_to_test:
                col_upper = col.upper()
                if col_upper in schema_map_lower:
                    field = schema_map_lower[col_upper]
                    if self._is_binary_type(field):
                        binary_excluded.append(col)
                    else:
                        all_columns.append(col)
                else:
                    # Column not in schema, include it (might be in dest only)
                    all_columns.append(col)

            if binary_excluded:
                logger.info(f"Excluding {len(binary_excluded)} binary columns from statistical tests: {binary_excluded}")
        else:
            # All columns from source, excluding binary types
            all_columns = [field.name for field in source_schema if not self._is_binary_type(field)]

            # Log if any binary columns were excluded
            binary_cols = [field.name for field in source_schema if self._is_binary_type(field)]
            if binary_cols:
                logger.info(f"Excluding {len(binary_cols)} binary columns from statistical tests: {binary_cols}")

        # PERFORMANCE OPTIMIZATION: Fetch all null counts in batch (2 queries instead of 2*N queries)
        print(f"\n  Fetching null counts for all {len(all_columns)} columns in batch...")
        try:
            src_null_counts, dst_null_counts, src_total, dst_total = self._get_all_null_counts(all_columns)
            print(f"  ✓ Null counts fetched successfully")
        except Exception as e:
            logger.error(f"Batch null count failed, falling back to per-column queries: {str(e)}")
            # Fallback: create empty dicts to trigger per-column fallback
            src_null_counts = {}
            dst_null_counts = {}
            src_total = 0
            dst_total = 0

        for idx, col_name in enumerate(all_columns, 1):
            print(f"\n  Column [{idx}/{len(all_columns)}]: {col_name}")
            logger.debug(f"Testing column: {col_name}")

            # Use lowercase for cached data
            col_name_lower = col_name.lower()

            # Null rate test (always) - now using pre-fetched batch data
            if col_name in src_null_counts and col_name in dst_null_counts:
                null_test = self._test_null_rate(
                    col_name,
                    src_null_counts[col_name],
                    dst_null_counts[col_name],
                    src_total,
                    dst_total
                )
            else:
                # Fallback: column not in batch results (shouldn't happen, but handle gracefully)
                logger.warning(f"Column {col_name} not in batch null counts, skipping null rate test")
                null_test = TestResult(
                    test_name='null_rate',
                    column=col_name,
                    status='ERROR',
                    details={'error': 'Column not found in batch null count results'}
                )

            results.append(null_test)
            print(f"    Null Rate: {null_test.status} "
                  f"(src={null_test.details.get('source_null_pct', 0):.1f}%, "
                  f"dst={null_test.details.get('dest_null_pct', 0):.1f}%)")

            # Type-specific tests
            if col_name in column_classification['numerical']:
                results.extend(self._test_numerical_column(source_conn, dest_conn, col_name_lower, col_name))
            elif col_name in column_classification['categorical']:
                results.extend(self._test_categorical_column(source_conn, dest_conn, col_name_lower, col_name))
            elif col_name in column_classification['temporal']:
                results.extend(self._test_temporal_column(source_conn, dest_conn, col_name_lower, col_name))
            else:
                print(f"    Unsupported type - skipped")

        source_conn.close()
        dest_conn.close()
        return results
    
    def _get_all_null_counts(self, columns: List[str]) -> tuple:
        """Get null counts for all columns in a single query for both source and destination.

        This is a major performance optimization - instead of running 2*N full table scans
        (one per column per table), we run just 2 queries total (one per table).

        For a 100-column table, this reduces 200 full scans to just 2 scans.

        IMPORTANT: This queries the cached tables (cached_source and cached_dest) which
        already have null-equivalent transformations applied (e.g., empty strings -> NULL
        in Dremio, '00000000' -> NULL in SAP HANA).

        Args:
            columns: List of column names to check (using original display case)

        Returns:
            Tuple of (source_null_counts, dest_null_counts, src_total, dst_total)
            where null_counts are dicts mapping column -> null count
        """
        try:
            # Get DuckDB connections (each cached table is in its respective connector's cache)
            source_conn = self.source_connector.get_cache_connection()
            dest_conn = self.dest_connector.get_cache_connection()

            # Get total row counts from cached tables
            src_total = source_conn.execute("SELECT COUNT(*) FROM cached_source").fetchone()[0]
            dst_total = dest_conn.execute("SELECT COUNT(*) FROM cached_dest").fetchone()[0]

            # Build CASE statements for cached tables (columns are lowercase in DuckDB)
            src_case_statements = []
            dst_case_statements = []

            for col in columns:
                col_lower = col.lower()  # DuckDB stores columns in lowercase
                src_case_statements.append(
                    f'SUM(CASE WHEN "{col_lower}" IS NULL THEN 1 ELSE 0 END) as "{col}_nulls"'
                )
                dst_case_statements.append(
                    f'SUM(CASE WHEN "{col_lower}" IS NULL THEN 1 ELSE 0 END) as "{col}_nulls"'
                )

            # Execute single query per cached table
            src_case_list = ', '.join(src_case_statements)
            dst_case_list = ', '.join(dst_case_statements)

            src_query = f'SELECT {src_case_list} FROM cached_source'
            dst_query = f'SELECT {dst_case_list} FROM cached_dest'

            logger.info(f"Fetching null counts for {len(columns)} columns from cached tables (2 queries total)...")
            logger.debug(f"Source null count query (cached): {src_query[:500]}...")
            logger.debug(f"Dest null count query (cached): {dst_query[:500]}...")

            src_result = source_conn.execute(src_query).fetchdf().iloc[0]
            dst_result = dest_conn.execute(dst_query).fetchdf().iloc[0]

            # Parse results into dictionaries
            src_null_counts = {col: int(src_result[f'{col}_nulls']) for col in columns}
            dst_null_counts = {col: int(dst_result[f'{col}_nulls']) for col in columns}

            source_conn.close()
            dest_conn.close()
            return src_null_counts, dst_null_counts, src_total, dst_total

        except Exception as e:
            logger.error(f"Batch null count query failed: {str(e)}")
            raise

    def _test_null_rate(self, col_name_display: str, src_nulls: int, dst_nulls: int,
                       src_total: int, dst_total: int) -> TestResult:
        """Test null rate comparison using pre-fetched null counts.

        This method is now called with batch-fetched data instead of querying per column.
        """
        try:
            src_pct = (src_nulls / src_total * 100) if src_total > 0 else 0
            dst_pct = (dst_nulls / dst_total * 100) if dst_total > 0 else 0

            diff = abs(dst_pct - src_pct)
            status = 'PASS' if diff <= self.null_rate_threshold_pct else 'FAIL'

            return TestResult(
                test_name='null_rate',
                column=col_name_display,
                status=status,
                details={
                    'source_null_pct': round(src_pct, 2),
                    'dest_null_pct': round(dst_pct, 2),
                    'difference_pct': round(diff, 2),
                    'threshold_pct': self.null_rate_threshold_pct,
                    'source_total_rows': src_total,
                    'dest_total_rows': dst_total,
                    'source_null_rows': int(src_nulls),
                    'dest_null_rows': int(dst_nulls)
                }
            )
        except Exception as e:
            logger.error(f"Null rate test calculation failed for {col_name_display}: {str(e)}")
            return TestResult(
                test_name='null_rate',
                column=col_name_display,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def _test_numerical_column(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        dest_conn: duckdb.DuckDBPyConnection,
        col_name_lower: str,
        col_name_display: str
    ) -> List[TestResult]:
        """Run numerical tests (KS-test, T-test)."""
        results = []

        try:
            # Fetch data using lowercase column name from respective caches
            src_data = source_conn.execute(
                f'SELECT "{col_name_lower}" FROM cached_source WHERE "{col_name_lower}" IS NOT NULL'
            ).fetchnumpy()[col_name_lower]

            dst_data = dest_conn.execute(
                f'SELECT "{col_name_lower}" FROM cached_dest WHERE "{col_name_lower}" IS NOT NULL'
            ).fetchnumpy()[col_name_lower]
            
            # KS-test
            ks_result = self.statistical_tests.ks_test(src_data, dst_data, col_name_display)
            results.append(ks_result)
            print(f"    KS-Test: {ks_result.status} (p={ks_result.details.get('p_value', 0):.4f})")
            
            # T-test
            t_result = self.statistical_tests.t_test(src_data, dst_data, col_name_display)
            results.append(t_result)
            print(f"    T-Test: {t_result.status} (p={t_result.details.get('p_value', 0):.4f})")
            
        except Exception as e:
            logger.error(f"Numerical tests failed for {col_name_display}: {str(e)}")
            print(f"    ERROR: {str(e)}")
        
        return results
    
    def _test_categorical_column(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        dest_conn: duckdb.DuckDBPyConnection,
        col_name_lower: str,
        col_name_display: str
    ) -> List[TestResult]:
        """Run categorical tests (PSI, Chi-square)."""
        results = []

        try:
            # Get cardinality from cached data
            cardinality = source_conn.execute(
                f'SELECT COUNT(DISTINCT "{col_name_lower}") FROM cached_source'
            ).fetchone()[0]

            if cardinality > self.max_cardinality_psi:
                print(f"    Skipped (high cardinality: {cardinality})")
                return results

            # Get distributions from cached data
            src_dist = source_conn.execute(f'''
                SELECT "{col_name_lower}" as value, COUNT(*) as cnt
                FROM cached_source
                WHERE "{col_name_lower}" IS NOT NULL
                GROUP BY "{col_name_lower}"
            ''').fetchdf()

            dst_dist = dest_conn.execute(f'''
                SELECT "{col_name_lower}" as value, COUNT(*) as cnt
                FROM cached_dest
                WHERE "{col_name_lower}" IS NOT NULL
                GROUP BY "{col_name_lower}"
            ''').fetchdf()
            
            # PSI test
            psi_result = self.statistical_tests.psi_test(src_dist, dst_dist, col_name_display)
            results.append(psi_result)
            if 'psi_value' in psi_result.details:
                print(f"    PSI: {psi_result.status} (psi={psi_result.details['psi_value']:.4f})")
            else:
                print(f"    PSI: {psi_result.status}")
            
            # Chi-square test (if cardinality is reasonable)
            if cardinality <= self.max_cardinality_chi_square:
                chi_result = self.statistical_tests.chi_square_test(src_dist, dst_dist, col_name_display)
                results.append(chi_result)
                print(f"    Chi-Square: {chi_result.status} (p={chi_result.details.get('p_value', 0):.4f})")
            
        except Exception as e:
            logger.error(f"Categorical tests failed for {col_name_display}: {str(e)}")
            print(f"    ERROR: {str(e)}")
        
        return results
    
    def _test_temporal_column(
        self,
        source_conn: duckdb.DuckDBPyConnection,
        dest_conn: duckdb.DuckDBPyConnection,
        col_name_lower: str,
        col_name_display: str
    ) -> List[TestResult]:
        """Run temporal tests (date range)."""
        results = []

        try:
            # Fetch date data from respective caches
            src_data = source_conn.execute(
                f'SELECT "{col_name_lower}" FROM cached_source WHERE "{col_name_lower}" IS NOT NULL'
            ).fetchdf()[col_name_lower]

            dst_data = dest_conn.execute(
                f'SELECT "{col_name_lower}" FROM cached_dest WHERE "{col_name_lower}" IS NOT NULL'
            ).fetchdf()[col_name_lower]
            
            # Date range test
            range_result = self.statistical_tests.date_range_test(
                src_data.values, dst_data.values, col_name_display
            )
            results.append(range_result)
            if range_result.status != 'ERROR':
                print(f"    Date Range: {range_result.status} "
                      f"({range_result.details.get('source_span_days', 0)} days span)")
            else:
                print(f"    Date Range: {range_result.status}")
            
        except Exception as e:
            logger.error(f"Temporal tests failed for {col_name_display}: {str(e)}")
            print(f"    Temporal tests skipped - {str(e)}")
        
        return results

    def _apply_fdr_correction(self, tests: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply False Discovery Rate (FDR) correction to tests with p-values.

        Uses the Benjamini-Hochberg procedure to control the false discovery rate
        across multiple hypothesis tests. This reduces false positives when testing
        many columns simultaneously.

        Example: When testing 100 columns at α=0.05, without correction you'd expect
        ~5 false positives. FDR correction reduces this while maintaining good power
        to detect real differences.

        Args:
            tests: List of test result dictionaries

        Returns:
            Updated list with corrected test statuses and FDR metadata
        """
        fdr_config = self.config.get('thresholds', {}).get('fdr_correction', {})

        if not fdr_config.get('enabled', False):
            return tests  # FDR correction disabled

        method = fdr_config.get('method', 'bh')  # 'bh' or 'by'
        alpha = fdr_config.get('alpha', 0.05)
        apply_per_test_type = fdr_config.get('apply_per_test_type', True)

        # Filter to only tests with p-values (excludes null_rate, psi, date_range, etc.)
        tests_with_pvalues = [
            (i, test) for i, test in enumerate(tests)
            if 'p_value' in test.get('details', {}) and test.get('status') not in ['SKIP', 'ERROR']
        ]

        if len(tests_with_pvalues) <= 1:
            logger.info("FDR correction skipped: only 0-1 tests with p-values")
            return tests  # No correction needed for single test

        logger.info(f"Applying FDR correction ({method}) to {len(tests_with_pvalues)} tests with p-values (α={alpha})")

        # Group tests by test type if configured
        if apply_per_test_type:
            test_groups = {}
            for idx, test in tests_with_pvalues:
                test_name = test.get('test_name', 'unknown')
                if test_name not in test_groups:
                    test_groups[test_name] = []
                test_groups[test_name].append((idx, test))
        else:
            # Single group: all tests together
            test_groups = {'all': tests_with_pvalues}

        # Apply FDR correction to each group
        corrections_made = 0
        for group_name, group_tests in test_groups.items():
            if len(group_tests) <= 1:
                continue  # Skip groups with only 1 test

            # Extract p-values and indices
            indices = [idx for idx, _ in group_tests]
            p_values = [test.get('details', {}).get('p_value', 1.0) for _, test in group_tests]

            # Apply FDR correction
            # IMPORTANT: FDR only applies to tests that originally FAILED (p < alpha)
            # We want to correct for false positives among failures, not re-evaluate passes
            try:
                # Only apply FDR to p-values from tests that originally failed
                failed_indices = []
                failed_pvalues = []

                for i, (test_idx, test) in enumerate(group_tests):
                    # For KS-test, T-test, Chi-square: low p-value = FAIL (reject null)
                    # We only apply FDR to those that failed (p < alpha)
                    if p_values[i] < alpha:
                        failed_indices.append(i)
                        failed_pvalues.append(p_values[i])

                # If no tests failed, no FDR correction needed
                if not failed_pvalues:
                    logger.debug(f"Group {group_name}: No failed tests, skipping FDR")
                    # Mark all as FDR-checked but unchanged
                    for test_idx, test in group_tests:
                        tests[test_idx]['details']['fdr_corrected'] = True
                        tests[test_idx]['details']['fdr_method'] = method
                        tests[test_idx]['details']['fdr_alpha'] = alpha
                    continue

                # Apply FDR correction only to failed tests
                reject = false_discovery_control(failed_pvalues, method=method)

            except Exception as e:
                logger.warning(f"FDR correction failed for group {group_name}: {str(e)}")
                continue

            # Update test statuses based on FDR results
            # Tests that originally passed remain passed
            # Tests that originally failed may be corrected to pass
            fdr_result_idx = 0
            for i, (test_idx, test) in enumerate(group_tests):
                original_status = test.get('status')

                # If this test originally failed, check FDR result
                if i in failed_indices:
                    should_reject = reject[fdr_result_idx]
                    fdr_result_idx += 1

                    # FDR says: should we reject null hypothesis?
                    # reject[i] = True → FAIL (distributions differ)
                    # reject[i] = False → PASS (FDR correction says this was likely false positive)
                    new_status = 'FAIL' if should_reject else 'PASS'
                else:
                    # Test originally passed (p >= alpha), keep it as PASS
                    new_status = 'PASS'

                if original_status != new_status:
                    tests[test_idx]['status'] = new_status
                    tests[test_idx]['details']['fdr_corrected'] = True
                    tests[test_idx]['details']['fdr_original_status'] = original_status
                    tests[test_idx]['details']['fdr_method'] = method
                    tests[test_idx]['details']['fdr_alpha'] = alpha
                    corrections_made += 1
                    logger.debug(f"FDR changed {test.get('column', 'unknown')} {test.get('test_name')}: {original_status} → {new_status}")
                else:
                    # Status unchanged, but mark that FDR was applied
                    tests[test_idx]['details']['fdr_corrected'] = True
                    tests[test_idx]['details']['fdr_method'] = method
                    tests[test_idx]['details']['fdr_alpha'] = alpha

        if corrections_made > 0:
            logger.info(f"FDR correction changed status of {corrections_made} test(s)")
        else:
            logger.info("FDR correction applied but no status changes")

        return tests

    def _finalize_result(self, result: Dict[str, Any]):
        """Finalize comparison result with summary."""
        all_tests = result['tests']

        # Apply FDR correction if enabled
        all_tests = self._apply_fdr_correction(all_tests)
        result['tests'] = all_tests
        failed_tests = [t for t in all_tests if t['status'] == 'FAIL']
        warning_tests = [t for t in all_tests if t['status'] == 'WARNING']
        passed_tests = [t for t in all_tests if t['status'] == 'PASS']
        
        result['summary'] = {
            'total_tests': len(all_tests),
            'passed': len(passed_tests),
            'warnings': len(warning_tests),
            'failed': len(failed_tests),
            'skipped': len([t for t in all_tests if t['status'] == 'SKIP']),
            'errors': len([t for t in all_tests if t['status'] == 'ERROR'])
        }
        
        # Check if row_count test failed (critical test)
        row_count_test = next((t for t in all_tests if t['test_name'] == 'row_count'), None)
        if row_count_test and row_count_test['status'] == 'FAIL':
            result['overall_status'] = 'FAIL'
            logger.info(f"Comparison complete: {result['overall_status']} (row count failed)")
            return
        
        # Use majority-based status (if row_count passed or doesn't exist)
        total_tests = len(all_tests)
        pass_rate = len(passed_tests) / total_tests * 100 if total_tests > 0 else 0
        fail_rate = len(failed_tests) / total_tests * 100 if total_tests > 0 else 0
        
        # Majority logic
        if pass_rate > 50:  # More than 50% passed
            result['overall_status'] = 'PASS'
        elif fail_rate > 50:  # More than 50% failed
            result['overall_status'] = 'FAIL'
        elif warning_tests:  # Tie or close call with warnings
            result['overall_status'] = 'WARNING'
        else:  # Tie without warnings - lean towards PASS
            result['overall_status'] = 'PASS'
        
        logger.info(f"Comparison complete: {result['overall_status']} (pass rate: {pass_rate:.1f}%)")
    
    def _print_summary(self, result: Dict[str, Any]):
        """Print comparison summary."""
        print(f"\n{'='*60}")
        print(f"RESULT: {result['overall_status']}")
        print(f"Passed: {result['summary']['passed']}/{result['summary']['total_tests']}")

        if result['summary']['warnings'] > 0:
            print(f"Warnings: {result['summary']['warnings']}")

        # Check if FDR correction was applied
        fdr_corrected_tests = [t for t in result['tests'] if t.get('details', {}).get('fdr_corrected', False)]
        if fdr_corrected_tests:
            fdr_changed = [t for t in fdr_corrected_tests if 'fdr_original_status' in t.get('details', {})]
            fdr_config = self.config.get('thresholds', {}).get('fdr_correction', {})
            method = fdr_config.get('method', 'bh')
            alpha = fdr_config.get('alpha', 0.05)
            print(f"\n⚠️  FDR correction applied ({method.upper()}, α={alpha})")
            print(f"   - Corrected {len(fdr_corrected_tests)} test(s) with p-values")
            if fdr_changed:
                print(f"   - Status changed for {len(fdr_changed)} test(s)")

        if result['summary']['failed'] > 0:
            failed = [t for t in result['tests'] if t['status'] == 'FAIL']
            print(f"\nFailed tests:")
            for t in failed:
                col_info = f" on {t.get('column')}" if t.get('column') else ""
                fdr_note = " (FDR-corrected)" if t.get('details', {}).get('fdr_corrected') else ""
                print(f"  - {t['test_name']}{col_info}{fdr_note}")

        print(f"{'='*60}\n")