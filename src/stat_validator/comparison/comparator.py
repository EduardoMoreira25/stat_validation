"""Main table comparison engine with statistical validation."""

import duckdb
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from ..connectors.base_connector import BaseConnector
from ..connectors.hana_connector import HanaConnector
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
        self.max_cardinality_psi = categorical_config.get('max_cardinality_for_psi', 100)
        self.max_cardinality_chi_square = categorical_config.get('max_cardinality_for_chi_square', 50)
    
    def compare(
        self,
        source_table: str,
        dest_table: str,
        columns_to_test: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Main comparison function.
        
        Args:
            source_table: Fully qualified source table name
            dest_table: Fully qualified destination table name
            columns_to_test: Optional list of specific columns to test
            
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
        
        row_test = self._test_row_count(source_table, dest_table)
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
        self._cache_tables(source_table, dest_table, cols_to_cache)
        
        # Phase 3: Column-level statistical tests
        logger.info("Phase 3: Statistical tests on columns")
        print("\n[Phase 3] Statistical Tests on Columns...")
        
        column_tests = self._test_columns(source_table, dest_table, cols_to_cache)
        result['tests'].extend([test.to_dict() for test in column_tests])
        
        # Finalize results
        self._finalize_result(result)
        self._print_summary(result)
        
        return result
    
    def _test_row_count(self, source_table: str, dest_table: str) -> TestResult:
        """Test row count validation using ratio-based comparison."""
        logger.debug(f"Testing row count for {source_table} vs {dest_table}")
        
        try:
            source_count = self.source_connector.get_row_count(source_table)
            dest_count = self.dest_connector.get_row_count(dest_table)
            
            diff = dest_count - source_count
            diff_pct = abs(diff / source_count * 100) if source_count > 0 else 100
            
            # Calculate ratio (dest / source)
            ratio = dest_count / source_count if source_count > 0 else 0
            
            # Threshold as ratio (e.g., 0.1% tolerance = 0.999 ratio)
            threshold_ratio = 1.0 - (self.row_count_threshold_pct / 100)
            
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
                    'ratio': round(ratio, 6),
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
    
    def _cache_tables(
        self,
        source_table: str,
        dest_table: str,
        columns: Optional[List[str]] = None
    ):
        """Cache source and destination tables to DuckDB."""
        logger.info("Caching source and destination tables")
        
        # Get schemas to map column names correctly
        source_schema = self.source_connector.get_table_schema(source_table)
        dest_schema = self.dest_connector.get_table_schema(dest_table)
        
        # Filter out binary columns (they cause encoding issues)
        binary_types = ['binary', 'binary[pyarrow]', 'large_binary', 'fixed_size_binary']
        
        source_cacheable = [
            field.name for field in source_schema 
            if str(field.type) not in binary_types
        ]
        
        dest_cacheable = [
            field.name for field in dest_schema 
            if str(field.type) not in binary_types
        ]
        
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
            
            source_col_list = ', '.join([f'"{col}"' for col in source_cols])
            dest_col_list = ', '.join([f'"{col}"' for col in dest_cols])
        else:
            # Select all non-binary columns
            source_col_list = ', '.join([f'"{col}"' for col in source_cacheable])
            dest_col_list = ', '.join([f'"{col}"' for col in dest_cacheable])
        
        # Build queries with sampling (different SQL dialects)
        if self.sampling_enabled:
            # HANA uses RAND(), Dremio uses random()
            if isinstance(self.source_connector, HanaConnector):
                source_query = f"SELECT {source_col_list} FROM {source_table} ORDER BY RAND() LIMIT {self.sample_size}"
            else:
                source_query = f"SELECT {source_col_list} FROM {source_table} ORDER BY random() LIMIT {self.sample_size}"
            
            if isinstance(self.dest_connector, HanaConnector):
                dest_query = f"SELECT {dest_col_list} FROM {dest_table} ORDER BY RAND() LIMIT {self.sample_size}"
            else:
                dest_query = f"SELECT {dest_col_list} FROM {dest_table} ORDER BY random() LIMIT {self.sample_size}"
        else:
            source_query = f"SELECT {source_col_list} FROM {source_table}"
            dest_query = f"SELECT {dest_col_list} FROM {dest_table}"
        
        print(f"  Caching source table (sample: {self.sampling_enabled})...")
        self.source_connector.cache_query(source_query, "cached_source")
        
        print(f"  Caching destination table (sample: {self.sampling_enabled})...")
        self.dest_connector.cache_query(dest_query, "cached_dest")
        
        logger.info("Tables cached successfully")
    
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
        
        # Get DuckDB connection for cached data - use source connector's cache
        conn = self.source_connector.get_cache_connection()
        
        # Determine which columns to test
        if columns_to_test:
            # User specified or common columns from schema mismatch
            all_columns = columns_to_test
        else:
            # All columns from source
            all_columns = [field.name for field in source_schema]
        
        for idx, col_name in enumerate(all_columns, 1):
            print(f"\n  Column [{idx}/{len(all_columns)}]: {col_name}")
            logger.debug(f"Testing column: {col_name}")
            
            # Use lowercase for cached data
            col_name_lower = col_name.lower()
            
            # Null rate test (always)
            null_test = self._test_null_rate(conn, col_name_lower, col_name)
            results.append(null_test)
            print(f"    Null Rate: {null_test.status} "
                  f"(src={null_test.details.get('source_null_pct', 0):.1f}%, "
                  f"dst={null_test.details.get('dest_null_pct', 0):.1f}%)")
            
            # Type-specific tests
            if col_name in column_classification['numerical']:
                results.extend(self._test_numerical_column(conn, col_name_lower, col_name))
            elif col_name in column_classification['categorical']:
                results.extend(self._test_categorical_column(conn, col_name_lower, col_name))
            elif col_name in column_classification['temporal']:
                results.extend(self._test_temporal_column(conn, col_name_lower, col_name))
            else:
                print(f"    Unsupported type - skipped")
        
        conn.close()
        return results
    
    def _test_null_rate(self, conn: duckdb.DuckDBPyConnection, col_name_lower: str, col_name_display: str) -> TestResult:
        """Test null rate comparison on FULL TABLE."""
        try:
            # Query FULL TABLES for accurate null counts
            src_total = self.source_connector.get_row_count(self.source_table_name)
            dst_total = self.dest_connector.get_row_count(self.dest_table_name)
            
            # Get null counts from full tables
            src_null_query = f'SELECT COUNT(*) FROM {self.source_table_name} WHERE "{col_name_display}" IS NULL'
            dst_null_query = f'SELECT COUNT(*) FROM {self.dest_table_name} WHERE "{col_name_display}" IS NULL'
            
            src_nulls = self.source_connector.execute_query(src_null_query).to_pandas().iloc[0, 0]
            dst_nulls = self.dest_connector.execute_query(dst_null_query).to_pandas().iloc[0, 0]
            
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
            logger.error(f"Full table null rate test failed for {col_name_display}: {str(e)}")
            return TestResult(
                test_name='null_rate',
                column=col_name_display,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def _test_numerical_column(
        self,
        conn: duckdb.DuckDBPyConnection,
        col_name_lower: str,
        col_name_display: str
    ) -> List[TestResult]:
        """Run numerical tests (KS-test, T-test)."""
        results = []
        
        try:
            # Fetch data using lowercase column name
            src_data = conn.execute(
                f'SELECT "{col_name_lower}" FROM cached_source WHERE "{col_name_lower}" IS NOT NULL'
            ).fetchnumpy()[col_name_lower]
            
            dst_data = conn.execute(
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
        conn: duckdb.DuckDBPyConnection,
        col_name_lower: str,
        col_name_display: str
    ) -> List[TestResult]:
        """Run categorical tests (PSI, Chi-square)."""
        results = []
        
        try:
            # Get cardinality from cached data
            cardinality = conn.execute(
                f'SELECT COUNT(DISTINCT "{col_name_lower}") FROM cached_source'
            ).fetchone()[0]
            
            if cardinality > self.max_cardinality_psi:
                print(f"    Skipped (high cardinality: {cardinality})")
                return results
            
            # Get distributions from cached data
            src_dist = conn.execute(f'''
                SELECT "{col_name_lower}" as value, COUNT(*) as cnt
                FROM cached_source
                WHERE "{col_name_lower}" IS NOT NULL
                GROUP BY "{col_name_lower}"
            ''').fetchdf()
            
            dst_dist = conn.execute(f'''
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
        conn: duckdb.DuckDBPyConnection,
        col_name_lower: str,
        col_name_display: str
    ) -> List[TestResult]:
        """Run temporal tests (date range)."""
        results = []
        
        try:
            # Fetch date data
            src_data = conn.execute(
                f'SELECT "{col_name_lower}" FROM cached_source WHERE "{col_name_lower}" IS NOT NULL'
            ).fetchdf()[col_name_lower]
            
            dst_data = conn.execute(
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
    
    def _finalize_result(self, result: Dict[str, Any]):
        """Finalize comparison result with summary."""
        all_tests = result['tests']
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
        
        if result['summary']['failed'] > 0:
            failed = [t for t in result['tests'] if t['status'] == 'FAIL']
            print(f"\nFailed tests:")
            for t in failed:
                col_info = f" on {t.get('column')}" if t.get('column') else ""
                print(f"  - {t['test_name']}{col_info}")
        
        print(f"{'='*60}\n")