"""Main table comparison engine with statistical validation."""

import duckdb
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Optional
from ..connectors.base_connector import BaseConnector
from ..utils.logger import get_logger
from .statistical_tests import StatisticalTests, TestResult
from .schema_validator import SchemaValidator


logger = get_logger('comparator')


class TableComparator:
    """
    Compares source and destination tables using statistical tests.
    
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
        connector: BaseConnector,
        config: Dict[str, Any]
    ):
        """
        Initialize table comparator.
        
        Args:
            connector: Data source connector
            config: Configuration dictionary from ConfigLoader
        """
        self.connector = connector
        self.config = config
        
        # Initialize validators
        thresholds = config.get('thresholds', {})
        sampling_config = config.get('sampling', {})
        categorical_config = config.get('categorical', {})
        
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
            
            # Get common columns
            source_schema = self.connector.get_table_schema(source_table)
            dest_schema = self.connector.get_table_schema(dest_table)
            source_cols = set([field.name for field in source_schema])
            dest_cols = set([field.name for field in dest_schema])
            common_column_names = list(source_cols & dest_cols)
            
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
        """Test row count validation."""
        logger.debug(f"Testing row count for {source_table} vs {dest_table}")
        
        try:
            source_count = self.connector.get_row_count(source_table)
            dest_count = self.connector.get_row_count(dest_table)
            
            diff = dest_count - source_count
            diff_pct = abs(diff / source_count * 100) if source_count > 0 else 100
            
            status = 'PASS' if diff_pct <= self.row_count_threshold_pct else 'FAIL'
            
            return TestResult(
                test_name='row_count',
                column=None,
                status=status,
                details={
                    'source_count': source_count,
                    'dest_count': dest_count,
                    'difference': diff,
                    'difference_pct': round(diff_pct, 3),
                    'threshold_pct': self.row_count_threshold_pct
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
            source_schema = self.connector.get_table_schema(source_table)
            dest_schema = self.connector.get_table_schema(dest_table)
            
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
        
        # Build query with optional column filter and sampling
        col_list = ', '.join([f'"{col}"' for col in columns]) if columns else '*'
        
        if self.sampling_enabled:
            source_query = f"SELECT {col_list} FROM {source_table} ORDER BY random() LIMIT {self.sample_size}"
            dest_query = f"SELECT {col_list} FROM {dest_table} ORDER BY random() LIMIT {self.sample_size}"
        else:
            source_query = f"SELECT {col_list} FROM {source_table}"
            dest_query = f"SELECT {col_list} FROM {dest_table}"
        
        print(f"  Caching source table (sample: {self.sampling_enabled})...")
        self.connector.cache_query(source_query, "cached_source")
        
        print(f"  Caching destination table (sample: {self.sampling_enabled})...")
        self.connector.cache_query(dest_query, "cached_dest")
        
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
        source_schema = self.connector.get_table_schema(source_table)
        column_classification = self.schema_validator.classify_columns(source_schema)
        
        # Get DuckDB connection for cached data
        conn = self.connector.get_cache_connection()
        
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
            
            # Null rate test (always)
            null_test = self._test_null_rate(conn, col_name)
            results.append(null_test)
            print(f"    Null Rate: {null_test.status} "
                  f"(src={null_test.details.get('source_null_pct', 0):.1f}%, "
                  f"dst={null_test.details.get('dest_null_pct', 0):.1f}%)")
            
            # Type-specific tests
            if col_name in column_classification['numerical']:
                results.extend(self._test_numerical_column(conn, col_name))
            elif col_name in column_classification['categorical']:
                results.extend(self._test_categorical_column(conn, col_name))
            elif col_name in column_classification['temporal']:
                results.extend(self._test_temporal_column(conn, col_name))
            else:
                print(f"    Unsupported type - skipped")
        
        conn.close()
        return results
    
    def _test_null_rate(self, conn: duckdb.DuckDBPyConnection, col_name: str) -> TestResult:
        """Test null rate comparison."""
        try:
            src_total = conn.execute("SELECT COUNT(*) FROM cached_source").fetchone()[0]
            dst_total = conn.execute("SELECT COUNT(*) FROM cached_dest").fetchone()[0]
            
            src_nulls = conn.execute(f"SELECT COUNT(*) FROM cached_source WHERE \"{col_name}\" IS NULL").fetchone()[0]
            dst_nulls = conn.execute(f"SELECT COUNT(*) FROM cached_dest WHERE \"{col_name}\" IS NULL").fetchone()[0]
            
            src_pct = (src_nulls / src_total * 100) if src_total > 0 else 0
            dst_pct = (dst_nulls / dst_total * 100) if dst_total > 0 else 0
            
            diff = abs(dst_pct - src_pct)
            status = 'PASS' if diff <= self.null_rate_threshold_pct else 'FAIL'
            
            return TestResult(
                test_name='null_rate',
                column=col_name,
                status=status,
                details={
                    'source_null_pct': round(src_pct, 2),
                    'dest_null_pct': round(dst_pct, 2),
                    'difference_pct': round(diff, 2),
                    'threshold_pct': self.null_rate_threshold_pct
                }
            )
        except Exception as e:
            return TestResult(
                test_name='null_rate',
                column=col_name,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def _test_numerical_column(
        self,
        conn: duckdb.DuckDBPyConnection,
        col_name: str
    ) -> List[TestResult]:
        """Run numerical tests (KS-test, T-test)."""
        results = []
        
        try:
            # Fetch data
            src_data = conn.execute(
                f'SELECT "{col_name}" FROM cached_source WHERE "{col_name}" IS NOT NULL'
            ).fetchnumpy()[col_name]
            
            dst_data = conn.execute(
                f'SELECT "{col_name}" FROM cached_dest WHERE "{col_name}" IS NOT NULL'
            ).fetchnumpy()[col_name]
            
            # KS-test
            ks_result = self.statistical_tests.ks_test(src_data, dst_data, col_name)
            results.append(ks_result)
            print(f"    KS-Test: {ks_result.status} (p={ks_result.details.get('p_value', 0):.4f})")
            
            # T-test
            t_result = self.statistical_tests.t_test(src_data, dst_data, col_name)
            results.append(t_result)
            print(f"    T-Test: {t_result.status} (p={t_result.details.get('p_value', 0):.4f})")
            
        except Exception as e:
            logger.error(f"Numerical tests failed for {col_name}: {str(e)}")
            print(f"    ERROR: {str(e)}")
        
        return results
    
    def _test_categorical_column(
        self,
        conn: duckdb.DuckDBPyConnection,
        col_name: str
    ) -> List[TestResult]:
        """Run categorical tests (PSI, Chi-square)."""
        results = []
        
        try:
            # Get cardinality
            cardinality = conn.execute(
                f'SELECT COUNT(DISTINCT "{col_name}") FROM cached_source'
            ).fetchone()[0]
            
            if cardinality > self.max_cardinality_psi:
                print(f"    Skipped (high cardinality: {cardinality})")
                return results
            
            # Get distributions
            src_dist = conn.execute(f'''
                SELECT "{col_name}" as value, COUNT(*) as cnt
                FROM cached_source
                WHERE "{col_name}" IS NOT NULL
                GROUP BY "{col_name}"
            ''').fetchdf()
            
            dst_dist = conn.execute(f'''
                SELECT "{col_name}" as value, COUNT(*) as cnt
                FROM cached_dest
                WHERE "{col_name}" IS NOT NULL
                GROUP BY "{col_name}"
            ''').fetchdf()
            
            # PSI test
            psi_result = self.statistical_tests.psi_test(src_dist, dst_dist, col_name)
            results.append(psi_result)
            if 'psi_value' in psi_result.details:
                print(f"    PSI: {psi_result.status} (psi={psi_result.details['psi_value']:.4f})")
            else:
                print(f"    PSI: {psi_result.status}")
            
            # Chi-square test (if cardinality is reasonable)
            if cardinality <= self.max_cardinality_chi_square:
                chi_result = self.statistical_tests.chi_square_test(src_dist, dst_dist, col_name)
                results.append(chi_result)
                print(f"    Chi-Square: {chi_result.status} (p={chi_result.details.get('p_value', 0):.4f})")
            
        except Exception as e:
            logger.error(f"Categorical tests failed for {col_name}: {str(e)}")
            print(f"    ERROR: {str(e)}")
        
        return results
    
    def _test_temporal_column(
        self,
        conn: duckdb.DuckDBPyConnection,
        col_name: str
    ) -> List[TestResult]:
        """Run temporal tests (date range)."""
        results = []
        
        try:
            # Fetch date data
            src_data = conn.execute(
                f'SELECT "{col_name}" FROM cached_source WHERE "{col_name}" IS NOT NULL'
            ).fetchdf()[col_name]
            
            dst_data = conn.execute(
                f'SELECT "{col_name}" FROM cached_dest WHERE "{col_name}" IS NOT NULL'
            ).fetchdf()[col_name]
            
            # Date range test
            range_result = self.statistical_tests.date_range_test(
                src_data.values, dst_data.values, col_name
            )
            results.append(range_result)
            if range_result.status != 'ERROR':
                print(f"    Date Range: {range_result.status} "
                      f"({range_result.details.get('source_span_days', 0)} days span)")
            else:
                print(f"    Date Range: {range_result.status}")
            
        except Exception as e:
            logger.error(f"Temporal tests failed for {col_name}: {str(e)}")
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
        
        if failed_tests:
            result['overall_status'] = 'FAIL'
        elif warning_tests:
            result['overall_status'] = 'WARNING'
        else:
            result['overall_status'] = 'PASS'
        
        logger.info(f"Comparison complete: {result['overall_status']}")
    
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