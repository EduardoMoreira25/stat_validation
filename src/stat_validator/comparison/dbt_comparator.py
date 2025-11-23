"""
DBT-Aware Comparator Module

Compares SAP source tables (with DBT filters applied) against Dremio refined tables.
"""

import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from datetime import datetime
import pyarrow as pa

from ..connectors.dremio_connector import DremioConnector
from ..connectors.hana_connector import HanaConnector
from ..parsers.dbt_sql_parser import DBTSQLParser, ParsedDBTSQL


logger = logging.getLogger(__name__)


@dataclass
class ColumnStats:
    """Statistics for a single column."""
    column_name: str
    null_count: int
    total_count: int
    null_percentage: float


@dataclass
class TableStats:
    """Statistics for a table."""
    row_count: int
    min_refresh_dt: Optional[datetime]
    max_refresh_dt: Optional[datetime]
    column_stats: List[ColumnStats]


@dataclass
class ComparisonResult:
    """Result of comparing SAP vs Dremio tables."""
    table_name: str
    year: int
    month: int
    sap_stats: TableStats
    dremio_stats: TableStats
    row_count_match: bool
    row_count_diff: int
    row_count_diff_pct: float
    refresh_dt_match: bool
    null_comparison: Dict[str, Dict[str, Any]]  # column -> {sap_null_pct, dremio_null_pct, match}
    overall_status: str  # 'PASS' or 'FAIL'
    issues: List[str]  # List of validation issues


class DBTComparator:
    """Comparator for SAP source tables with DBT filters vs Dremio refined tables."""

    def __init__(
        self,
        dremio_connector: DremioConnector,
        sap_connector: HanaConnector,
        dbt_parser: Optional[DBTSQLParser] = None,
        row_count_tolerance: float = 0.001  # 0.1% tolerance
    ):
        """
        Initialize the DBT comparator.

        Args:
            dremio_connector: Connector to Dremio
            sap_connector: Connector to SAP HANA
            dbt_parser: Optional DBT SQL parser (will create default if not provided)
            row_count_tolerance: Tolerance for row count differences (default 0.1%)
        """
        self.dremio_connector = dremio_connector
        self.sap_connector = sap_connector
        self.dbt_parser = dbt_parser or DBTSQLParser()
        self.row_count_tolerance = row_count_tolerance

    def compare_table(
        self,
        dremio_schema: str,
        dremio_table: str,
        sap_schema: str,
        sap_table: str,
        year: int,
        month: int
    ) -> ComparisonResult:
        """
        Compare a SAP table (with DBT filters) against a Dremio refined table.

        Args:
            dremio_schema: Dremio schema (e.g., 'sapisu')
            dremio_table: Dremio refined table name (e.g., 'rfn_but000')
            sap_schema: SAP schema (e.g., 'SAP_RISE_1')
            sap_table: SAP table name (e.g., 'T_RISE_BUT000')
            year: Year filter
            month: Month filter

        Returns:
            ComparisonResult object
        """
        logger.info(f"Comparing {dremio_table} for {year}-{month:02d}")

        # Parse DBT SQL to get filters
        parsed_dbt = self.dbt_parser.parse_file(dremio_table)

        # Build queries
        dremio_query = self._build_dremio_query(dremio_schema, dremio_table, year, month)
        sap_query = self._build_sap_query(sap_schema, sap_table, parsed_dbt, year, month)

        logger.debug(f"Dremio query: {dremio_query}")
        logger.debug(f"SAP query: {sap_query}")

        # Execute queries
        dremio_result = self.dremio_connector.execute_query(dremio_query)
        sap_result = self.sap_connector.execute_query(sap_query)

        # Parse statistics
        dremio_stats = self._parse_stats_result(dremio_result)
        sap_stats = self._parse_stats_result(sap_result)

        # Compare results
        return self._compare_stats(dremio_table, year, month, sap_stats, dremio_stats)

    def _build_dremio_query(
        self,
        schema: str,
        table: str,
        year: int,
        month: int
    ) -> str:
        """Build the statistics query for Dremio refined table."""
        # First, get column list
        column_query = f'SELECT * FROM ulysses.{schema}."{table}" LIMIT 1'
        sample = self.dremio_connector.execute_query(column_query)
        columns = sample.schema.names

        # Exclude system columns from null analysis
        exclude_columns = {'sync_ts', 'system_ts', 'ulysses_ts'}
        data_columns = [col for col in columns if col not in exclude_columns]

        # Build null count expressions for each column
        null_counts = []
        for col in data_columns:
            # Replace special characters in alias name to avoid SQL syntax errors
            safe_alias = col.replace('/', '_').replace('-', '_').replace(' ', '_')
            null_counts.append(
                f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "null_{safe_alias}"'
            )

        query = f"""
        SELECT
            COUNT(*) AS row_count,
            MIN(system_ts) AS min_refresh_dt,
            MAX(system_ts) AS max_refresh_dt,
            {', '.join(null_counts)}
        FROM ulysses.{schema}."{table}"
        WHERE EXTRACT(YEAR FROM system_ts) = {year}
          AND EXTRACT(MONTH FROM system_ts) = {month}
        """

        return query

    def _build_sap_query(
        self,
        schema: str,
        table: str,
        parsed_dbt: ParsedDBTSQL,
        year: int,
        month: int
    ) -> str:
        """Build the statistics query for SAP source table with DBT filters."""
        # Get filter components
        filters = self.dbt_parser.build_sap_query_filters(parsed_dbt)

        # First, get column list from main table
        column_query = f'SELECT TOP 1 * FROM "{schema}"."{table}"'
        sample = self.sap_connector.execute_query(column_query)
        columns = sample.schema.names

        # Exclude system columns from null analysis
        exclude_columns = {'SYNC_TS', 'SYSTEM_TS', 'ULYSSES_TS', 'REFRESH_DT', 'EIM_CHANGE_STATUS', 'D_EXTRACT'}
        data_columns = [col for col in columns if col not in exclude_columns]

        # Build null count expressions for each column (reference main table alias if it exists)
        main_alias = parsed_dbt.main_alias
        col_prefix = f"{main_alias}." if main_alias else ""

        null_counts = []
        for col in data_columns:
            # Replace special characters in alias name to avoid SQL syntax errors
            safe_alias = col.replace('/', '_').replace('-', '_').replace(' ', '_')
            null_counts.append(
                f'SUM(CASE WHEN {col_prefix}"{col}" IS NULL THEN 1 ELSE 0 END) AS "null_{safe_alias}"'
            )

        # Build the query
        query = f"""
        SELECT
            COUNT(*) AS row_count,
            MIN({col_prefix}REFRESH_DT) AS min_refresh_dt,
            MAX({col_prefix}REFRESH_DT) AS max_refresh_dt,
            {', '.join(null_counts)}
        {filters['from_clause']}
        """

        # Add JOIN clause if exists
        if filters['join_clause']:
            query += f"\n{filters['join_clause']}"

        # Add WHERE clause (includes EIM_CHANGE_STATUS filter + DBT filters + date filter)
        query += f"""
        WHERE {filters['where_clause']}
          AND YEAR({col_prefix}REFRESH_DT) = {year}
          AND MONTH({col_prefix}REFRESH_DT) = {month}
        """

        return query

    def _parse_stats_result(self, result: pa.Table) -> TableStats:
        """Parse the statistics result from a query."""
        if len(result) == 0:
            return TableStats(
                row_count=0,
                min_refresh_dt=None,
                max_refresh_dt=None,
                column_stats=[]
            )

        row = result.to_pydict()

        # Create case-insensitive column name mapping
        col_mapping = {k.lower(): k for k in row.keys()}

        # Extract basic stats (handle case-insensitive keys)
        row_count_key = col_mapping.get('row_count', 'row_count')
        min_key = col_mapping.get('min_refresh_dt', 'min_refresh_dt')
        max_key = col_mapping.get('max_refresh_dt', 'max_refresh_dt')

        row_count = int(row[row_count_key][0]) if row[row_count_key][0] is not None else 0

        # Parse dates to datetime objects for proper comparison
        min_refresh_dt = None
        if row[min_key][0] is not None:
            min_val = row[min_key][0]
            if isinstance(min_val, datetime):
                min_refresh_dt = min_val
            else:
                # Parse string to datetime (handle various formats)
                try:
                    min_refresh_dt = datetime.fromisoformat(str(min_val).replace(' ', 'T').split('.')[0])
                except:
                    min_refresh_dt = None

        max_refresh_dt = None
        if row[max_key][0] is not None:
            max_val = row[max_key][0]
            if isinstance(max_val, datetime):
                max_refresh_dt = max_val
            else:
                # Parse string to datetime (handle various formats)
                try:
                    max_refresh_dt = datetime.fromisoformat(str(max_val).replace(' ', 'T').split('.')[0])
                except:
                    max_refresh_dt = None

        # Extract column null statistics
        column_stats = []
        for col_name in row.keys():
            if col_name.lower().startswith('null_'):
                actual_col_name = col_name[5:].lower()  # Remove 'null_' prefix and normalize to lowercase
                null_count = int(row[col_name][0]) if row[col_name][0] is not None else 0
                null_pct = (null_count / row_count * 100) if row_count > 0 else 0.0

                column_stats.append(ColumnStats(
                    column_name=actual_col_name,
                    null_count=null_count,
                    total_count=row_count,
                    null_percentage=null_pct
                ))

        return TableStats(
            row_count=row_count,
            min_refresh_dt=min_refresh_dt,
            max_refresh_dt=max_refresh_dt,
            column_stats=column_stats
        )

    def _compare_stats(
        self,
        table_name: str,
        year: int,
        month: int,
        sap_stats: TableStats,
        dremio_stats: TableStats
    ) -> ComparisonResult:
        """Compare SAP and Dremio statistics and generate result."""
        issues = []

        # Compare row counts
        row_count_diff = abs(sap_stats.row_count - dremio_stats.row_count)
        if sap_stats.row_count > 0:
            row_count_diff_pct = (row_count_diff / sap_stats.row_count) * 100
        else:
            row_count_diff_pct = 0.0 if dremio_stats.row_count == 0 else 100.0

        row_count_match = row_count_diff_pct <= (self.row_count_tolerance * 100)

        if not row_count_match:
            issues.append(
                f"Row count mismatch: SAP={sap_stats.row_count}, "
                f"Dremio={dremio_stats.row_count}, diff={row_count_diff_pct:.2f}%"
            )

        # Compare refresh_dt ranges (compare dates ignoring microseconds)
        refresh_dt_match = True
        if sap_stats.min_refresh_dt and dremio_stats.min_refresh_dt:
            # Truncate to seconds for comparison (ignore microseconds)
            sap_min = sap_stats.min_refresh_dt.replace(microsecond=0)
            dremio_min = dremio_stats.min_refresh_dt.replace(microsecond=0)
            sap_max = sap_stats.max_refresh_dt.replace(microsecond=0) if sap_stats.max_refresh_dt else None
            dremio_max = dremio_stats.max_refresh_dt.replace(microsecond=0) if dremio_stats.max_refresh_dt else None

            refresh_dt_match = (sap_min == dremio_min and sap_max == dremio_max)
        else:
            # If either is None, consider it a mismatch
            refresh_dt_match = (sap_stats.min_refresh_dt == dremio_stats.min_refresh_dt == None)

        if not refresh_dt_match:
            issues.append(
                f"Refresh date range mismatch: "
                f"SAP=[{sap_stats.min_refresh_dt} to {sap_stats.max_refresh_dt}], "
                f"Dremio=[{dremio_stats.min_refresh_dt} to {dremio_stats.max_refresh_dt}]"
            )

        # Compare null percentages by column
        null_comparison = {}
        sap_nulls = {col.column_name: col for col in sap_stats.column_stats}
        dremio_nulls = {col.column_name: col for col in dremio_stats.column_stats}

        all_columns = set(sap_nulls.keys()) | set(dremio_nulls.keys())

        for col_name in sorted(all_columns):
            sap_col = sap_nulls.get(col_name)
            dremio_col = dremio_nulls.get(col_name)

            sap_null_pct = sap_col.null_percentage if sap_col else None
            dremio_null_pct = dremio_col.null_percentage if dremio_col else None

            # Consider match if difference is within 1%
            if sap_null_pct is not None and dremio_null_pct is not None:
                null_diff = abs(sap_null_pct - dremio_null_pct)
                match = null_diff <= 1.0
            else:
                match = False

            null_comparison[col_name] = {
                'sap_null_pct': sap_null_pct,
                'dremio_null_pct': dremio_null_pct,
                'match': match
            }

            if not match and sap_null_pct is not None and dremio_null_pct is not None:
                issues.append(
                    f"Null percentage mismatch for {col_name}: "
                    f"SAP={sap_null_pct:.2f}%, Dremio={dremio_null_pct:.2f}%"
                )

        # Determine overall status
        overall_status = 'PASS' if len(issues) == 0 else 'FAIL'

        return ComparisonResult(
            table_name=table_name,
            year=year,
            month=month,
            sap_stats=sap_stats,
            dremio_stats=dremio_stats,
            row_count_match=row_count_match,
            row_count_diff=row_count_diff,
            row_count_diff_pct=row_count_diff_pct,
            refresh_dt_match=refresh_dt_match,
            null_comparison=null_comparison,
            overall_status=overall_status,
            issues=issues
        )

    def to_dict(self, result: ComparisonResult) -> Dict[str, Any]:
        """Convert ComparisonResult to dictionary for JSON serialization."""
        # Helper function to convert datetime to ISO string
        def dt_to_str(dt):
            return dt.isoformat() if dt else None

        return {
            'table_name': result.table_name,
            'year': result.year,
            'month': result.month,
            'overall_status': result.overall_status,
            'row_count_match': result.row_count_match,
            'row_count_diff': result.row_count_diff,
            'row_count_diff_pct': round(result.row_count_diff_pct, 2),
            'refresh_dt_match': result.refresh_dt_match,
            'sap_stats': {
                'row_count': result.sap_stats.row_count,
                'min_refresh_dt': dt_to_str(result.sap_stats.min_refresh_dt),
                'max_refresh_dt': dt_to_str(result.sap_stats.max_refresh_dt),
                'column_null_stats': [asdict(col) for col in result.sap_stats.column_stats]
            },
            'dremio_stats': {
                'row_count': result.dremio_stats.row_count,
                'min_refresh_dt': dt_to_str(result.dremio_stats.min_refresh_dt),
                'max_refresh_dt': dt_to_str(result.dremio_stats.max_refresh_dt),
                'column_null_stats': [asdict(col) for col in result.dremio_stats.column_stats]
            },
            'null_comparison': result.null_comparison,
            'issues': result.issues
        }
