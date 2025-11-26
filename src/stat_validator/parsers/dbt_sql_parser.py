"""
DBT SQL Parser Module

Parses DBT SQL files to extract WHERE clauses, JOIN conditions, and source table references.
Converts DBT source references to actual SAP HANA table names.
"""

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class JoinInfo:
    """Information about a JOIN in the SQL query."""
    join_type: str  # 'JOIN', 'INNER JOIN', 'LEFT JOIN', etc.
    table_alias: str  # e.g., 'b'
    table_name: str  # SAP table name, e.g., "SAP_RISE_1"."T_RISE_EKUN"
    on_condition: str  # e.g., 'a.partner = b.partner'


@dataclass
class ParsedDBTSQL:
    """Parsed information from a DBT SQL file."""
    main_table: str  # SAP table name for the main FROM clause
    main_alias: str  # Table alias (usually 'a')
    joins: List[JoinInfo]  # List of JOIN information
    where_clause: Optional[str]  # WHERE clause without the 'where' keyword
    raw_sql: str  # Original SQL content


class DBTSQLParser:
    """Parser for DBT SQL files with Jinja templating."""

    # Pattern to match {{source('schema','table_name')}}
    SOURCE_PATTERN = re.compile(
        r"{{\s*source\s*\(\s*['\"]sapisu['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*}}"
    )

    # Pattern to match table names like 'raw_t_rise_xxx'
    RAW_TABLE_PATTERN = re.compile(r"raw_t_rise_(\w+)", re.IGNORECASE)

    def __init__(self, dbt_models_path: str = "/home/eduardo/statistical_validation/repos/galp-coedaai-dbt-saps4-billup-s3/models/sapisu/refined"):
        """
        Initialize the DBT SQL parser.

        Args:
            dbt_models_path: Path to the DBT models directory
        """
        self.dbt_models_path = Path(dbt_models_path)

    def parse_file(self, table_name: str) -> ParsedDBTSQL:
        """
        Parse a DBT SQL file for a given refined table name.

        Args:
            table_name: The refined table name (e.g., 'rfn_but000')

        Returns:
            ParsedDBTSQL object with extracted information

        Raises:
            FileNotFoundError: If the SQL file doesn't exist
            ValueError: If parsing fails
        """
        sql_file = self.dbt_models_path / f"{table_name}.sql"

        if not sql_file.exists():
            raise FileNotFoundError(f"DBT SQL file not found: {sql_file}")

        with open(sql_file, 'r', encoding='utf-8') as f:
            raw_sql = f.read()

        # Remove the config block at the beginning
        sql_content = self._remove_config_block(raw_sql)

        # Remove SQL comments to avoid parsing issues
        sql_content = self._remove_sql_comments(sql_content)

        # Extract FROM clause with main table
        main_table, main_alias = self._extract_main_table(sql_content)

        # Extract JOIN clauses
        joins = self._extract_joins(sql_content)

        # Extract WHERE clause
        where_clause = self._extract_where_clause(sql_content)

        return ParsedDBTSQL(
            main_table=main_table,
            main_alias=main_alias,
            joins=joins,
            where_clause=where_clause,
            raw_sql=raw_sql
        )

    def _remove_config_block(self, sql: str) -> str:
        """Remove the {{ config(...) }} block from the SQL."""
        # Remove config block (everything from first {{ config to matching }})
        config_pattern = re.compile(r'{{\s*config\s*\([^}]*\)\s*}}', re.DOTALL | re.IGNORECASE)
        return config_pattern.sub('', sql)

    def _remove_sql_comments(self, sql: str) -> str:
        """Remove SQL line comments (--) from the SQL."""
        # Remove lines that start with -- (with optional leading whitespace)
        # Also remove inline comments (everything after -- on a line)
        lines = []
        for line in sql.split('\n'):
            # Find the position of -- in the line
            comment_pos = line.find('--')
            if comment_pos != -1:
                # Keep only the part before the comment
                line = line[:comment_pos]
            # Only add non-empty lines or lines with content
            if line.strip():
                lines.append(line)
            else:
                lines.append('')  # Preserve empty lines for line numbers
        return '\n'.join(lines)

    def _convert_source_to_sap_table(self, source_ref: str) -> str:
        """
        Convert a DBT source reference to SAP HANA table name.

        Args:
            source_ref: DBT source like 'raw_t_rise_but000'

        Returns:
            SAP table name like "SAP_RISE_1"."T_RISE_BUT000"
        """
        match = self.RAW_TABLE_PATTERN.match(source_ref)
        if not match:
            raise ValueError(f"Invalid source reference format: {source_ref}")

        table_suffix = match.group(1).upper()
        return f'"SAP_RISE_1"."T_RISE_{table_suffix}"'

    def _extract_main_table(self, sql: str) -> Tuple[str, str]:
        """
        Extract the main table from the FROM clause.

        Returns:
            Tuple of (table_name, alias) - alias can be empty string if not present
        """
        # Pattern: from {{source(...)}} [optional_alias]
        # The alias must be on the same line (not newline) and not be a SQL keyword
        # Use [^\S\n] to match whitespace except newlines
        from_pattern_with_alias = re.compile(
            r'from\s+' + self.SOURCE_PATTERN.pattern + r'[^\S\n]+(\w+)(?=\s|$)',
            re.IGNORECASE
        )

        from_pattern_no_alias = re.compile(
            r'from\s+' + self.SOURCE_PATTERN.pattern + r'(?:\s*$|\s*\n)',
            re.IGNORECASE | re.MULTILINE
        )

        # Try with alias first (alias must be on same line)
        match = from_pattern_with_alias.search(sql)
        if match:
            source_table = match.group(1)
            alias = match.group(2)
            # Make sure the alias is not a SQL keyword
            sql_keywords = {'where', 'join', 'inner', 'left', 'right', 'outer', 'cross',
                           'group', 'order', 'having', 'limit', 'union', 'select'}
            if alias.lower() in sql_keywords:
                # This is not an alias, it's a SQL keyword on the next line
                alias = ''
            else:
                sap_table = self._convert_source_to_sap_table(source_table)
                return sap_table, alias

        # Try without alias (source reference ends with newline or end of line)
        match = from_pattern_no_alias.search(sql)
        if match:
            source_table = match.group(1)
            alias = ''
        else:
            # Fallback: just find the source reference
            match = self.SOURCE_PATTERN.search(sql)
            if match and 'from' in sql[:match.start()].lower().split()[-1:]:
                source_table = match.group(1)
                alias = ''
            else:
                raise ValueError("Could not find FROM clause with source reference")

        sap_table = self._convert_source_to_sap_table(source_table)
        return sap_table, alias

    def _extract_joins(self, sql: str) -> List[JoinInfo]:
        """
        Extract all JOIN clauses from the SQL.

        Returns:
            List of JoinInfo objects
        """
        joins = []

        # Pattern for JOIN: (LEFT|RIGHT|INNER|OUTER)? JOIN {{source(...)}} alias ON condition
        join_pattern = re.compile(
            r'((?:left|right|inner|outer|cross)?\s*join)\s+' +
            self.SOURCE_PATTERN.pattern +
            r'\s+(\w+)\s+on\s+([^\n]+?)(?=\s+(?:join|where|group|order|limit|$))',
            re.IGNORECASE | re.DOTALL
        )

        for match in join_pattern.finditer(sql):
            join_type = match.group(1).strip().upper()
            if join_type == 'JOIN':
                join_type = 'INNER JOIN'

            source_table = match.group(2)
            alias = match.group(3)
            on_condition = match.group(4).strip()

            sap_table = self._convert_source_to_sap_table(source_table)

            joins.append(JoinInfo(
                join_type=join_type,
                table_alias=alias,
                table_name=sap_table,
                on_condition=on_condition
            ))

        return joins

    def _extract_where_clause(self, sql: str) -> Optional[str]:
        """
        Extract the WHERE clause from the SQL.

        Returns:
            WHERE clause without the 'where' keyword, or None if no WHERE clause
        """
        # Pattern: where ... (until we hit group by, order by, limit, or end of string)
        # Use lookahead that doesn't require whitespace before end
        where_pattern = re.compile(
            r'where\s+(.*?)(?=\s*(?:group\s+by|order\s+by|limit|$))',
            re.IGNORECASE | re.DOTALL
        )

        match = where_pattern.search(sql)
        if not match:
            return None

        where_clause = match.group(1).strip()

        # Remove any trailing semicolons or whitespace
        where_clause = where_clause.rstrip(';').strip()

        return where_clause if where_clause else None

    def build_sap_query_filters(self, parsed: ParsedDBTSQL, status_column: str = 'EIM_CHANGE_STATUS') -> Dict[str, str]:
        """
        Build the filter components for a SAP query.

        Args:
            parsed: ParsedDBTSQL object
            status_column: Name of the status column to filter on (default: 'EIM_CHANGE_STATUS')

        Returns:
            Dictionary with 'from_clause', 'join_clause', 'where_clause'
        """
        # Build FROM clause (handle empty alias)
        if parsed.main_alias:
            from_clause = f"FROM {parsed.main_table} {parsed.main_alias}"
        else:
            from_clause = f"FROM {parsed.main_table}"

        # Build JOIN clause
        join_clause = ""
        if parsed.joins:
            join_parts = []
            for join in parsed.joins:
                join_parts.append(
                    f"{join.join_type} {join.table_name} {join.table_alias} "
                    f"ON {join.on_condition}"
                )
            join_clause = "\n".join(join_parts)

        # Build WHERE clause (always include status column filter)
        # Use alias prefix only if alias exists
        if parsed.main_alias:
            eim_filter = f'{parsed.main_alias}.{status_column} NOT IN (\'D\', \'X\')'
        else:
            eim_filter = f'{status_column} NOT IN (\'D\', \'X\')'

        where_parts = [eim_filter]

        if parsed.where_clause:
            where_parts.append(f"({parsed.where_clause})")

        where_clause = " AND ".join(where_parts)

        return {
            'from_clause': from_clause,
            'join_clause': join_clause,
            'where_clause': where_clause
        }


def parse_dbt_sql(table_name: str, dbt_models_path: Optional[str] = None) -> ParsedDBTSQL:
    """
    Convenience function to parse a DBT SQL file.

    Args:
        table_name: The refined table name (e.g., 'rfn_but000')
        dbt_models_path: Optional custom path to DBT models directory

    Returns:
        ParsedDBTSQL object
    """
    parser = DBTSQLParser(dbt_models_path) if dbt_models_path else DBTSQLParser()
    return parser.parse_file(table_name)
