"""
CSV Report Generator Module

Generates CSV reports from comparison results with detailed statistics.
"""

import csv
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..comparison.dbt_comparator import (
    ComparisonResult,
    DetailedColumnStats,
    NumericalStats,
    CategoricalStats,
    TemporalStats
)


class CSVGenerator:
    """Generate CSV reports from comparison results."""

    @staticmethod
    def generate_detailed_stats_csv(
        result: ComparisonResult,
        output_path: Path
    ) -> None:
        """
        Generate CSV file with detailed column statistics.
        Only includes columns that exist in both SAP and Dremio datasets.

        Args:
            result: Comparison result with detailed statistics
            output_path: Path to output CSV file
        """
        if not result.sap_detailed_stats and not result.dremio_detailed_stats:
            return

        # Build column mapping for easy lookup (case-insensitive keys)
        sap_stats_map = {}
        if result.sap_detailed_stats:
            sap_stats_map = {
                stat.column_name.lower(): stat
                for stat in result.sap_detailed_stats
            }

        dremio_stats_map = {}
        if result.dremio_detailed_stats:
            dremio_stats_map = {
                stat.column_name.lower(): stat
                for stat in result.dremio_detailed_stats
            }

        # Get common columns only (columns that exist in both datasets)
        common_columns = set(sap_stats_map.keys()) & set(dremio_stats_map.keys())

        # Prepare rows - only for common columns
        rows = []
        for col_name_lower in sorted(common_columns):
            sap_stat = sap_stats_map[col_name_lower]
            dremio_stat = dremio_stats_map[col_name_lower]

            # Use original column name from SAP (usually uppercase)
            display_name = sap_stat.column_name

            # Base row
            row = {
                'table': result.table_name,
                'column': display_name,
                'sap_type': sap_stat.column_type,
                'dremio_type': dremio_stat.column_type,
                'type_match': 'YES' if sap_stat.column_type == dremio_stat.column_type else 'NO'
            }

            # Add SAP statistics
            row.update(CSVGenerator._flatten_stats('sap', sap_stat))

            # Add Dremio statistics
            row.update(CSVGenerator._flatten_stats('dremio', dremio_stat))

            rows.append(row)

        # Write CSV
        if rows:
            fieldnames = list(rows[0].keys())
            with open(output_path, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

    @staticmethod
    def _flatten_stats(prefix: str, col_stat: DetailedColumnStats) -> Dict[str, Any]:
        """
        Flatten detailed statistics to dictionary with prefixed keys.

        Args:
            prefix: Prefix for keys ('sap' or 'dremio')
            col_stat: Detailed column statistics

        Returns:
            Dictionary with flattened statistics
        """
        stats = col_stat.stats
        flat = {}

        if isinstance(stats, NumericalStats):
            flat[f'{prefix}_null_count'] = stats.null_count
            flat[f'{prefix}_unique_count'] = stats.unique_count
            flat[f'{prefix}_unique_pct'] = round(stats.unique_percentage, 2)
            flat[f'{prefix}_mean'] = round(stats.mean, 4) if stats.mean is not None else None
            flat[f'{prefix}_median'] = round(stats.median, 4) if stats.median is not None else None
            flat[f'{prefix}_mode1'] = stats.mode1
            flat[f'{prefix}_mode2'] = stats.mode2
            flat[f'{prefix}_min'] = stats.min
            flat[f'{prefix}_max'] = stats.max
            # Add empty categorical/temporal fields
            flat[f'{prefix}_min_length'] = None
            flat[f'{prefix}_max_length'] = None
            flat[f'{prefix}_avg_length'] = None
            flat[f'{prefix}_date_format'] = None
            flat[f'{prefix}_min_date'] = None
            flat[f'{prefix}_max_date'] = None
            flat[f'{prefix}_most_common_date'] = None

        elif isinstance(stats, CategoricalStats):
            flat[f'{prefix}_null_count'] = stats.null_count
            flat[f'{prefix}_unique_count'] = stats.unique_count
            flat[f'{prefix}_unique_pct'] = round(stats.unique_percentage, 2)
            flat[f'{prefix}_min_length'] = stats.min_length
            flat[f'{prefix}_max_length'] = stats.max_length
            flat[f'{prefix}_avg_length'] = round(stats.avg_length, 2) if stats.avg_length is not None else None
            flat[f'{prefix}_mode1'] = stats.mode1
            flat[f'{prefix}_mode2'] = stats.mode2
            # Add empty numerical/temporal fields
            flat[f'{prefix}_mean'] = None
            flat[f'{prefix}_median'] = None
            flat[f'{prefix}_min'] = None
            flat[f'{prefix}_max'] = None
            flat[f'{prefix}_date_format'] = None
            flat[f'{prefix}_min_date'] = None
            flat[f'{prefix}_max_date'] = None
            flat[f'{prefix}_most_common_date'] = None

        elif isinstance(stats, TemporalStats):
            flat[f'{prefix}_null_count'] = stats.null_count
            flat[f'{prefix}_unique_count'] = stats.unique_count
            flat[f'{prefix}_unique_pct'] = round(stats.unique_percentage, 2)
            flat[f'{prefix}_date_format'] = stats.format
            flat[f'{prefix}_min_date'] = stats.min_date
            flat[f'{prefix}_max_date'] = stats.max_date
            flat[f'{prefix}_most_common_date'] = stats.most_common_date
            # Add empty numerical/categorical fields
            flat[f'{prefix}_mean'] = None
            flat[f'{prefix}_median'] = None
            flat[f'{prefix}_min'] = None
            flat[f'{prefix}_max'] = None
            flat[f'{prefix}_min_length'] = None
            flat[f'{prefix}_max_length'] = None
            flat[f'{prefix}_avg_length'] = None
            flat[f'{prefix}_mode1'] = None
            flat[f'{prefix}_mode2'] = None

        return flat

    @staticmethod
    def _get_empty_stats(prefix: str, col_type: str) -> Dict[str, Any]:
        """
        Get empty statistics dictionary with None values.

        Args:
            prefix: Prefix for keys ('sap' or 'dremio')
            col_type: Column type

        Returns:
            Dictionary with None values
        """
        return {
            f'{prefix}_null_count': None,
            f'{prefix}_unique_count': None,
            f'{prefix}_unique_pct': None,
            f'{prefix}_mean': None,
            f'{prefix}_median': None,
            f'{prefix}_mode1': None,
            f'{prefix}_mode2': None,
            f'{prefix}_min': None,
            f'{prefix}_max': None,
            f'{prefix}_min_length': None,
            f'{prefix}_max_length': None,
            f'{prefix}_avg_length': None,
            f'{prefix}_date_format': None,
            f'{prefix}_min_date': None,
            f'{prefix}_max_date': None,
            f'{prefix}_most_common_date': None
        }
