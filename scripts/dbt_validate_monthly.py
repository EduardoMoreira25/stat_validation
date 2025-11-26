#!/usr/bin/env python3
"""
DBT Monthly Validation Script

Validates SAP source tables (with DBT filters applied) against Dremio refined tables
for a specific month and year.

Usage:
    python3 scripts/dbt_validate_monthly.py --year 2025 --month 7 --table rfn_but000
    python3 scripts/dbt_validate_monthly.py --year 2025 --month 7  # All tables from CSV
"""

import argparse
import csv
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.stat_validator.connectors.dremio_connector import DremioConnector
from src.stat_validator.connectors.hana_connector import HanaConnector
from src.stat_validator.comparison.dbt_comparator import DBTComparator
from src.stat_validator.parsers.dbt_sql_parser import DBTSQLParser
from src.stat_validator.utils.config_loader import ConfigLoader
from src.stat_validator.reporting.excel_generator import ExcelGenerator


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_table_mappings(csv_path: str) -> List[Dict[str, str]]:
    """
    Load table mappings from CSV file.

    Args:
        csv_path: Path to the CSV file with table mappings

    Returns:
        List of dictionaries with table information
    """
    tables = []
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tables.append({
                'dremio_schema': row['schema'],
                'dremio_table': row['Ulysses'],
                'sap_schema': row['schema.1'],
                'sap_table': row['SAP EIM']
            })
    return tables


def format_text_report(result) -> str:
    """Format comparison result as a text report."""
    lines = []
    lines.append("=" * 80)
    lines.append(f"DBT MONTHLY VALIDATION REPORT")
    lines.append("=" * 80)
    lines.append(f"Table: {result.table_name}")
    lines.append(f"Period: {result.year}-{result.month:02d}")
    lines.append(f"Status: {result.overall_status}")
    lines.append("")

    lines.append("-" * 80)
    lines.append("ROW COUNT COMPARISON")
    lines.append("-" * 80)
    lines.append(f"SAP Source:      {result.sap_stats.row_count:,}")
    lines.append(f"Dremio Refined:  {result.dremio_stats.row_count:,}")
    lines.append(f"Difference:      {result.row_count_diff:,} ({result.row_count_diff_pct:.2f}%)")
    lines.append(f"Match:           {'YES' if result.row_count_match else 'NO'}")
    lines.append("")

    lines.append("-" * 80)
    lines.append("REFRESH DATE RANGE COMPARISON")
    lines.append("-" * 80)

    # Format datetime objects for display
    sap_min = result.sap_stats.min_refresh_dt.strftime('%Y-%m-%d %H:%M:%S') if result.sap_stats.min_refresh_dt else 'None'
    sap_max = result.sap_stats.max_refresh_dt.strftime('%Y-%m-%d %H:%M:%S') if result.sap_stats.max_refresh_dt else 'None'
    dremio_min = result.dremio_stats.min_refresh_dt.strftime('%Y-%m-%d %H:%M:%S') if result.dremio_stats.min_refresh_dt else 'None'
    dremio_max = result.dremio_stats.max_refresh_dt.strftime('%Y-%m-%d %H:%M:%S') if result.dremio_stats.max_refresh_dt else 'None'

    lines.append(f"SAP Source:      [{sap_min} to {sap_max}]")
    lines.append(f"Dremio Refined:  [{dremio_min} to {dremio_max}]")
    lines.append(f"Match:           {'YES' if result.refresh_dt_match else 'NO'}")
    lines.append("")

    lines.append("-" * 80)
    lines.append("NULL PERCENTAGE COMPARISON (Top 20 Mismatches)")
    lines.append("-" * 80)
    lines.append(f"{'Column':<40} {'SAP %':>10} {'Dremio %':>10} {'Match':>8}")
    lines.append("-" * 80)

    # Show mismatches first, then matches
    mismatches = [(col, info) for col, info in result.null_comparison.items() if not info['match']]
    matches = [(col, info) for col, info in result.null_comparison.items() if info['match']]

    # Show up to 20 mismatches
    for col, info in sorted(mismatches, key=lambda x: abs((x[1]['sap_null_pct'] or 0) - (x[1]['dremio_null_pct'] or 0)), reverse=True)[:20]:
        sap_pct = f"{info['sap_null_pct']:.2f}" if info['sap_null_pct'] is not None else "N/A"
        dremio_pct = f"{info['dremio_null_pct']:.2f}" if info['dremio_null_pct'] is not None else "N/A"
        match_str = "YES" if info['match'] else "NO"
        lines.append(f"{col:<40} {sap_pct:>10} {dremio_pct:>10} {match_str:>8}")

    # Show summary of matches
    if matches:
        lines.append("")
        lines.append(f"({len(matches)} columns with matching null percentages)")

    if result.issues:
        lines.append("")
        lines.append("-" * 80)
        lines.append("VALIDATION ISSUES")
        lines.append("-" * 80)
        for issue in result.issues:
            lines.append(f"  - {issue}")

    lines.append("")
    lines.append("=" * 80)
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Validate SAP source tables (with DBT filters) against Dremio refined tables"
    )
    parser.add_argument(
        '--year',
        type=int,
        required=True,
        help='Year to validate (e.g., 2025)'
    )
    parser.add_argument(
        '--month',
        type=int,
        required=True,
        help='Month to validate (1-12)'
    )
    parser.add_argument(
        '--table',
        type=str,
        help='Specific table to validate (e.g., rfn_but000). If not provided, validates all tables from CSV.'
    )
    parser.add_argument(
        '--csv',
        type=str,
        default='rfn_sapisu_tables1.csv',
        help='CSV file with table mappings (default: rfn_sapisu_tables1.csv)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='dbt_validation_results',
        help='Output directory for reports (default: dbt_validation_results)'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Configuration file path (default: config/config.yaml)'
    )

    args = parser.parse_args()

    # Validate month
    if not 1 <= args.month <= 12:
        logger.error(f"Invalid month: {args.month}. Must be between 1 and 12.")
        sys.exit(1)

    # Load configuration
    config_loader = ConfigLoader(args.config)
    config = config_loader.get_all()

    # Create output directory with hierarchical structure: sap/year/month
    output_dir = Path(args.output_dir) / "sap" / str(args.year) / f"{args.month:02d}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load table mappings
    csv_path = Path(args.csv)
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    tables = load_table_mappings(str(csv_path))

    # Filter to specific table if provided
    if args.table:
        tables = [t for t in tables if t['dremio_table'] == args.table]
        if not tables:
            logger.error(f"Table {args.table} not found in {csv_path}")
            sys.exit(1)

    logger.info(f"Loaded {len(tables)} table(s) to validate")

    # Initialize connectors
    logger.info("Connecting to Dremio...")
    dremio_config = config_loader.get_dremio_config()
    dremio_connector = DremioConnector(**dremio_config)

    logger.info("Connecting to SAP HANA...")
    hana_config = config_loader.get_hana_config()
    sap_connector = HanaConnector(**hana_config)

    # Initialize comparator
    dbt_parser = DBTSQLParser()
    comparator = DBTComparator(dremio_connector, sap_connector, dbt_parser)

    # Run validations
    results = []
    daily_breakdowns = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for table_info in tables:
        try:
            logger.info(f"Validating {table_info['dremio_table']}...")

            result = comparator.compare_table(
                dremio_schema=table_info['dremio_schema'],
                dremio_table=table_info['dremio_table'],
                sap_schema=table_info['sap_schema'],
                sap_table=table_info['sap_table'],
                year=args.year,
                month=args.month
            )

            results.append(result)

            logger.info(f"  Status: {result.overall_status}")
            logger.info(f"  Row count: SAP={result.sap_stats.row_count:,}, Dremio={result.dremio_stats.row_count:,}")

            # Save individual reports
            table_name = table_info['dremio_table']

            # Save text report
            txt_file = output_dir / f"{table_name}_{args.year}{args.month:02d}_{timestamp}.txt"
            with open(txt_file, 'w') as f:
                f.write(format_text_report(result))
            logger.info(f"  Text report: {txt_file}")

            # Save JSON report
            json_file = output_dir / f"{table_name}_{args.year}{args.month:02d}_{timestamp}.json"
            with open(json_file, 'w') as f:
                json.dump(comparator.to_dict(result), f, indent=2)
            logger.info(f"  JSON report: {json_file}")

            # Get daily breakdown for Excel report
            try:
                logger.info(f"  Fetching daily breakdown...")
                daily_breakdown = comparator.get_daily_breakdown(
                    dremio_schema=table_info['dremio_schema'],
                    dremio_table=table_info['dremio_table'],
                    sap_schema=table_info['sap_schema'],
                    sap_table=table_info['sap_table'],
                    year=args.year,
                    month=args.month
                )
                daily_breakdowns[table_name] = daily_breakdown
            except Exception as e:
                logger.warning(f"  Could not fetch daily breakdown: {e}")
                # Continue even if daily breakdown fails
                daily_breakdowns[table_name] = {
                    'table_name': table_name,
                    'year': args.year,
                    'month': args.month,
                    'daily_data': []
                }

        except Exception as e:
            logger.error(f"Error validating {table_info['dremio_table']}: {e}", exc_info=True)
            continue

    # Generate summary report
    if results:
        summary_file = output_dir / f"summary_{args.year}{args.month:02d}_{timestamp}.txt"
        with open(summary_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write(f"DBT MONTHLY VALIDATION SUMMARY\n")
            f.write("=" * 80 + "\n")
            f.write(f"Period: {args.year}-{args.month:02d}\n")
            f.write(f"Total tables: {len(results)}\n")
            f.write(f"Passed: {sum(1 for r in results if r.overall_status == 'PASS')}\n")
            f.write(f"Failed: {sum(1 for r in results if r.overall_status == 'FAIL')}\n")
            f.write("\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Table':<40} {'Status':>10} {'Row Diff %':>15}\n")
            f.write("-" * 80 + "\n")
            # Sort by absolute row difference percentage (largest first)
            sorted_results = sorted(results, key=lambda r: abs(r.row_count_diff_pct), reverse=True)
            for result in sorted_results:
                f.write(f"{result.table_name:<40} {result.overall_status:>10} {result.row_count_diff_pct:>14.2f}%\n")
            f.write("=" * 80 + "\n")

        logger.info(f"\nSummary report: {summary_file}")

        # Generate Excel report with summary and daily breakdowns
        try:
            excel_file = output_dir / f"summary_{args.year}{args.month:02d}_{timestamp}.xlsx"
            logger.info(f"\nGenerating Excel report...")
            excel_generator = ExcelGenerator()
            excel_generator.generate_validation_report(
                output_path=excel_file,
                results=results,
                daily_breakdowns=daily_breakdowns,
                year=args.year,
                month=args.month
            )
            logger.info(f"Excel report: {excel_file}")
        except Exception as e:
            logger.error(f"Error generating Excel report: {e}", exc_info=True)

    # Close connectors
    dremio_connector.close()
    sap_connector.close()

    logger.info("Validation complete!")


if __name__ == "__main__":
    main()
