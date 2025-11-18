#!/usr/bin/env python3
"""
Bulk Validation Script for SAPISU Tables

Reads sapisu_tables.csv and runs compare-cross validation for all tables,
organizing outputs by date into reports/ and logs/ directories.

Usage:
    python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18
    python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --csv sapisu_tables.csv
    python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --parallel 4
"""

import csv
import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional, Dict, Any
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
import json


# ============================================================================
# Summary Report Generation Functions (from summarize_validation_results.py)
# ============================================================================

def find_json_files(report_dir: Path) -> List[Path]:
    """Find all validation JSON files in a directory."""
    json_files = list(report_dir.glob("validation_*.json"))
    return sorted(json_files)


def parse_validation_report(json_file: Path) -> Optional[Dict[str, Any]]:
    """Parse a validation JSON report and extract key metrics."""
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)

        # Extract row count test
        row_count_test = None
        for test in data.get('tests', []):
            if test.get('test_name') == 'row_count':
                row_count_test = test
                break

        if not row_count_test:
            return None

        details = row_count_test.get('details', {})

        return {
            'source_table': data.get('source_table', 'Unknown'),
            'dest_table': data.get('dest_table', 'Unknown'),
            'source_count': details.get('source_count', 0),
            'dest_count': details.get('dest_count', 0),
            'difference': details.get('difference', 0),
            'difference_pct': details.get('difference_pct', 0),
            'status': row_count_test.get('status', 'UNKNOWN'),
            'overall_status': data.get('overall_status', 'UNKNOWN'),
            'timestamp': data.get('timestamp', 'Unknown')
        }

    except Exception as e:
        print(f"Error parsing {json_file}: {str(e)}", file=sys.stderr)
        return None


def extract_table_names(source_table: str, dest_table: str) -> tuple:
    """Extract clean table names from fully qualified names."""
    # SAP: "SAP_RISE_1"."T_RISE_DFKKOP" -> T_RISE_DFKKOP
    sap_name = source_table.split('.')[-1].replace('"', '')

    # Dremio: ulysses.sapisu."rfn_dfkkop" -> rfn_dfkkop
    dremio_name = dest_table.split('.')[-1].replace('"', '')

    return sap_name, dremio_name


def generate_summary_report_content(reports: List[Dict[str, Any]], filter_date: str = None) -> str:
    """Generate summary report content as a string."""

    if len(reports) == 0:
        return "No validation reports found!"

    # Calculate statistics
    total_tables = len(reports)
    perfect_match = sum(1 for r in reports if r['difference'] == 0)
    with_differences = sum(1 for r in reports if r['difference'] != 0)
    more_in_dremio = sum(1 for r in reports if r['difference'] > 0)
    more_in_sap = sum(1 for r in reports if r['difference'] < 0)

    total_sap_rows = sum(r['source_count'] for r in reports)
    total_dremio_rows = sum(r['dest_count'] for r in reports)
    total_difference = sum(abs(r['difference']) for r in reports)

    # Sort by absolute difference (largest first)
    reports_sorted = sorted(reports, key=lambda x: abs(x['difference']), reverse=True)

    # Build report content
    lines = []
    lines.append("=" * 100)
    lines.append(" " * 30 + "VALIDATION SUMMARY REPORT")
    lines.append("=" * 100)
    lines.append("")

    if filter_date:
        lines.append(f"Filter Date: {filter_date}")
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Total Tables: {total_tables}")
    lines.append("")

    lines.append("-" * 100)
    lines.append("OVERALL STATISTICS")
    lines.append("-" * 100)
    lines.append(f"  Perfect Match (0 difference):     {perfect_match:>6} tables ({perfect_match/total_tables*100:.1f}%)")
    lines.append(f"  With Differences:                 {with_differences:>6} tables ({with_differences/total_tables*100:.1f}%)")
    lines.append(f"    - More rows in Dremio:          {more_in_dremio:>6} tables")
    lines.append(f"    - More rows in SAP HANA:        {more_in_sap:>6} tables")
    lines.append("")

    lines.append(f"  Total Rows in SAP HANA:           {total_sap_rows:>15,}")
    lines.append(f"  Total Rows in Dremio:             {total_dremio_rows:>15,}")
    lines.append(f"  Total Absolute Difference:        {total_difference:>15,}")
    if total_sap_rows > 0:
        lines.append(f"  Overall Match Rate:               {(1 - total_difference/total_sap_rows)*100:>14.2f}%")
    lines.append("")

    lines.append("=" * 100)
    lines.append("DETAILED TABLE COMPARISON (sorted by difference)")
    lines.append("=" * 100)
    lines.append("")

    # Table header
    lines.append(f"{'SAP Table':<35} {'Dremio Table':<35} {'SAP Count':>12} {'Dremio Count':>12} {'Diff':>10} {'%':>8} {'Status':<8}")
    lines.append("-" * 140)

    # Table rows
    for report in reports_sorted:
        sap_name, dremio_name = extract_table_names(report['source_table'], report['dest_table'])

        # Truncate long names
        sap_display = sap_name[:34] if len(sap_name) <= 34 else sap_name[:31] + "..."
        dremio_display = dremio_name[:34] if len(dremio_name) <= 34 else dremio_name[:31] + "..."

        diff = report['difference']
        diff_pct = report['difference_pct']
        status = "MATCH" if diff == 0 else ("WARN" if abs(diff_pct) < 1.0 else "FAIL")

        # Format difference with +/- sign
        diff_str = f"{diff:+,}"

        lines.append(f"{sap_display:<35} {dremio_display:<35} {report['source_count']:>12,} {report['dest_count']:>12,} {diff_str:>10} {diff_pct:>7.2f}% {status:<8}")

    lines.append("")
    lines.append("=" * 100)
    lines.append("TABLES WITH LARGEST DIFFERENCES (Top 10)")
    lines.append("=" * 100)
    lines.append("")

    for i, report in enumerate(reports_sorted[:10], 1):
        sap_name, dremio_name = extract_table_names(report['source_table'], report['dest_table'])
        diff = report['difference']
        diff_pct = report['difference_pct']

        lines.append(f"{i:2}. {sap_name} → {dremio_name}")
        lines.append(f"    SAP: {report['source_count']:,} | Dremio: {report['dest_count']:,} | Diff: {diff:+,} ({diff_pct:+.2f}%)")
        lines.append("")

    lines.append("=" * 100)
    lines.append("PERFECT MATCHES (0 difference)")
    lines.append("=" * 100)
    lines.append("")

    perfect_reports = [r for r in reports_sorted if r['difference'] == 0]
    if perfect_reports:
        lines.append(f"Total: {len(perfect_reports)} tables")
        lines.append("")
        for report in perfect_reports:
            sap_name, dremio_name = extract_table_names(report['source_table'], report['dest_table'])
            lines.append(f"  ✓ {sap_name:<40} → {dremio_name:<40} ({report['source_count']:,} rows)")
    else:
        lines.append("  No perfect matches found.")

    lines.append("")
    lines.append("=" * 100)
    lines.append("END OF REPORT")
    lines.append("=" * 100)

    return '\n'.join(lines)


# ============================================================================
# End of Summary Report Generation Functions
# ============================================================================


def setup_logging(log_dir: Path, table_name: str) -> logging.Logger:
    """Setup logging for a specific table validation."""
    log_file = log_dir / f"{table_name}.log"

    # Create logger
    logger = logging.getLogger(f"bulk_validator.{table_name}")
    logger.setLevel(logging.DEBUG)

    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger


def read_tables_csv(csv_path: Path) -> List[Tuple[str, str, str, str]]:
    """
    Read tables from CSV file.

    Returns:
        List of tuples: (dremio_schema, dremio_table, sap_schema, sap_table)
    """
    tables = []

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dremio_schema = row['schema']
            dremio_table = row['Ulysses']
            sap_schema = row['schema.1']
            sap_table = row['SAP EIM']

            tables.append((dremio_schema, dremio_table, sap_schema, sap_table))

    return tables


def validate_table(
    dremio_schema: str,
    dremio_table: str,
    sap_schema: str,
    sap_table: str,
    filter_date: str,
    report_dir: Path,
    log_dir: Path,
    dremio_prefix: str = "ulysses"
) -> dict:
    """
    Run validation for a single table.

    Returns:
        dict with validation results and metadata
    """
    # Setup logging for this table
    logger = setup_logging(log_dir, dremio_table)

    # Build table names
    dremio_full = f'{dremio_prefix}.{dremio_schema}."{dremio_table}"'
    sap_full = f'"{sap_schema}"."{sap_table}"'

    logger.info(f"Starting validation: {sap_full} → {dremio_full}")
    logger.info(f"Filter date: {filter_date}")

    # Build command
    cmd = [
        'python3', '-m', 'src.stat_validator.cli', 'compare-cross',
        sap_full,
        dremio_full,
        '--filter-date', filter_date,
        '--output-dir', str(report_dir),
        '--formats', 'json',
        '--formats', 'html'
    ]

    logger.debug(f"Command: {' '.join(cmd)}")

    # Run validation
    start_time = datetime.now()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout per table
        )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Log output
        if result.stdout:
            logger.info("STDOUT:")
            for line in result.stdout.split('\n'):
                if line.strip():
                    logger.info(f"  {line}")

        if result.stderr:
            logger.warning("STDERR:")
            for line in result.stderr.split('\n'):
                if line.strip():
                    logger.warning(f"  {line}")

        # Check exit code
        if result.returncode == 0:
            logger.info(f"✅ Validation PASSED (duration: {duration:.1f}s)")
            status = 'PASS'
        elif result.returncode == 1:
            logger.error(f"❌ Validation FAILED (duration: {duration:.1f}s)")
            status = 'FAIL'
        else:
            logger.error(f"⚠️  Validation ERROR (exit code: {result.returncode}, duration: {duration:.1f}s)")
            status = 'ERROR'

        return {
            'table': dremio_table,
            'sap_table': sap_full,
            'dremio_table': dremio_full,
            'status': status,
            'exit_code': result.returncode,
            'duration': duration,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat()
        }

    except subprocess.TimeoutExpired:
        logger.error(f"❌ Validation TIMEOUT (exceeded 1 hour)")
        return {
            'table': dremio_table,
            'sap_table': sap_full,
            'dremio_table': dremio_full,
            'status': 'TIMEOUT',
            'exit_code': -1,
            'duration': 3600,
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"❌ Validation EXCEPTION: {str(e)}")
        return {
            'table': dremio_table,
            'sap_table': sap_full,
            'dremio_table': dremio_full,
            'status': 'EXCEPTION',
            'exit_code': -1,
            'duration': 0,
            'error': str(e),
            'start_time': start_time.isoformat(),
            'end_time': datetime.now().isoformat()
        }


def run_bulk_validation(
    csv_path: Path,
    filter_date: str,
    dremio_prefix: str = "ulysses",
    parallel: int = 1
):
    """Run bulk validation for all tables in CSV."""

    # Parse filter date to create directory structure
    try:
        date_obj = datetime.strptime(filter_date, '%Y-%m-%d')
        year = date_obj.strftime('%Y')
        month = date_obj.strftime('%m')
        day = date_obj.strftime('%d')
    except ValueError:
        print(f"❌ Invalid date format: {filter_date}. Expected YYYY-MM-DD")
        sys.exit(1)

    # Create directory structure
    base_report_dir = Path('reports/sap') / year / month / day
    base_log_dir = Path('logs/sap') / year / month / day

    base_report_dir.mkdir(parents=True, exist_ok=True)
    base_log_dir.mkdir(parents=True, exist_ok=True)

    # Setup main logger
    main_log_file = base_log_dir / '_bulk_validation.log'
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(main_log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger('bulk_validator')

    # Read tables
    logger.info(f"Reading tables from {csv_path}")
    tables = read_tables_csv(csv_path)
    logger.info(f"Found {len(tables)} tables to validate")

    # Run validations
    results = []

    if parallel > 1:
        logger.info(f"Running validations in parallel (workers: {parallel})")

        with ProcessPoolExecutor(max_workers=parallel) as executor:
            futures = {}

            for dremio_schema, dremio_table, sap_schema, sap_table in tables:
                future = executor.submit(
                    validate_table,
                    dremio_schema, dremio_table, sap_schema, sap_table,
                    filter_date, base_report_dir, base_log_dir, dremio_prefix
                )
                futures[future] = dremio_table

            # Process results as they complete
            for future in as_completed(futures):
                table_name = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"Completed: {table_name} - {result['status']}")
                except Exception as e:
                    logger.error(f"Failed to process {table_name}: {str(e)}")
                    results.append({
                        'table': table_name,
                        'status': 'EXCEPTION',
                        'error': str(e)
                    })
    else:
        logger.info("Running validations sequentially")

        for idx, (dremio_schema, dremio_table, sap_schema, sap_table) in enumerate(tables, 1):
            logger.info(f"\n{'='*80}")
            logger.info(f"Processing table {idx}/{len(tables)}: {dremio_table}")
            logger.info(f"{'='*80}")

            result = validate_table(
                dremio_schema, dremio_table, sap_schema, sap_table,
                filter_date, base_report_dir, base_log_dir, dremio_prefix
            )
            results.append(result)

    # Generate summary
    logger.info(f"\n{'='*80}")
    logger.info("BULK VALIDATION SUMMARY")
    logger.info(f"{'='*80}")

    passed = sum(1 for r in results if r['status'] == 'PASS')
    failed = sum(1 for r in results if r['status'] == 'FAIL')
    errors = sum(1 for r in results if r['status'] in ['ERROR', 'EXCEPTION', 'TIMEOUT'])

    logger.info(f"Total tables: {len(results)}")
    logger.info(f"Passed: {passed}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Errors: {errors}")

    total_duration = sum(r.get('duration', 0) for r in results)
    logger.info(f"Total duration: {total_duration:.1f}s ({total_duration/60:.1f} minutes)")

    # List failed tables
    if failed > 0:
        logger.info(f"\n❌ Failed tables:")
        for r in results:
            if r['status'] == 'FAIL':
                logger.info(f"  - {r['table']}")

    # List error tables
    if errors > 0:
        logger.info(f"\n⚠️  Error tables:")
        for r in results:
            if r['status'] in ['ERROR', 'EXCEPTION', 'TIMEOUT']:
                logger.info(f"  - {r['table']} ({r['status']})")

    # Save summary JSON
    summary_file = base_log_dir / '_summary.json'
    with open(summary_file, 'w') as f:
        json.dump({
            'filter_date': filter_date,
            'total_tables': len(results),
            'passed': passed,
            'failed': failed,
            'errors': errors,
            'total_duration': total_duration,
            'results': results
        }, f, indent=2)

    logger.info(f"\n📊 Summary saved to: {summary_file}")
    logger.info(f"📁 Reports directory: {base_report_dir}")
    logger.info(f"📁 Logs directory: {base_log_dir}")

    # Generate text summary report automatically
    logger.info(f"\n{'='*80}")
    logger.info("GENERATING TEXT SUMMARY REPORT")
    logger.info(f"{'='*80}")

    try:
        # Find and parse all JSON reports
        logger.info(f"📂 Searching for validation reports in: {base_report_dir}")
        json_files = find_json_files(base_report_dir)

        if len(json_files) > 0:
            logger.info(f"📊 Found {len(json_files)} validation reports")

            # Parse reports
            logger.info("🔍 Parsing validation reports...")
            parsed_reports = []
            for json_file in json_files:
                report = parse_validation_report(json_file)
                if report:
                    parsed_reports.append(report)

            if len(parsed_reports) > 0:
                logger.info(f"✅ Parsed {len(parsed_reports)} valid reports")

                # Create summary directory
                summary_dir = Path('reports/sap/summary')
                summary_dir.mkdir(parents=True, exist_ok=True)

                # Generate filename with filter date and execution date
                # Format: summary_{filter_date}____{execution_date}.txt
                # Example: summary_2025_11_12____17_11_2025.txt
                filter_date_formatted = filter_date.replace('-', '_')
                execution_date = datetime.now().strftime('%d_%m_%Y')
                summary_filename = f"summary_{filter_date_formatted}____{execution_date}.txt"
                summary_path = summary_dir / summary_filename

                # Generate and save summary
                logger.info(f"📝 Generating summary report...")
                summary_content = generate_summary_report_content(parsed_reports, filter_date)

                with open(summary_path, 'w') as f:
                    f.write(summary_content)

                logger.info(f"✅ Text summary report saved to: {summary_path}")
                logger.info(f"\n📄 Summary report location:")
                logger.info(f"   {summary_path}")

            else:
                logger.warning("⚠️  No valid reports could be parsed for summary")
        else:
            logger.warning(f"⚠️  No validation JSON files found in {base_report_dir}")

    except Exception as e:
        logger.error(f"❌ Failed to generate text summary: {str(e)}")
        # Don't fail the entire script if summary generation fails
        import traceback
        traceback.print_exc()

    # Exit with appropriate code
    if errors > 0 or failed > 0:
        sys.exit(1)
    else:
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description='Bulk validation for SAPISU tables',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate all tables for a specific date
  python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18

  # Use custom CSV file
  python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --csv custom_tables.csv

  # Run validations in parallel (4 workers)
  python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --parallel 4

  # Use different Dremio prefix
  python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --dremio-prefix ulysses2
        """
    )

    parser.add_argument(
        '--filter-date', '-d',
        required=True,
        help='Filter date for incremental validation (YYYY-MM-DD)'
    )

    parser.add_argument(
        '--csv',
        default='sapisu_tables.csv',
        help='Path to CSV file with table mappings (default: sapisu_tables.csv)'
    )

    parser.add_argument(
        '--dremio-prefix',
        default='ulysses',
        help='Dremio catalog prefix (default: ulysses)'
    )

    parser.add_argument(
        '--parallel', '-p',
        type=int,
        default=1,
        help='Number of parallel workers (default: 1 = sequential)'
    )

    args = parser.parse_args()

    # Validate CSV exists
    csv_path = Path(args.csv)
    if not csv_path.exists():
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)

    # Run bulk validation
    run_bulk_validation(
        csv_path=csv_path,
        filter_date=args.filter_date,
        dremio_prefix=args.dremio_prefix,
        parallel=args.parallel
    )


if __name__ == '__main__':
    main()
