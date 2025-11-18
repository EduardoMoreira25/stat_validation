#!/usr/bin/env python3
"""
Generate Summary Report from Bulk Validation Results

Reads JSON validation reports and creates a summary table showing
row count differences between SAP HANA and Dremio.

Usage:
    # By date
    python scripts/summarize_validation_results.py --date 2025-11-11

    # By path
    python scripts/summarize_validation_results.py --path reports/sap/2025/11/11

    # With custom output file
    python scripts/summarize_validation_results.py --date 2025-11-11 --output summary_2025-11-11.txt
"""

import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any


def find_json_files(report_dir: Path) -> List[Path]:
    """Find all validation JSON files in a directory."""
    json_files = list(report_dir.glob("validation_*.json"))
    return sorted(json_files)


def parse_validation_report(json_file: Path) -> Dict[str, Any]:
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

    # Dremio: ulysses1.sapisu."rfn_dfkkop" -> rfn_dfkkop
    dremio_name = dest_table.split('.')[-1].replace('"', '')

    return sap_name, dremio_name


def generate_summary_report(reports: List[Dict[str, Any]], output_file: Path, filter_date: str = None):
    """Generate a text summary report."""

    if len(reports) == 0:
        print("No validation reports found!")
        return

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

    # Write report
    with open(output_file, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write(" " * 30 + "VALIDATION SUMMARY REPORT\n")
        f.write("=" * 100 + "\n\n")

        if filter_date:
            f.write(f"Filter Date: {filter_date}\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total Tables: {total_tables}\n\n")

        f.write("-" * 100 + "\n")
        f.write("OVERALL STATISTICS\n")
        f.write("-" * 100 + "\n")
        f.write(f"  Perfect Match (0 difference):     {perfect_match:>6} tables ({perfect_match/total_tables*100:.1f}%)\n")
        f.write(f"  With Differences:                 {with_differences:>6} tables ({with_differences/total_tables*100:.1f}%)\n")
        f.write(f"    - More rows in Dremio:          {more_in_dremio:>6} tables\n")
        f.write(f"    - More rows in SAP HANA:        {more_in_sap:>6} tables\n\n")

        f.write(f"  Total Rows in SAP HANA:           {total_sap_rows:>15,}\n")
        f.write(f"  Total Rows in Dremio:             {total_dremio_rows:>15,}\n")
        f.write(f"  Total Absolute Difference:        {total_difference:>15,}\n")
        f.write(f"  Overall Match Rate:               {(1 - total_difference/total_sap_rows)*100:>14.2f}%\n\n")

        f.write("=" * 100 + "\n")
        f.write("DETAILED TABLE COMPARISON (sorted by difference)\n")
        f.write("=" * 100 + "\n\n")

        # Table header
        f.write(f"{'SAP Table':<35} {'Dremio Table':<35} {'SAP Count':>12} {'Dremio Count':>12} {'Diff':>10} {'%':>8} {'Status':<8}\n")
        f.write("-" * 140 + "\n")

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

            f.write(f"{sap_display:<35} {dremio_display:<35} {report['source_count']:>12,} {report['dest_count']:>12,} {diff_str:>10} {diff_pct:>7.2f}% {status:<8}\n")

        f.write("\n" + "=" * 100 + "\n")
        f.write("TABLES WITH LARGEST DIFFERENCES (Top 10)\n")
        f.write("=" * 100 + "\n\n")

        for i, report in enumerate(reports_sorted[:10], 1):
            sap_name, dremio_name = extract_table_names(report['source_table'], report['dest_table'])
            diff = report['difference']
            diff_pct = report['difference_pct']

            f.write(f"{i:2}. {sap_name} → {dremio_name}\n")
            f.write(f"    SAP: {report['source_count']:,} | Dremio: {report['dest_count']:,} | Diff: {diff:+,} ({diff_pct:+.2f}%)\n\n")

        f.write("=" * 100 + "\n")
        f.write("PERFECT MATCHES (0 difference)\n")
        f.write("=" * 100 + "\n\n")

        perfect_reports = [r for r in reports_sorted if r['difference'] == 0]
        if perfect_reports:
            f.write(f"Total: {len(perfect_reports)} tables\n\n")
            for report in perfect_reports:
                sap_name, dremio_name = extract_table_names(report['source_table'], report['dest_table'])
                f.write(f"  ✓ {sap_name:<40} → {dremio_name:<40} ({report['source_count']:,} rows)\n")
        else:
            f.write("  No perfect matches found.\n")

        f.write("\n" + "=" * 100 + "\n")
        f.write("END OF REPORT\n")
        f.write("=" * 100 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Generate summary report from bulk validation results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Summarize results by date
  python scripts/summarize_validation_results.py --date 2025-11-11

  # Summarize results by path
  python scripts/summarize_validation_results.py --path reports/sap/2025/11/11

  # Custom output file
  python scripts/summarize_validation_results.py --date 2025-11-11 --output my_summary.txt
        """
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--date', '-d',
        help='Filter date (YYYY-MM-DD) - will look in reports/sap/YYYY/MM/DD'
    )
    group.add_argument(
        '--path', '-p',
        help='Direct path to reports directory'
    )

    parser.add_argument(
        '--output', '-o',
        help='Output file path (default: summary_YYYY-MM-DD.txt)'
    )

    args = parser.parse_args()

    # Determine report directory
    if args.date:
        try:
            date_obj = datetime.strptime(args.date, '%Y-%m-%d')
            year = date_obj.strftime('%Y')
            month = date_obj.strftime('%m')
            day = date_obj.strftime('%d')
            report_dir = Path('reports/sap') / year / month / day
            filter_date = args.date
        except ValueError:
            print(f"❌ Invalid date format: {args.date}. Expected YYYY-MM-DD")
            sys.exit(1)
    else:
        report_dir = Path(args.path)
        filter_date = None

    # Validate directory exists
    if not report_dir.exists():
        print(f"❌ Directory not found: {report_dir}")
        sys.exit(1)

    # Find JSON files
    print(f"📂 Searching for validation reports in: {report_dir}")
    json_files = find_json_files(report_dir)

    if len(json_files) == 0:
        print(f"❌ No validation JSON files found in {report_dir}")
        sys.exit(1)

    print(f"📊 Found {len(json_files)} validation reports")

    # Parse reports
    print("🔍 Parsing validation reports...")
    reports = []
    for json_file in json_files:
        report = parse_validation_report(json_file)
        if report:
            reports.append(report)

    if len(reports) == 0:
        print("❌ No valid reports could be parsed")
        sys.exit(1)

    print(f"✅ Parsed {len(reports)} valid reports")

    # Determine output file
    if args.output:
        output_file = Path(args.output)
    else:
        if filter_date:
            output_file = Path(f"summary_{filter_date.replace('-', '_')}.txt")
        else:
            output_file = Path("summary_validation_results.txt")

    # Generate summary
    print(f"\n📝 Generating summary report...")
    generate_summary_report(reports, output_file, filter_date)

    print(f"✅ Summary report saved to: {output_file}")
    print(f"\n📄 Report contents:\n")

    # Print report to console
    with open(output_file, 'r') as f:
        print(f.read())


if __name__ == '__main__':
    main()
