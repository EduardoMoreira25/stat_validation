#!/usr/bin/env python3
"""Bulk table comparison script - processes multiple table pairs from CSV."""

import csv
import sys
import os
from pathlib import Path
from datetime import datetime
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from stat_validator.utils.config_loader import ConfigLoader
from stat_validator.utils.logger import setup_logging, get_logger
from stat_validator.connectors.dremio_connector import DremioConnector
from stat_validator.connectors.hana_connector import HanaConnector
from stat_validator.comparison.comparator import TableComparator
from stat_validator.reporting.report_generator import ReportGenerator


def load_table_pairs(csv_path: str):
    """Load table pairs from CSV file."""
    table_pairs = []
    
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Build table names
            ulysses_schema = row['schema']
            ulysses_table = row['Ulysses']
            sap_schema = row['schema.1']  # Second schema column
            sap_table = row['SAP EIM']
            
            # Format table names
            dremio_table = f'ulysses1.{ulysses_schema}."{ulysses_table}"'
            hana_table = f'"{sap_schema}"."{sap_table}"'
            
            table_pairs.append({
                'hana_table': hana_table,
                'dremio_table': dremio_table,
                'display_name': f"{sap_table} → {ulysses_table}"
            })
    
    return table_pairs


def run_bulk_comparison(csv_path: str, output_dir: str = './reports/bulk', verbose: bool = False):
    """Run comparison for all table pairs in CSV."""
    
    # Setup
    log_level = 'DEBUG' if verbose else 'INFO'
    logger = setup_logging()
    logger.setLevel(log_level)
    
    print("=" * 80)
    print("BULK TABLE COMPARISON")
    print("=" * 80)
    
    # Load table pairs
    print(f"\n📋 Loading table pairs from: {csv_path}")
    table_pairs = load_table_pairs(csv_path)
    print(f"   Found {len(table_pairs)} table pairs to compare")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Load configuration
    print("\n⚙️  Loading configuration...")
    config_loader = ConfigLoader()
    app_config = config_loader.get_all()
    
    # Connect to both sources
    print("\n🔌 Connecting to data sources...")
    print("   - SAP HANA (source)...")
    hana_config = config_loader.get_hana_config()
    source_connector = HanaConnector(**hana_config)
    
    print("   - Dremio (destination)...")
    dremio_config = config_loader.get_dremio_config()
    dest_connector = DremioConnector(**dremio_config)
    
    print("   ✅ Connections established\n")
    
    # Initialize comparator and report generator
    comparator = TableComparator(source_connector, dest_connector, app_config)
    report_gen = ReportGenerator(output_dir)
    
    # Track results
    results_summary = []
    start_time = datetime.now()
    
    # Process each table pair
    for idx, pair in enumerate(table_pairs, 1):
        print("\n" + "=" * 80)
        print(f"[{idx}/{len(table_pairs)}] {pair['display_name']}")
        print("=" * 80)
        
        try:
            # Run comparison
            result = comparator.compare(
                pair['hana_table'],
                pair['dremio_table']
            )
            
            # Generate reports (JSON and HTML)
            report_files = report_gen.generate_report(result, formats=['json', 'html'])
            
            # Track summary
            results_summary.append({
                'table_pair': pair['display_name'],
                'hana_table': pair['hana_table'],
                'dremio_table': pair['dremio_table'],
                'status': result['overall_status'],
                'total_tests': result['summary']['total_tests'],
                'passed': result['summary']['passed'],
                'failed': result['summary']['failed'],
                'warnings': result['summary']['warnings'],
                'errors': result['summary']['errors'],
                'report_html': report_files.get('html', ''),
                'report_json': report_files.get('json', '')
            })
            
            print(f"\n✅ Comparison complete: {result['overall_status']}")
            
        except Exception as e:
            print(f"\n❌ ERROR: {str(e)}")
            results_summary.append({
                'table_pair': pair['display_name'],
                'hana_table': pair['hana_table'],
                'dremio_table': pair['dremio_table'],
                'status': 'ERROR',
                'error': str(e)
            })
            
            if verbose:
                import traceback
                traceback.print_exc()
    
    # Generate summary report
    end_time = datetime.now()
    duration = end_time - start_time
    
    print("\n" + "=" * 80)
    print("BULK COMPARISON SUMMARY")
    print("=" * 80)
    print(f"Total tables processed: {len(table_pairs)}")
    print(f"Duration: {duration}")
    
    # Count statuses
    status_counts = {}
    for r in results_summary:
        status = r['status']
        status_counts[status] = status_counts.get(status, 0) + 1
    
    print(f"\nResults by status:")
    for status, count in sorted(status_counts.items()):
        print(f"  {status}: {count}")
    
    # Save summary to JSON
    summary_file = os.path.join(output_dir, f'bulk_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    with open(summary_file, 'w') as f:
        json.dump({
            'total_comparisons': len(table_pairs),
            'duration_seconds': duration.total_seconds(),
            'status_counts': status_counts,
            'results': results_summary
        }, f, indent=2, default=str)
    
    print(f"\n📊 Summary saved to: {summary_file}")
    
    # Generate HTML summary
    html_summary = generate_html_summary(results_summary, duration)
    html_summary_file = os.path.join(output_dir, f'bulk_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html')
    with open(html_summary_file, 'w') as f:
        f.write(html_summary)
    
    print(f"📊 HTML summary saved to: {html_summary_file}")
    print("\n" + "=" * 80)
    
    # Return success if no errors
    if status_counts.get('ERROR', 0) == 0 and status_counts.get('FAIL', 0) == 0:
        return 0
    else:
        return 1


def generate_html_summary(results: list, duration) -> str:
    """Generate HTML summary report."""
    
    # Count statuses
    total = len(results)
    passed = len([r for r in results if r['status'] == 'PASS'])
    failed = len([r for r in results if r['status'] == 'FAIL'])
    warnings = len([r for r in results if r['status'] == 'WARNING'])
    errors = len([r for r in results if r['status'] == 'ERROR'])
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Bulk Comparison Summary</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #007bff;
            padding-bottom: 10px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .summary-card {{
            padding: 20px;
            border-radius: 8px;
            text-align: center;
            color: white;
        }}
        .summary-card.total {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }}
        .summary-card.passed {{ background: linear-gradient(135deg, #28a745 0%, #20c997 100%); }}
        .summary-card.failed {{ background: linear-gradient(135deg, #dc3545 0%, #c82333 100%); }}
        .summary-card.warnings {{ background: linear-gradient(135deg, #ffc107 0%, #ff8c00 100%); }}
        .summary-card.errors {{ background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%); }}
        .summary-number {{
            font-size: 36px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .summary-label {{
            font-size: 14px;
            opacity: 0.9;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th {{
            background: #343a40;
            color: white;
            padding: 12px;
            text-align: left;
        }}
        td {{
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status {{
            font-weight: bold;
            padding: 4px 8px;
            border-radius: 4px;
            display: inline-block;
        }}
        .status-PASS {{ background: #d4edda; color: #155724; }}
        .status-FAIL {{ background: #f8d7da; color: #721c24; }}
        .status-WARNING {{ background: #fff3cd; color: #856404; }}
        .status-ERROR {{ background: #e2e3e5; color: #383d41; }}
        a {{ color: #007bff; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Bulk Comparison Summary</h1>
        <p>Duration: {duration}</p>
        
        <div class="summary-grid">
            <div class="summary-card total">
                <div class="summary-label">Total Comparisons</div>
                <div class="summary-number">{total}</div>
            </div>
            <div class="summary-card passed">
                <div class="summary-label">Passed</div>
                <div class="summary-number">{passed}</div>
            </div>
            <div class="summary-card failed">
                <div class="summary-label">Failed</div>
                <div class="summary-number">{failed}</div>
            </div>
            <div class="summary-card warnings">
                <div class="summary-label">Warnings</div>
                <div class="summary-number">{warnings}</div>
            </div>
            <div class="summary-card errors">
                <div class="summary-label">Errors</div>
                <div class="summary-number">{errors}</div>
            </div>
        </div>
        
        <h2>Comparison Results</h2>
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>Table Pair</th>
                    <th>Status</th>
                    <th>Tests</th>
                    <th>Passed</th>
                    <th>Failed</th>
                    <th>Report</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for idx, result in enumerate(results, 1):
        status_class = f"status-{result['status']}"
        
        if result['status'] == 'ERROR':
            tests_info = f"<td colspan='3'>{result.get('error', 'Unknown error')}</td>"
            report_link = "-"
        else:
            tests_info = f"""
                <td>{result.get('total_tests', 0)}</td>
                <td>{result.get('passed', 0)}</td>
                <td>{result.get('failed', 0)}</td>
            """
            report_link = f"<a href='{result.get('report_html', '')}' target='_blank'>View Report</a>" if result.get('report_html') else "-"
        
        html += f"""
                <tr>
                    <td>{idx}</td>
                    <td>{result['table_pair']}</td>
                    <td><span class="status {status_class}">{result['status']}</span></td>
                    {tests_info}
                    <td>{report_link}</td>
                </tr>
        """
    
    html += """
            </tbody>
        </table>
    </div>
</body>
</html>
"""
    
    return html


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Bulk table comparison from CSV')
    parser.add_argument('csv_file', help='Path to CSV file with table pairs')
    parser.add_argument('--output-dir', '-o', default='./reports/bulk', help='Output directory for reports')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Run bulk comparison
    exit_code = run_bulk_comparison(args.csv_file, args.output_dir, args.verbose)
    sys.exit(exit_code)