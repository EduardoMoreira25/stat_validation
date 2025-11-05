#!/usr/bin/env python3
"""Bulk table profiling script - profiles multiple tables and generates reports.
# Profile just a few tables by name
python3 bulk_profiling.py \
    --schema ulysses1.sapisu \
    --tables rfn_adcp rfn_adr2 rfn_adr6 \
    --source dremio \
    --sample-size 10000 \
    -o ./profiles/rfn_test
    
2. Profile All rfn_ Tables (Auto-Discovery)*
# Discovers and profiles ALL 172 rfn_* tables
python3 bulk_profiling.py \
    --schema ulysses1.sapisu \
    --pattern 'rfn_%' \
    --source dremio \
    --sample-size 10000 \
    -o ./profiles/all_rfn_tables

# Note: This will take ~2-3 hours for 172 tables
# (172 tables × ~40 seconds average = 114 minutes)

3. Profile Subset with Pattern
# Profile only rfn_a* tables (tables starting with rfn_a)
python3 bulk_profiling.py \
    --schema ulysses1.sapisu \
    --pattern 'rfn_a%' \
    --source dremio \
    -o ./profiles/rfn_a_tables

4. Smaller Sample for Faster Profiling
# Use 5,000 rows instead of 50,000 for faster results
python3 bulk_profiling.py \
    --schema ulysses1.sapisu \
    --pattern 'rfn_%' \
    --sample-size 5000 \
    -o ./profiles/quick_scan
"""

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
from stat_validator.profiling.profiler import TableProfiler
from stat_validator.profiling.profile_report_generator import ProfileReportGenerator


def discover_tables(connector, schema_pattern: str, table_pattern: str = None):
    """
    Discover tables in Dremio that match the given patterns.

    Args:
        connector: Dremio connector
        schema_pattern: Schema path (e.g., 'ulysses1.sapisu')
        table_pattern: SQL LIKE pattern for table names (e.g., 'rfn_%')

    Returns:
        List of table names
    """
    logger = get_logger('bulk_profiling')

    # Query to list tables in Dremio
    # Using INFORMATION_SCHEMA.TABLES
    if table_pattern:
        query = f"""
        SELECT table_name
        FROM INFORMATION_SCHEMA."TABLES"
        WHERE table_schema = '{schema_pattern}'
          AND table_name LIKE '{table_pattern}'
        ORDER BY table_name
        """
    else:
        query = f"""
        SELECT table_name
        FROM INFORMATION_SCHEMA."TABLES"
        WHERE table_schema = '{schema_pattern}'
        ORDER BY table_name
        """

    try:
        result = connector.execute_query(query)
        tables = result.to_pandas()['table_name'].tolist()
        logger.info(f"Discovered {len(tables)} tables matching pattern '{table_pattern or '*'}'")
        return tables
    except Exception as e:
        logger.error(f"Failed to discover tables: {e}")
        logger.info("Falling back to manual table construction...")
        # Fallback: Return empty list if discovery fails
        return []


def profile_tables_bulk(
    connector,
    schema: str,
    table_names: list,
    output_dir: str = './profiles/bulk',
    sample_size: int = 50000,
    verbose: bool = False
):
    """
    Profile multiple tables and generate reports.

    Args:
        connector: Database connector
        schema: Schema name (e.g., 'ulysses1.sapisu')
        table_names: List of table names to profile
        output_dir: Output directory for profiles
        sample_size: Number of rows to sample per table
        verbose: Verbose logging
    """
    # Setup
    log_level = 'DEBUG' if verbose else 'INFO'
    logger = setup_logging(default_level=log_level if verbose else 20)

    print("=" * 80)
    print("BULK TABLE PROFILING")
    print("=" * 80)
    print(f"\nSchema: {schema}")
    print(f"Tables to profile: {len(table_names)}")
    print(f"Sample size: {sample_size:,} rows per table")
    print(f"Output directory: {output_dir}\n")

    # Create output directory
    os.makedirs(output_dir, exist_ok=True)

    # Initialize profiler and report generator
    profiler = TableProfiler(connector=connector, sample_size=sample_size)
    report_gen = ProfileReportGenerator(output_dir)

    # Track results
    results_summary = []
    start_time = datetime.now()

    # Process each table
    for idx, table_name in enumerate(table_names, 1):
        print("\n" + "=" * 80)
        print(f"[{idx}/{len(table_names)}] Profiling: {schema}.{table_name}")
        print("=" * 80)

        # Build full table name
        full_table_name = f'{schema}."{table_name}"'

        try:
            # Profile the table
            profile_start = datetime.now()
            profile = profiler.profile_table(full_table_name)
            profile_duration = (datetime.now() - profile_start).total_seconds()

            # Generate reports
            print("\nGenerating reports...")
            report_files = report_gen.generate_report(profile, formats=['json', 'html'])

            # Track summary
            results_summary.append({
                'table_name': table_name,
                'full_table_name': full_table_name,
                'status': 'SUCCESS',
                'row_count': profile['metadata']['row_count'],
                'column_count': profile['metadata']['column_count'],
                'completeness': profile['table_metrics']['completeness_percentage'],
                'duration_seconds': profile_duration,
                'report_html': os.path.basename(report_files.get('html', '')),
                'report_json': os.path.basename(report_files.get('json', ''))
            })

            print(f"\n✅ Profile completed successfully")
            print(f"   Completeness: {profile['table_metrics']['completeness_percentage']}%")
            print(f"   Duration: {profile_duration:.1f}s")

        except Exception as e:
            print(f"\n❌ ERROR: {str(e)}")
            results_summary.append({
                'table_name': table_name,
                'full_table_name': full_table_name,
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
    print("BULK PROFILING SUMMARY")
    print("=" * 80)
    print(f"Total tables processed: {len(table_names)}")
    print(f"Total duration: {duration}")

    # Count statuses
    successful = len([r for r in results_summary if r['status'] == 'SUCCESS'])
    failed = len([r for r in results_summary if r['status'] == 'ERROR'])

    print(f"\nResults:")
    print(f"  ✅ Successful: {successful}")
    print(f"  ❌ Failed: {failed}")

    # Calculate average completeness for successful profiles
    if successful > 0:
        avg_completeness = sum(r['completeness'] for r in results_summary if r['status'] == 'SUCCESS') / successful
        print(f"\nAverage Metrics (across successful profiles):")
        print(f"  Completeness: {avg_completeness:.1f}%")

    # Save summary to JSON
    summary_file = os.path.join(output_dir, f'profiling_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    with open(summary_file, 'w') as f:
        json.dump({
            'schema': schema,
            'total_tables': len(table_names),
            'duration_seconds': duration.total_seconds(),
            'successful': successful,
            'failed': failed,
            'results': results_summary
        }, f, indent=2, default=str)

    print(f"\n📊 Summary saved to: {summary_file}")

    # Generate HTML summary
    html_summary = generate_html_summary(results_summary, duration, schema)
    html_summary_file = os.path.join(output_dir, f'profiling_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.html')
    with open(html_summary_file, 'w') as f:
        f.write(html_summary)

    print(f"📊 HTML summary saved to: {html_summary_file}")
    print("\n" + "=" * 80)

    # Return success if no errors
    return 0 if failed == 0 else 1


def generate_html_summary(results: list, duration, schema: str) -> str:
    """Generate HTML summary report for bulk profiling."""

    # Count statuses
    total = len(results)
    successful = len([r for r in results if r['status'] == 'SUCCESS'])
    failed = len([r for r in results if r['status'] == 'ERROR'])

    # Calculate averages
    avg_completeness = 0
    if successful > 0:
        avg_completeness = sum(r['completeness'] for r in results if r['status'] == 'SUCCESS') / successful

    html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Bulk Profiling Summary - {schema}</title>
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
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
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
        .summary-card.successful {{ background: linear-gradient(135deg, #28a745 0%, #20c997 100%); }}
        .summary-card.failed {{ background: linear-gradient(135deg, #dc3545 0%, #c82333 100%); }}
        .summary-card.quality {{ background: linear-gradient(135deg, #17a2b8 0%, #138496 100%); }}
        .summary-card.completeness {{ background: linear-gradient(135deg, #ffc107 0%, #ff8c00 100%); }}
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
        .status-SUCCESS {{ background: #d4edda; color: #155724; }}
        .status-ERROR {{ background: #f8d7da; color: #721c24; }}
        a {{ color: #007bff; text-decoration: none; }}
        a:hover {{ text-decoration: underline; }}
        .quality-bar {{
            height: 20px;
            background: #e9ecef;
            border-radius: 4px;
            overflow: hidden;
            position: relative;
        }}
        .quality-bar-fill {{
            height: 100%;
            transition: width 0.3s;
        }}
        .quality-good {{ background: #28a745; }}
        .quality-fair {{ background: #ffc107; }}
        .quality-poor {{ background: #dc3545; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Bulk Profiling Summary</h1>
        <p><strong>Schema:</strong> {schema}</p>
        <p><strong>Duration:</strong> {duration}</p>

        <div class="summary-grid">
            <div class="summary-card total">
                <div class="summary-label">Total Tables</div>
                <div class="summary-number">{total}</div>
            </div>
            <div class="summary-card successful">
                <div class="summary-label">Successful</div>
                <div class="summary-number">{successful}</div>
            </div>
            <div class="summary-card failed">
                <div class="summary-label">Failed</div>
                <div class="summary-number">{failed}</div>
            </div>
            <div class="summary-card completeness">
                <div class="summary-label">Avg Completeness</div>
                <div class="summary-number">{avg_completeness:.1f}%</div>
            </div>
        </div>

        <h2>Profiling Results</h2>
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>Table Name</th>
                    <th>Status</th>
                    <th>Rows</th>
                    <th>Columns</th>
                    <th>Completeness</th>
                    <th>Duration</th>
                    <th>Report</th>
                </tr>
            </thead>
            <tbody>
"""

    for idx, result in enumerate(results, 1):
        status_class = f"status-{result['status']}"

        if result['status'] == 'ERROR':
            info_cells = f"""
                <td colspan='4'>{result.get('error', 'Unknown error')}</td>
                <td>-</td>
            """
        else:
            info_cells = f"""
                <td>{result.get('row_count', 0):,}</td>
                <td>{result.get('column_count', 0)}</td>
                <td>{result.get('completeness', 0):.1f}%</td>
                <td>{result.get('duration_seconds', 0):.1f}s</td>
                <td><a href='{result.get('report_html', '')}' target='_blank'>View Report</a></td>
            """

        html += f"""
                <tr>
                    <td>{idx}</td>
                    <td><strong>{result['table_name']}</strong></td>
                    <td><span class="status {status_class}">{result['status']}</span></td>
                    {info_cells}
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

    parser = argparse.ArgumentParser(description='Bulk table profiling - discover and profile multiple tables')
    parser.add_argument('--schema', '-s', required=True, help='Schema path (e.g., ulysses1.sapisu)')
    parser.add_argument('--pattern', '-p', default='rfn_%', help='Table name pattern (SQL LIKE syntax, e.g., rfn_%%)')
    parser.add_argument('--tables', '-t', nargs='+', help='Explicit list of table names (overrides --pattern)')
    parser.add_argument('--output-dir', '-o', default='./profiles/bulk', help='Output directory for profiles')
    parser.add_argument('--sample-size', default=50000, type=int, help='Number of rows to sample per table')
    parser.add_argument('--source', default='dremio', choices=['dremio', 'hana'], help='Data source')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')

    args = parser.parse_args()

    # Load configuration
    config_loader = ConfigLoader()

    # Connect to data source
    print(f"\n🔌 Connecting to {args.source.upper()}...")
    if args.source == 'hana':
        config = config_loader.get_hana_config()
        connector = HanaConnector(**config)
    else:
        config = config_loader.get_dremio_config()
        connector = DremioConnector(**config)

    print("✅ Connected\n")

    # Get list of tables
    if args.tables:
        # Use explicit list
        table_names = args.tables
        print(f"Using explicit table list: {len(table_names)} tables")
    else:
        # Discover tables
        print(f"Discovering tables matching pattern '{args.pattern}' in schema '{args.schema}'...")
        table_names = discover_tables(connector, args.schema, args.pattern)

        if not table_names:
            print(f"\n⚠️  No tables found. Please check your schema and pattern.")
            sys.exit(1)

    print(f"Found {len(table_names)} tables to profile:\n")
    for i, table in enumerate(table_names[:10], 1):
        print(f"  {i}. {table}")
    if len(table_names) > 10:
        print(f"  ... and {len(table_names) - 10} more")

    # Confirm
    print(f"\n⚠️  This will profile {len(table_names)} tables (may take a while)")
    response = input("Continue? [y/N]: ").strip().lower()
    if response not in ['y', 'yes']:
        print("Aborted.")
        sys.exit(0)

    # Run bulk profiling
    exit_code = profile_tables_bulk(
        connector=connector,
        schema=args.schema,
        table_names=table_names,
        output_dir=args.output_dir,
        sample_size=args.sample_size,
        verbose=args.verbose
    )

    sys.exit(exit_code)
