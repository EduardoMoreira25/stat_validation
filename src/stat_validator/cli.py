"""Command-line interface for statistical validator."""

import click
import sys
from pathlib import Path
from typing import Optional, List
from .utils.config_loader import ConfigLoader
from .utils.logger import setup_logging, get_logger
from .connectors.dremio_connector import DremioConnector
from .connectors.hana_connector import HanaConnector
from .comparison.comparator import TableComparator
from .reporting.report_generator import ReportGenerator


@click.group()
@click.version_option(version='1.0.0')
def cli():
    """Statistical Validation Tool for Data Quality."""
    pass


@cli.command('compare-cross')
@click.argument('hana_table')
@click.argument('dremio_table')
@click.option('--config', '-c', help='Path to config YAML file')
@click.option('--env', '-e', help='Path to .env file')
@click.option('--columns', '-col', multiple=True, help='Specific columns to test')
@click.option('--filter-date', '-d', help='Filter date for incremental validation (YYYY-MM-DD)')
@click.option('--output-dir', '-o', default='./reports', help='Output directory for reports')
@click.option('--formats', '-f', multiple=True, default=['json', 'html'],
              help='Report formats (json, html, csv)')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def compare_cross(
    hana_table: str,
    dremio_table: str,
    config: Optional[str],
    env: Optional[str],
    columns: tuple,
    filter_date: Optional[str],
    output_dir: str,
    formats: tuple,
    verbose: bool
):
    """
    Compare HANA (source) to Dremio (destination).

    Examples:
        # Full table comparison
        stat-validator compare-cross '"SAP_RISE_1"."T_RISE_DFKKOP"' 'ulysses1.sapisu."rfn_dfkkop"' -v

        # Incremental validation (filter by date)
        stat-validator compare-cross '"SAP_RISE_1"."T_RISE_ADCP"' 'ulysses1.sapisu."rfn_adcp"' --filter-date 2025-11-04
    """
    try:
        # Setup logging
        log_level = 'DEBUG' if verbose else 'INFO'
        logger = setup_logging()
        logger.setLevel(log_level)
        
        click.echo(f"\n🔍 Statistical Validation Tool - Cross-Source Comparison")
        click.echo(f"{'='*60}\n")
        
        # Load configuration
        click.echo("Loading configuration...")
        config_loader = ConfigLoader(config_path=config, env_path=env)
        app_config = config_loader.get_all()
        
        # Connect to SAP HANA (SOURCE)
        click.echo("Connecting to SAP HANA (source)...")
        hana_config = config_loader.get_hana_config()
        source_connector = HanaConnector(**hana_config)
        
        # Connect to Dremio (DESTINATION)
        click.echo("Connecting to Dremio (destination)...")
        dremio_config = config_loader.get_dremio_config()
        dest_connector = DremioConnector(**dremio_config)
        
        # Build temporal filter WHERE clauses if filter_date is provided
        source_where = None
        dest_where = None

        if filter_date:
            click.echo(f"\n📅 Applying temporal filter: {filter_date}")
            temporal_config = app_config.get('temporal_filters', {})

            # Build SAP HANA WHERE clause
            sap_config = temporal_config.get('sap', {})
            sap_column = sap_config.get('column', 'REFRESH_DT')
            sap_template = sap_config.get('sql_template', "TO_DATE({column}) = TO_DATE('{date}')")
            source_where = sap_template.format(column=sap_column, date=filter_date)

            # Build Dremio WHERE clause
            dremio_config = temporal_config.get('dremio', {})
            dremio_column = dremio_config.get('column', 'refresh_dt')
            dremio_template = dremio_config.get('sql_template', "TO_DATE({column} / 1000) = DATE '{date}'")
            dest_where = dremio_template.format(column=dremio_column, date=filter_date)

            click.echo(f"  SAP filter: WHERE {source_where}")
            click.echo(f"  Dremio filter: WHERE {dest_where}")

        # Run cross-source comparison
        click.echo(f"\nComparing tables (Cross-Source):")
        click.echo(f"  Source (HANA): {hana_table}")
        click.echo(f"  Destination (Dremio): {dremio_table}")

        comparator = TableComparator(source_connector, dest_connector, app_config)

        columns_list = list(columns) if columns else None
        result = comparator.compare(hana_table, dremio_table, columns_list, source_where, dest_where)
        
        # Generate reports
        click.echo(f"\nGenerating reports...")
        report_gen = ReportGenerator(output_dir)
        report_files = report_gen.generate_report(result, formats=list(formats))
        
        click.echo(f"\n✅ Reports generated:")
        for fmt, path in report_files.items():
            click.echo(f"  {fmt.upper()}: {path}")
        
        # Exit with appropriate code
        if result['overall_status'] == 'FAIL':
            click.echo(f"\n❌ Validation FAILED")
            sys.exit(1)
        elif result['overall_status'] == 'WARNING':
            click.echo(f"\n⚠️  Validation completed with WARNINGS")
            sys.exit(0)
        else:
            click.echo(f"\n✅ Validation PASSED")
            sys.exit(0)
    
    except Exception as e:
        click.echo(f"\n❌ Error: {str(e)}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@cli.command('key-count')
@click.argument('hana_table')
@click.argument('dremio_table')
@click.argument('key_column')
@click.option('--config', '-c', help='Path to config YAML file')
@click.option('--env', '-e', help='Path to .env file')
@click.option('--filter-date', '-d', help='Filter date for incremental validation (YYYY-MM-DD)')
@click.option('--output-dir', '-o', default='./key_counts', help='Output directory for count results')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def key_count(
    hana_table: str,
    dremio_table: str,
    key_column: str,
    config: Optional[str],
    env: Optional[str],
    filter_date: Optional[str],
    output_dir: str,
    verbose: bool
):
    """
    Compare value counts for a key column between HANA and Dremio.

    This counts how many times each distinct value appears in the key column
    and compares the distributions between source and destination.

    Examples:
        # Compare OPBEL counts between HANA and Dremio
        stat-validator key-count '"SAP_RISE_1"."T_RISE_DFKKOP"' 'ulysses1.sapisu."rfn_dfkkop"' OPBEL --filter-date 2025-11-12

        # Compare PARTNER counts
        stat-validator key-count '"SAP_RISE_1"."T_RISE_ADCP"' 'ulysses1.sapisu."rfn_adcp"' PARTNER -d 2025-11-12
    """
    try:
        import pandas as pd
        from datetime import datetime

        # Setup logging
        log_level = 'DEBUG' if verbose else 'INFO'
        logger = setup_logging()
        logger.setLevel(log_level)

        click.echo(f"\n🔑 Key Column Count Comparison")
        click.echo(f"{'='*60}\n")
        click.echo(f"Key column: {key_column}")

        # Load configuration
        click.echo("Loading configuration...")
        config_loader = ConfigLoader(config_path=config, env_path=env)
        app_config = config_loader.get_all()

        # Connect to SAP HANA (SOURCE)
        click.echo("Connecting to SAP HANA (source)...")
        hana_config = config_loader.get_hana_config()
        source_connector = HanaConnector(**hana_config)

        # Connect to Dremio (DESTINATION)
        click.echo("Connecting to Dremio (destination)...")
        dremio_config = config_loader.get_dremio_config()
        dest_connector = DremioConnector(**dremio_config)

        # Build temporal filter WHERE clauses if filter_date is provided
        source_where = None
        dest_where = None

        if filter_date:
            click.echo(f"\n📅 Applying temporal filter: {filter_date}")
            temporal_config = app_config.get('temporal_filters', {})

            # Build SAP HANA WHERE clause
            sap_config = temporal_config.get('sap', {})
            sap_column = sap_config.get('column', 'REFRESH_DT')
            sap_template = sap_config.get('sql_template', "TO_DATE({column}) = TO_DATE('{date}')")
            source_where = sap_template.format(column=sap_column, date=filter_date)

            # Build Dremio WHERE clause
            dremio_config_filter = temporal_config.get('dremio', {})
            dremio_column = dremio_config_filter.get('column', 'refresh_dt')
            dremio_template = dremio_config_filter.get('sql_template', "TO_DATE({column} / 1000) = DATE '{date}'")
            dest_where = dremio_template.format(column=dremio_column, date=filter_date)

            click.echo(f"  SAP filter: WHERE {source_where}")
            click.echo(f"  Dremio filter: WHERE {dest_where}")

        # Build count queries
        # SAP HANA uses uppercase column names
        source_col = key_column.upper()
        # Dremio uses lowercase column names
        dest_col = key_column.lower()

        click.echo(f"\n📊 Counting distinct values...")
        click.echo(f"  HANA column: {source_col}")
        click.echo(f"  Dremio column: {dest_col}")

        # Build HANA query
        source_query = f'''
            SELECT "{source_col}", COUNT(*) as row_count
            FROM {hana_table}
        '''
        if source_where:
            source_query += f" WHERE {source_where}"
        source_query += f' GROUP BY "{source_col}" ORDER BY row_count DESC'

        # Build Dremio query
        dest_query = f'''
            SELECT "{dest_col}", COUNT(*) as row_count
            FROM {dremio_table}
        '''
        if dest_where:
            dest_query += f" WHERE {dest_where}"
        dest_query += f' GROUP BY "{dest_col}" ORDER BY row_count DESC'

        # Execute queries
        click.echo("\n  Fetching counts from HANA...")
        if verbose:
            click.echo(f"    Query: {source_query}")
        source_data = source_connector.execute_query(source_query)
        source_df = source_data.to_pandas()
        source_df.columns = ['key_value', 'hana_count']

        click.echo("  Fetching counts from Dremio...")
        if verbose:
            click.echo(f"    Query: {dest_query}")
        dest_data = dest_connector.execute_query(dest_query)
        dest_df = dest_data.to_pandas()
        dest_df.columns = ['key_value', 'dremio_count']

        # Convert key_value to string for comparison (handles different types)
        source_df['key_value'] = source_df['key_value'].astype(str)
        dest_df['key_value'] = dest_df['key_value'].astype(str)

        click.echo(f"\n📊 Results:")
        click.echo(f"  Unique values in HANA: {len(source_df):,}")
        click.echo(f"  Unique values in Dremio: {len(dest_df):,}")
        click.echo(f"  Total rows in HANA: {source_df['hana_count'].sum():,}")
        click.echo(f"  Total rows in Dremio: {dest_df['dremio_count'].sum():,}")

        # Merge the dataframes
        merged_df = pd.merge(source_df, dest_df, on='key_value', how='outer', indicator=True)
        merged_df['hana_count'] = merged_df['hana_count'].fillna(0).astype(int)
        merged_df['dremio_count'] = merged_df['dremio_count'].fillna(0).astype(int)
        merged_df['difference'] = merged_df['dremio_count'] - merged_df['hana_count']
        merged_df['match'] = merged_df['hana_count'] == merged_df['dremio_count']

        # Identify differences
        only_in_hana = merged_df[merged_df['_merge'] == 'left_only']
        only_in_dremio = merged_df[merged_df['_merge'] == 'right_only']
        in_both = merged_df[merged_df['_merge'] == 'both']
        count_mismatch = in_both[in_both['difference'] != 0]
        perfect_match = in_both[in_both['difference'] == 0]

        click.echo(f"\n🔍 Comparison Summary:")
        click.echo(f"  Values in both tables: {len(in_both):,}")
        click.echo(f"    - Perfect match (same count): {len(perfect_match):,}")
        click.echo(f"    - Count mismatch: {len(count_mismatch):,}")
        click.echo(f"  Values only in HANA: {len(only_in_hana):,}")
        click.echo(f"  Values only in Dremio: {len(only_in_dremio):,}")

        # Create output directory
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Generate filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        source_name = hana_table.replace('"', '').replace('.', '_')
        dest_name = dremio_table.replace('"', '').replace('.', '_')
        prefix = f"key_count_{source_name}_to_{dest_name}_{key_column}_{timestamp}"

        # Save full comparison
        comparison_file = Path(output_dir) / f"{prefix}_comparison.csv"
        output_df = merged_df[['key_value', 'hana_count', 'dremio_count', 'difference', 'match']].copy()
        output_df = output_df.sort_values('difference', key=abs, ascending=False)
        output_df.to_csv(comparison_file, index=False)
        click.echo(f"\n✅ Full comparison saved to: {comparison_file}")

        # Show top mismatches
        if len(count_mismatch) > 0:
            click.echo(f"\n⚠️  Top 10 count mismatches:")
            top_mismatches = count_mismatch.nlargest(10, 'difference', keep='all')[['key_value', 'hana_count', 'dremio_count', 'difference']]
            click.echo(top_mismatches.to_string(index=False))

            # Save mismatches
            mismatch_file = Path(output_dir) / f"{prefix}_mismatches.csv"
            count_mismatch[['key_value', 'hana_count', 'dremio_count', 'difference']].to_csv(mismatch_file, index=False)
            click.echo(f"\n✅ All mismatches saved to: {mismatch_file}")

        # Show values only in HANA
        if len(only_in_hana) > 0:
            click.echo(f"\n⚠️  Sample of values only in HANA (top 10 by count):")
            top_hana = only_in_hana.nlargest(10, 'hana_count', keep='all')[['key_value', 'hana_count']]
            click.echo(top_hana.to_string(index=False))

            only_hana_file = Path(output_dir) / f"{prefix}_only_in_hana.csv"
            only_in_hana[['key_value', 'hana_count']].to_csv(only_hana_file, index=False)
            click.echo(f"\n✅ Values only in HANA saved to: {only_hana_file}")

        # Show values only in Dremio
        if len(only_in_dremio) > 0:
            click.echo(f"\n⚠️  Sample of values only in Dremio (top 10 by count):")
            top_dremio = only_in_dremio.nlargest(10, 'dremio_count', keep='all')[['key_value', 'dremio_count']]
            click.echo(top_dremio.to_string(index=False))

            only_dremio_file = Path(output_dir) / f"{prefix}_only_in_dremio.csv"
            only_in_dremio[['key_value', 'dremio_count']].to_csv(only_dremio_file, index=False)
            click.echo(f"\n✅ Values only in Dremio saved to: {only_dremio_file}")

        # Save summary
        summary = {
            'source_table': hana_table,
            'dest_table': dremio_table,
            'key_column': key_column,
            'filter_date': filter_date,
            'timestamp': datetime.now().isoformat(),
            'unique_values_hana': int(len(source_df)),
            'unique_values_dremio': int(len(dest_df)),
            'total_rows_hana': int(source_df['hana_count'].sum()),
            'total_rows_dremio': int(dest_df['dremio_count'].sum()),
            'values_in_both': int(len(in_both)),
            'perfect_match_count': int(len(perfect_match)),
            'count_mismatch_count': int(len(count_mismatch)),
            'only_in_hana_count': int(len(only_in_hana)),
            'only_in_dremio_count': int(len(only_in_dremio)),
            'match_rate': float(len(perfect_match) / len(merged_df) * 100) if len(merged_df) > 0 else 0
        }

        import json
        summary_file = Path(output_dir) / f"{prefix}_summary.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)

        click.echo(f"\n📄 Summary saved to: {summary_file}")
        click.echo(f"\n✅ Key column count comparison complete!")

        # Exit with appropriate code
        if len(count_mismatch) > 0 or len(only_in_hana) > 0 or len(only_in_dremio) > 0:
            click.echo(f"\n⚠️  Differences found!")
            sys.exit(1)
        else:
            click.echo(f"\n✅ All key values match perfectly!")
            sys.exit(0)

    except Exception as e:
        click.echo(f"\n❌ Error: {str(e)}", err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    cli()