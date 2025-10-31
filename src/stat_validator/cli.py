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
from .reporting.alerting import AlertManager


@click.group()
@click.version_option(version='1.0.0')
def cli():
    """Statistical Validation Tool for Data Quality."""
    pass


@cli.command()
@click.argument('source_table')
@click.argument('dest_table')
@click.option('--config', '-c', help='Path to config YAML file')
@click.option('--env', '-e', help='Path to .env file')
@click.option('--columns', '-col', multiple=True, help='Specific columns to test')
@click.option('--output-dir', '-o', default='./reports', help='Output directory for reports')
@click.option('--formats', '-f', multiple=True, default=['json', 'html'], 
              help='Report formats (json, html, csv)')
@click.option('--no-cache', is_flag=True, help='Skip DuckDB caching (direct queries)')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
def compare(
    source_table: str,
    dest_table: str,
    config: Optional[str],
    env: Optional[str],
    columns: tuple,
    output_dir: str,
    formats: tuple,
    no_cache: bool,
    verbose: bool
):
    """
    Compare two tables using statistical validation (same data source).
    
    Examples:
    
        # Basic comparison (Dremio to Dremio)
        stat-validator compare schema.source_table schema.dest_table
        
        # With specific columns
        stat-validator compare schema.source_table schema.dest_table -col col1 -col col2
        
        # Custom config and output
        stat-validator compare schema.src schema.dst -c custom_config.yaml -o ./my_reports
    """
    try:
        # Setup logging
        log_level = 'DEBUG' if verbose else 'INFO'
        logger = setup_logging()
        logger.setLevel(log_level)
        
        click.echo(f"\n🔍 Statistical Validation Tool")
        click.echo(f"{'='*60}\n")
        
        # Load configuration
        click.echo("Loading configuration...")
        config_loader = ConfigLoader(config_path=config, env_path=env)
        app_config = config_loader.get_all()
        
        # Connect to Dremio (default)
        click.echo("Connecting to Dremio...")
        dremio_config = config_loader.get_dremio_config()
        connector = DremioConnector(**dremio_config)
        
        # Run comparison (same connector for both source and dest)
        click.echo(f"\nComparing tables:")
        click.echo(f"  Source: {source_table}")
        click.echo(f"  Destination: {dest_table}")
        
        comparator = TableComparator(connector, connector, app_config)
        
        columns_list = list(columns) if columns else None
        result = comparator.compare(source_table, dest_table, columns_list)
        
        # Generate reports
        click.echo(f"\nGenerating reports...")
        report_gen = ReportGenerator(output_dir)
        report_files = report_gen.generate_report(result, formats=list(formats))
        
        click.echo(f"\n✅ Reports generated:")
        for fmt, path in report_files.items():
            click.echo(f"  {fmt.upper()}: {path}")
        
        # Send alerts if configured
        alerting_config = app_config.get('alerting', {})
        if alerting_config.get('enabled', False):
            click.echo(f"\nSending alerts...")
            alert_manager = AlertManager(alerting_config)
            alert_manager.send_alert(result, report_files)
        
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


@cli.command('compare-cross')
@click.argument('hana_table')
@click.argument('dremio_table')
@click.option('--config', '-c', help='Path to config YAML file')
@click.option('--env', '-e', help='Path to .env file')
@click.option('--columns', '-col', multiple=True, help='Specific columns to test')
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
    output_dir: str,
    formats: tuple,
    verbose: bool
):
    """
    Compare HANA (source) to Dremio (destination).
    
    Examples:
        stat-validator compare-cross '"SAP_RISE_1"."T_RISE_DFKKOP"' 'ulysses1.sapisu."rfn_dfkkop"' -v
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
        
        # Run cross-source comparison
        click.echo(f"\nComparing tables (Cross-Source):")
        click.echo(f"  Source (HANA): {hana_table}")
        click.echo(f"  Destination (Dremio): {dremio_table}")
        
        comparator = TableComparator(source_connector, dest_connector, app_config)
        
        columns_list = list(columns) if columns else None
        result = comparator.compare(hana_table, dremio_table, columns_list)
        
        # Generate reports
        click.echo(f"\nGenerating reports...")
        report_gen = ReportGenerator(output_dir)
        report_files = report_gen.generate_report(result, formats=list(formats))
        
        click.echo(f"\n✅ Reports generated:")
        for fmt, path in report_files.items():
            click.echo(f"  {fmt.upper()}: {path}")
        
        # Send alerts if configured
        alerting_config = app_config.get('alerting', {})
        if alerting_config.get('enabled', False):
            click.echo(f"\nSending alerts...")
            alert_manager = AlertManager(alerting_config)
            alert_manager.send_alert(result, report_files)
        
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


@cli.command()
@click.argument('table_name')
@click.option('--env', '-e', help='Path to .env file')
@click.option('--source', '-s', type=click.Choice(['dremio', 'hana']), default='dremio',
              help='Data source type (default: dremio)')
def inspect(table_name: str, env: Optional[str], source: str):
    """
    Inspect a table's schema and basic statistics.
    
    Example:
        stat-validator inspect schema.table_name
        stat-validator inspect '"SCHEMA"."TABLE"' --source hana
    """
    try:
        click.echo(f"\n🔍 Inspecting table: {table_name}\n")
        
        # Load config
        config_loader = ConfigLoader(env_path=env)
        
        # Connect based on source type
        if source == 'hana':
            hana_config = config_loader.get_hana_config()
            connector = HanaConnector(**hana_config)
        else:
            dremio_config = config_loader.get_dremio_config()
            connector = DremioConnector(**dremio_config)
        
        # Get schema
        schema = connector.get_table_schema(table_name)
        row_count = connector.get_row_count(table_name)
        
        click.echo(f"Row Count: {row_count:,}\n")
        click.echo("Schema:")
        click.echo(f"{'Column':<30} {'Type':<20}")
        click.echo("-" * 50)
        
        for field in schema:
            click.echo(f"{field.name:<30} {str(field.type):<20}")
        
        click.echo()
    
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
def init():
    """
    Initialize a new validation project (create config files).
    """
    try:
        click.echo("\n🚀 Initializing Statistical Validator project...\n")
        
        # Create directories
        Path("config").mkdir(exist_ok=True)
        Path("reports").mkdir(exist_ok=True)
        Path("logs").mkdir(exist_ok=True)
        
        # Create .env.example if it doesn't exist
        env_example = Path(".env.example")
        if not env_example.exists():
            env_content = """# Dremio Connection Settings
DREMIO_HOSTNAME=your-dremio-hostname.com
DREMIO_PORT=32010
DREMIO_USERNAME=your_username
DREMIO_PASSWORD=your_password
DREMIO_PAT=your_personal_access_token_here
DREMIO_TLS=true
DREMIO_DISABLE_SERVER_VERIFICATION=true

# SAP HANA Connection Settings
HANA_HOST=your-hana-hostname.com
HANA_PORT=30015
HANA_USER=your_username
HANA_PASSWORD=your_password
HANA_SCHEMA=your_default_schema
HANA_ENCRYPT=true
HANA_SSL_VALIDATE=false

# DuckDB Cache
DUCKDB_CACHE_PATH=_validation_cache.duckdb

# Reporting/Alerting (optional)
ALERT_EMAIL=your-email@company.com
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@company.com
SMTP_PASSWORD=your-app-password

# Output Settings
REPORTS_DIR=./reports
LOG_LEVEL=INFO
"""
            env_example.write_text(env_content)
            click.echo("✅ Created .env.example")
        
        click.echo("✅ Created directories: config/, reports/, logs/")
        click.echo("\n📝 Next steps:")
        click.echo("  1. Copy .env.example to .env and fill in your credentials")
        click.echo("  2. Review config/config.yaml for threshold settings")
        click.echo("  3. Run: stat-validator compare-cross <hana_table> <dremio_table>")
        click.echo()
    
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--source', '-s', type=click.Choice(['dremio', 'hana', 'both']), default='both',
              help='Which connection to test (default: both)')
def test_connection(source: str):
    """Test connection to data sources."""
    try:
        config_loader = ConfigLoader()
        
        if source in ['dremio', 'both']:
            click.echo("\n🔌 Testing Dremio connection...\n")
            dremio_config = config_loader.get_dremio_config()
            click.echo(f"Hostname: {dremio_config['hostname']}")
            click.echo(f"Port: {dremio_config['flightport']}")
            click.echo(f"TLS: {dremio_config['tls']}")
            
            try:
                connector = DremioConnector(**dremio_config)
                result = connector.execute_query("SELECT 1 as test")
                click.echo("✅ Dremio connection successful!")
                click.echo(f"Test query result: {result.to_pandas()['test'].iloc[0]}\n")
            except Exception as e:
                click.echo(f"❌ Dremio connection failed: {str(e)}\n")
        
        if source in ['hana', 'both']:
            click.echo("🔌 Testing SAP HANA connection...\n")
            hana_config = config_loader.get_hana_config()
            click.echo(f"Hostname: {hana_config['hostname']}")
            click.echo(f"Port: {hana_config['port']}")
            click.echo(f"Encrypt: {hana_config['encrypt']}")
            
            try:
                connector = HanaConnector(**hana_config)
                result = connector.execute_query("SELECT 1 as test FROM DUMMY")
                click.echo("✅ SAP HANA connection successful!")
                click.echo(f"Test query result: {result.to_pandas()['test'].iloc[0]}\n")
            except Exception as e:
                click.echo(f"❌ SAP HANA connection failed: {str(e)}\n")
    
    except Exception as e:
        click.echo(f"\n❌ Error: {str(e)}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()