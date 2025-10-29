"""Command-line interface for statistical validator."""

import click
import sys
from pathlib import Path
from typing import Optional, List
from .utils.config_loader import ConfigLoader
from .utils.logger import setup_logging, get_logger
from .connectors.dremio_connector import DremioConnector
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
    Compare two tables using statistical validation.
    
    Examples:
    
        # Basic comparison
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
        
        # Connect to Dremio
        click.echo("Connecting to Dremio...")
        dremio_config = config_loader.get_dremio_config()
        connector = DremioConnector(**dremio_config)
        
        # Run comparison
        click.echo(f"\nComparing tables:")
        click.echo(f"  Source: {source_table}")
        click.echo(f"  Destination: {dest_table}")
        
        comparator = TableComparator(connector, app_config)
        
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


@cli.command()
@click.argument('table_name')
@click.option('--env', '-e', help='Path to .env file')
def inspect(table_name: str, env: Optional[str]):
    """
    Inspect a table's schema and basic statistics.
    
    Example:
        stat-validator inspect schema.table_name
    """
    try:
        click.echo(f"\n🔍 Inspecting table: {table_name}\n")
        
        # Load config
        config_loader = ConfigLoader(env_path=env)
        dremio_config = config_loader.get_dremio_config()
        
        # Connect
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

# Connection Options
DREMIO_TLS=true
DREMIO_DISABLE_SERVER_VERIFICATION=true

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
        click.echo("  3. Run: stat-validator compare <source_table> <dest_table>")
        click.echo()
    
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)
        sys.exit(1)


@cli.command()
def test_connection():
    """Test connection to Dremio."""
    try:
        click.echo("\n🔌 Testing Dremio connection...\n")
        
        config_loader = ConfigLoader()
        dremio_config = config_loader.get_dremio_config()
        
        click.echo(f"Hostname: {dremio_config['hostname']}")
        click.echo(f"Port: {dremio_config['flightport']}")
        click.echo(f"TLS: {dremio_config['tls']}")
        
        connector = DremioConnector(**dremio_config)
        
        # Try a simple query
        result = connector.execute_query("SELECT 1 as test")
        
        click.echo("\n✅ Connection successful!")
        click.echo(f"Test query result: {result.to_pandas()['test'].iloc[0]}")
        click.echo()
    
    except Exception as e:
        click.echo(f"\n❌ Connection failed: {str(e)}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()
