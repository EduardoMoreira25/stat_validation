"""
Basic usage example for Statistical Validator.

This script demonstrates how to:
1. Load configuration
2. Connect to Dremio
3. Run a table comparison
4. Generate reports
"""

from stat_validator import DremioConnector, TableComparator, ConfigLoader, ReportGenerator

def main():
    # Load configuration
    config = ConfigLoader()
    
    # Connect to Dremio using environment variables
    connector = DremioConnector.from_env()
    
    # Define tables to compare
    source_table = "schema.source_table"
    dest_table = "schema.dest_table"
    
    # Initialize comparator
    comparator = TableComparator(connector, config.get_all())
    
    # Run comparison
    print(f"Comparing {source_table} with {dest_table}...")
    result = comparator.compare(source_table, dest_table)
    
    # Generate reports
    report_gen = ReportGenerator(output_dir="./reports")
    report_files = report_gen.generate_report(result, formats=['json', 'html'])
    
    print(f"\n✅ Comparison complete!")
    print(f"Status: {result['overall_status']}")
    print(f"Reports generated:")
    for fmt, path in report_files.items():
        print(f"  - {fmt.upper()}: {path}")
    
    # Access specific results
    if result['overall_status'] == 'FAIL':
        failed_tests = [t for t in result['tests'] if t['status'] == 'FAIL']
        print(f"\n❌ Failed tests:")
        for test in failed_tests:
            print(f"  - {test['test_name']} on {test.get('column', 'table')}")

if __name__ == '__main__':
    main()
