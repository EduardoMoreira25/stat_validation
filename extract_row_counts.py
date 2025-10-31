#!/usr/bin/env python3
"""Extract row counts from bulk validation reports and save to CSV."""

import json
import csv
import os
from pathlib import Path
from datetime import datetime


def extract_row_counts_from_reports(reports_dir='./reports/bulk', output_csv='row_counts_summary.csv'):
    """
    Extract row counts from all JSON reports in the bulk directory.
    
    Args:
        reports_dir: Directory containing bulk validation reports
        output_csv: Output CSV file path
    """
    
    print(f"📊 Extracting row counts from reports in: {reports_dir}")
    
    # Find all JSON report files
    reports_path = Path(reports_dir)
    json_files = list(reports_path.glob('validation_*.json'))
    
    if not json_files:
        print(f"❌ No validation JSON files found in {reports_dir}")
        return
    
    print(f"   Found {len(json_files)} report files")
    
    # Extract data
    row_data = []
    
    for json_file in sorted(json_files):
        try:
            with open(json_file, 'r') as f:
                report = json.load(f)
            
            # Get table names
            source_table = report.get('source_table', 'Unknown')
            dest_table = report.get('dest_table', 'Unknown')
            
            # Find row_count test
            row_count_test = None
            for test in report.get('tests', []):
                if test.get('test_name') == 'row_count':
                    row_count_test = test
                    break
            
            if row_count_test:
                details = row_count_test.get('details', {})
                source_count = details.get('source_count', 'N/A')
                dest_count = details.get('dest_count', 'N/A')
                difference = details.get('difference', 'N/A')
                ratio = details.get('ratio', 'N/A')
                status = row_count_test.get('status', 'N/A')
            else:
                source_count = 'N/A'
                dest_count = 'N/A'
                difference = 'N/A'
                ratio = 'N/A'
                status = 'N/A'
            
            row_data.append({
                'dremio_table': dest_table,
                'dremio_count': dest_count,
                'hana_table': source_table,
                'hana_count': source_count,
                'difference': difference,
                'ratio': ratio,
                'status': status
            })
            
        except Exception as e:
            print(f"   ⚠️  Error processing {json_file.name}: {str(e)}")
    
    # Write to CSV
    if row_data:
        output_path = Path(reports_dir) / output_csv
        
        with open(output_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'dremio_table',
                'dremio_count',
                'hana_table', 
                'hana_count',
                'difference',
                'ratio',
                'status'
            ])
            
            writer.writeheader()
            writer.writerows(row_data)
        
        print(f"\n✅ Row counts exported to: {output_path}")
        print(f"   Total tables: {len(row_data)}")
        
        # Print summary
        status_counts = {}
        for row in row_data:
            status = row['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        print(f"\n📈 Status Summary:")
        for status, count in sorted(status_counts.items()):
            print(f"   {status}: {count}")
    else:
        print("❌ No data extracted from reports")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Extract row counts from bulk validation reports')
    parser.add_argument('--reports-dir', '-d', default='./reports/bulk', 
                        help='Directory containing bulk reports (default: ./reports/)')
    parser.add_argument('--output', '-o', default='row_counts_summary.csv',
                        help='Output CSV filename (default: row_counts_summary.csv)')
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("ROW COUNTS EXTRACTION")
    print("=" * 80 + "\n")
    
    extract_row_counts_from_reports(args.reports_dir, args.output)
    
    print("\n" + "=" * 80)


if __name__ == '__main__':
    main()