#!/usr/bin/env python3
"""Diagnostic script to identify Unicode/encoding issues in tables."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from stat_validator.utils.config_loader import ConfigLoader
from stat_validator.connectors.dremio_connector import DremioConnector
from stat_validator.connectors.hana_connector import HanaConnector
import pandas as pd


def diagnose_table(table_name: str, connector, source_name: str):
    """Diagnose a table for encoding issues."""
    print(f"\n{'='*80}")
    print(f"Diagnosing {source_name}: {table_name}")
    print(f"{'='*80}")
    
    try:
        # Get schema
        print("\n1. SCHEMA:")
        schema = connector.get_table_schema(table_name)
        print(f"{'Column':<30} {'Type':<30} {'Nullable'}")
        print("-" * 80)
        for field in schema:
            nullable = "YES" if field.nullable else "NO"
            print(f"{field.name:<30} {str(field.type):<30} {nullable}")
        
        # Get row count
        row_count = connector.get_row_count(table_name)
        print(f"\nTotal rows: {row_count:,}")
        
        # Get sample data
        print("\n2. SAMPLE DATA (first 5 rows):")
        sample_query = f"SELECT * FROM {table_name} LIMIT 5"
        result = connector.execute_query(sample_query)
        df = result.to_pandas()
        
        print(df.to_string())
        
        # Check for string columns with potential encoding issues
        print("\n3. STRING COLUMNS - Checking for encoding issues:")
        string_cols = [col for col in df.columns if df[col].dtype == 'object']
        
        if not string_cols:
            print("   No string columns found.")
            return
        
        print(f"   Found {len(string_cols)} string columns: {', '.join(string_cols)}")
        
        for col in string_cols:
            print(f"\n   Column: {col}")
            
            # Check for non-ASCII characters
            non_ascii_count = 0
            encoding_errors = []
            
            for idx, value in enumerate(df[col]):
                if pd.isna(value):
                    continue
                
                try:
                    # Try to encode/decode
                    value_str = str(value)
                    
                    # Check for non-ASCII
                    if not all(ord(char) < 128 for char in value_str):
                        non_ascii_count += 1
                    
                    # Try encoding/decoding
                    value_str.encode('utf-8').decode('utf-8')
                    
                except Exception as e:
                    encoding_errors.append({
                        'row': idx,
                        'value': repr(value)[:50],
                        'error': str(e)
                    })
            
            if non_ascii_count > 0:
                print(f"      ⚠️  Contains {non_ascii_count} non-ASCII values")
            else:
                print(f"      ✓ All ASCII")
            
            if encoding_errors:
                print(f"      ❌ {len(encoding_errors)} encoding errors:")
                for err in encoding_errors[:3]:  # Show first 3
                    print(f"         Row {err['row']}: {err['value']} - {err['error']}")
        
        # Test caching
        print("\n4. TESTING CACHE:")
        try:
            cache_query = f"SELECT * FROM {table_name} LIMIT 100"
            connector.cache_query(cache_query, "test_cache")
            print("   ✓ Caching successful!")
        except Exception as e:
            print(f"   ❌ Caching failed: {str(e)}")
            
            # Try to identify problematic column
            print("\n   Trying to identify problematic column...")
            for col in string_cols:
                try:
                    test_query = f'SELECT "{col}" FROM {table_name} LIMIT 100'
                    connector.cache_query(test_query, f"test_{col}")
                    print(f"      ✓ {col} - OK")
                except Exception as col_err:
                    print(f"      ❌ {col} - PROBLEM: {str(col_err)}")
    
    except Exception as e:
        print(f"\n❌ Error diagnosing table: {str(e)}")
        import traceback
        traceback.print_exc()


def main():
    print("=" * 80)
    print("TABLE DIAGNOSTIC TOOL")
    print("=" * 80)
    
    # Get table names from user
    hana_table = input("\nEnter HANA table name (e.g., \"SAP_RISE_1\".\"T_RISE_ADCP\"): ").strip()
    dremio_table = input("Enter Dremio table name (e.g., ulysses1.sapisu.\"rfn_adcp\"): ").strip()
    
    # Load config
    config_loader = ConfigLoader()
    
    # Connect to HANA
    print("\n🔌 Connecting to SAP HANA...")
    hana_config = config_loader.get_hana_config()
    hana_connector = HanaConnector(**hana_config)
    
    # Connect to Dremio
    print("🔌 Connecting to Dremio...")
    dremio_config = config_loader.get_dremio_config()
    dremio_connector = DremioConnector(**dremio_config)
    
    # Diagnose HANA table
    diagnose_table(hana_table, hana_connector, "HANA")
    
    # Diagnose Dremio table
    diagnose_table(dremio_table, dremio_connector, "Dremio")
    
    print("\n" + "=" * 80)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 80)


if __name__ == '__main__':
    main()