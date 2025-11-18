# Bulk Validation Scripts

## bulk_validate_sapisu.py

Automates validation of all SAPISU tables listed in `sapisu_tables.csv`.

### Features

- ✅ Reads table mappings from CSV
- ✅ Runs `compare-cross` validation for each table
- ✅ Organizes outputs by date: `reports/sap/YYYY/MM/DD/` and `logs/sap/YYYY/MM/DD/`
- ✅ Individual log file per table
- ✅ Summary JSON with all results
- ✅ Sequential or parallel execution
- ✅ Timeout protection (1 hour per table)

### Usage

```bash
# Basic usage - validate all tables for a specific date
python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18

# Run in parallel (4 workers)
python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --parallel 4

# Use custom CSV file
python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --csv custom_tables.csv

# Use different Dremio prefix
python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --dremio-prefix ulysses2
```

### Output Structure

```
reports/sap/2025/10/18/
  ├── validation_SAP_RISE_1_T_RISE_ADCP_to_ulysses1_sapisu_rfn_adcp_20251018_143022.html
  ├── validation_SAP_RISE_1_T_RISE_ADCP_to_ulysses1_sapisu_rfn_adcp_20251018_143022.json
  ├── validation_SAP_RISE_1_T_RISE_ADR2_to_ulysses1_sapisu_rfn_adr2_20251018_143145.html
  └── ...

logs/sap/2025/10/18/
  ├── _bulk_validation.log           # Main log file
  ├── _summary.json                   # Summary with all results
  ├── rfn_adcp.log                    # Individual table logs
  ├── rfn_adr2.log
  └── ...
```

### Summary JSON Format

```json
{
  "filter_date": "2025-10-18",
  "total_tables": 136,
  "passed": 120,
  "failed": 10,
  "errors": 6,
  "total_duration": 3456.7,
  "results": [
    {
      "table": "rfn_adcp",
      "sap_table": "\"SAP_RISE_1\".\"T_RISE_ADCP\"",
      "dremio_table": "ulysses1.sapisu.\"rfn_adcp\"",
      "status": "PASS",
      "exit_code": 0,
      "duration": 12.3,
      "start_time": "2025-10-18T14:30:00",
      "end_time": "2025-10-18T14:30:12"
    }
  ]
}
```

### CSV Format

The script expects `sapisu_tables.csv` with the following columns:

```csv
schema,Ulysses,schema.1,SAP EIM
sapisu,rfn_adcp,SAP_RISE_1,T_RISE_ADCP
sapisu,rfn_adr2,SAP_RISE_1,T_RISE_ADR2
...
```

- Column 1 (`schema`): Dremio schema name
- Column 2 (`Ulysses`): Dremio table name
- Column 3 (`schema.1`): SAP HANA schema name
- Column 4 (`SAP EIM`): SAP HANA table name

### Exit Codes

- `0`: All validations passed
- `1`: One or more validations failed or encountered errors

### Performance Tips

1. **Parallel Execution**: Use `--parallel 4` to run 4 validations simultaneously
   - Recommended for large CSV files (100+ tables)
   - Adjust based on system resources and database load

2. **Monitor Progress**: Check main log file in real-time:
   ```bash
   tail -f logs/sap/2025/10/18/_bulk_validation.log
   ```

3. **Resume Failed Tables**: Filter `_summary.json` for failed tables and create a new CSV:
   ```bash
   python scripts/filter_failed_tables.py logs/sap/2025/10/18/_summary.json > retry_tables.csv
   python scripts/bulk_validate_sapisu.py --filter-date 2025-10-18 --csv retry_tables.csv
   ```

---

## summarize_validation_results.py

Generates a human-readable summary report from bulk validation results showing row count differences between SAP HANA and Dremio.

### Features

- ✅ Reads all validation JSON reports from a directory
- ✅ Calculates overall statistics (match rate, total differences)
- ✅ Shows detailed table-by-table comparison sorted by difference
- ✅ Highlights top 10 tables with largest differences
- ✅ Lists all perfect matches (0 difference)
- ✅ Outputs to formatted text file

### Usage

```bash
# Summarize results by date
python scripts/summarize_validation_results.py --date 2025-11-11

# Summarize results by directory path
python scripts/summarize_validation_results.py --path reports/sap/2025/11/11

# Custom output file
python scripts/summarize_validation_results.py --date 2025-11-11 --output my_summary.txt
```

### Example Output

```
====================================================================================================
                              VALIDATION SUMMARY REPORT
====================================================================================================

Filter Date: 2025-11-11
Generated: 2025-11-14 12:30:45
Total Tables: 136

----------------------------------------------------------------------------------------------------
OVERALL STATISTICS
----------------------------------------------------------------------------------------------------
  Perfect Match (0 difference):        120 tables (88.2%)
  With Differences:                     16 tables (11.8%)
    - More rows in Dremio:              10 tables
    - More rows in SAP HANA:             6 tables

  Total Rows in SAP HANA:           5,234,567
  Total Rows in Dremio:             5,234,640
  Total Absolute Difference:              173
  Overall Match Rate:                   99.997%

====================================================================================================
DETAILED TABLE COMPARISON (sorted by difference)
====================================================================================================

SAP Table                           Dremio Table                        SAP Count  Dremio Count        Diff        %   Status
--------------------------------------------------------------------------------------------------------------------------------------------
T_RISE_DFKKOP                       rfn_dfkkop                             39,101        39,174         +73    +0.19% FAIL
T_RISE_ADCP                         rfn_adcp                               12,345        12,290         -55    -0.45% FAIL
T_RISE_ADR2                         rfn_adr2                               98,234        98,234           0    +0.00% MATCH
...

====================================================================================================
TABLES WITH LARGEST DIFFERENCES (Top 10)
====================================================================================================

 1. T_RISE_DFKKOP → rfn_dfkkop
    SAP: 39,101 | Dremio: 39,174 | Diff: +73 (+0.19%)

 2. T_RISE_ADCP → rfn_adcp
    SAP: 12,345 | Dremio: 12,290 | Diff: -55 (-0.45%)
...

====================================================================================================
PERFECT MATCHES (0 difference)
====================================================================================================

Total: 120 tables

  ✓ T_RISE_ADR2                              → rfn_adr2                                (98,234 rows)
  ✓ T_RISE_ADR6                              → rfn_adr6                                (45,678 rows)
...
```

### Workflow

1. **Run bulk validation**:
   ```bash
   python scripts/bulk_validate_sapisu.py --filter-date 2025-11-11
   ```

2. **Generate summary**:
   ```bash
   python scripts/summarize_validation_results.py --date 2025-11-11
   ```

3. **Review the summary** to identify tables with differences

4. **Investigate specific tables** with largest differences using `key-count`:
   ```bash
   python3 -m src.stat_validator.cli key-count \
     '"SAP_RISE_1"."T_RISE_DFKKOP"' \
     'ulysses1.sapisu."rfn_dfkkop"' \
     OPBEL \
     --filter-date 2025-11-11
   ```
