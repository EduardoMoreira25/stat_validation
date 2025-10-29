# 📊 Statistical Validator

A comprehensive Python-based statistical validation framework for comparing large datasets and ensuring data consistency across systems.

## 🎯 Features

- **Statistical Analysis**: KS-test, T-test, Chi-square, PSI for distribution comparison
- **Automated Validation**: Row count, schema, null rate, and column-level testing
- **Scalable**: Handles large datasets with sampling and DuckDB caching
- **Multi-Format Reports**: JSON, HTML, and CSV output
- **Alerting**: Email and Slack notifications
- **CLI Interface**: Easy command-line usage
- **Production-Ready**: Proper logging, configuration, and error handling

## 📋 Prerequisites

- Python 3.8+
- Access to Dremio data lake
- Dremio Personal Access Token (PAT) or username/password

## 🚀 Installation

### 1. Clone and Install
```bash
cd statistical_validation
pip install -e .
```

### 2. Initialize Project
```bash
stat-validator init
```

This creates:
- `config/` directory with default configurations
- `reports/` directory for output
- `logs/` directory for logging
- `.env.example` template

### 3. Configure Environment
```bash
cp .env.example .env
nano .env  # Fill in your Dremio credentials eg Username and PAT(Personal Access Token)
```

Required environment variables:
```bash
DREMIO_HOSTNAME=your-dremio-host.com
DREMIO_PORT=32010
DREMIO_PAT=your_personal_access_token

DREMIO_TLS=true
DREMIO_DISABLE_SERVER_VERIFICATION=true
```

## 💻 Usage

### Test Connection
```bash
stat-validator test-connection
```

### Inspect a Table
```bash
stat-validator inspect schema.table_name
```

### Compare Two Tables
```bash
# Basic comparison
stat-validator compare schema.source_table schema.dest_table

# With specific columns
stat-validator compare schema.src schema.dst -col column1 -col column2

# Custom output directory
stat-validator compare schema.src schema.dst -o ./my_reports

# Generate specific formats
stat-validator compare schema.src schema.dst -f json -f html -f csv

# Verbose output
stat-validator compare schema.src schema.dst -v
```

### Python API
```python
from stat_validator import DremioConnector, TableComparator, ConfigLoader

# Load configuration
config = ConfigLoader()

# Connect to Dremio
connector = DremioConnector.from_env()

# Run comparison
comparator = TableComparator(connector, config.get_all())
result = comparator.compare('schema.source_table', 'schema.dest_table')

# Check results
print(f"Status: {result['overall_status']}")
print(f"Tests passed: {result['summary']['passed']}/{result['summary']['total_tests']}")
```

## ⚙️ Configuration

Edit `config/config.yaml` to customize thresholds:
```yaml
thresholds:
  row_count_tolerance_pct: 0.1      # 0.1% tolerance
  ks_test_pvalue: 0.05              # 95% confidence
  psi_threshold: 0.1                # PSI threshold
  null_rate_tolerance_pct: 2.0      # 2% tolerance

sampling:
  enabled: true
  max_sample_size: 50000

categorical:
  max_cardinality_for_psi: 100
```

## 📊 Statistical Tests

### 1. **Row Count Validation**
Ensures source and destination have similar row counts within threshold.

### 2. **Schema Validation**
Compares column names and types between tables.

### 3. **Null Rate Comparison**
Validates null percentages are consistent.

### 4. **KS-Test (Kolmogorov-Smirnov)**
Tests if numerical distributions are similar.

### 5. **T-Test**
Compares means of numerical columns.

### 6. **PSI (Population Stability Index)**
Measures distribution shifts for categorical data.

### 7. **Chi-Square Test**
Tests independence of categorical distributions.

## 📈 Reports

Generated reports include:

- **JSON**: Machine-readable results
- **HTML**: Interactive web report with visualizations
- **CSV**: Spreadsheet-compatible test results

Example report structure:
```json
{
  "source_table": "schema.source",
  "dest_table": "schema.dest",
  "overall_status": "PASS",
  "summary": {
    "total_tests": 45,
    "passed": 43,
    "failed": 2
  },
  "tests": [...]
}
```

## 🔔 Alerting

Configure email and Slack alerts in `config/config.yaml`:
```yaml
alerting:
  enabled: true
  on_failure_only: true
  channels: ['email', 'slack']
```

Set environment variables:
```bash
ALERT_EMAIL=your-email@company.com

```

## 🧪 Testing
```bash
# Run tests
pytest

# With coverage
pytest --cov=stat_validator --cov-report=html

# Specific test
pytest tests/test_comparator.py
```

## 📁 Project Structure
```
statistical_validation/
├── config/              # Configuration files
├── src/stat_validator/  # Main package
│   ├── connectors/      # Dremio connector
│   ├── comparison/      # Comparison engine
│   ├── reporting/       # Reports and alerts
│   ├── utils/           # Utilities
│   └── cli.py           # CLI interface
├── tests/               # Unit tests
├── reports/             # Generated reports
└── docs/                # Documentation
```

## 🔧 Development
```bash
# Install development dependencies
pip install -e ".[dev]"

# Format code
black src/ tests/

# Lint
flake8 src/ tests/

# Type checking
mypy src/
```

## 📝 Examples

See `examples/` directory for:
- `basic_comparison.py` - Simple usage example
- `advanced_config.yaml` - Advanced configuration

## How the comparisons work

Column Classification (schema_validator.py):

Numerical: INT, DOUBLE, FLOAT, DECIMAL → runs KS-test + T-test
Categorical: VARCHAR, STRING, CHAR → runs PSI + Chi-square (if cardinality ≤ 100)
Temporal: DATE, TIMESTAMP → currently skipped (future enhancement)
