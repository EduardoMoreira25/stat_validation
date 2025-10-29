"""Report generation for comparison results."""

import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
from jinja2 import Template
from ..utils.logger import get_logger
import numpy as np


logger = get_logger('reporting')

# need to convert int64 in the data itself not just during JSON dump
def convert_numpy_types(obj):
    """Convert numpy types to native Python types for JSON serialization."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    return obj

class ReportGenerator:
    """Generate reports in various formats (JSON, HTML, CSV)."""
    
    def __init__(self, output_dir: str = "./reports"):
        """
        Initialize report generator.
        
        Args:
            output_dir: Directory to save reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def generate_report(
        self,
        result: Dict[str, Any],
        formats: list = ['json', 'html'],
        filename_prefix: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Generate reports in multiple formats.
        
        Args:
            result: Comparison result dictionary
            formats: List of formats to generate (json, html, csv)
            filename_prefix: Optional prefix for filename
            
        Returns:
            Dictionary mapping format to file path
        """

        # Clean result at the start
        result = convert_numpy_types(result)

        if filename_prefix is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            source = result.get('source_table', 'source').replace('.', '_')
            dest = result.get('dest_table', 'dest').replace('.', '_')
            filename_prefix = f"comparison_{source}_vs_{dest}_{timestamp}"
        
        output_files = {}
        
        if 'json' in formats:
            json_path = self._generate_json(result, filename_prefix)
            output_files['json'] = json_path
            logger.info(f"JSON report generated: {json_path}")
        
        if 'html' in formats:
            html_path = self._generate_html(result, filename_prefix)
            output_files['html'] = html_path
            logger.info(f"HTML report generated: {html_path}")
        
        if 'csv' in formats:
            csv_path = self._generate_csv(result, filename_prefix)
            output_files['csv'] = csv_path
            logger.info(f"CSV report generated: {csv_path}")
        
        return output_files
    
    def _generate_json(self, result: Dict[str, Any], prefix: str) -> str:
        """Generate JSON report."""
        filepath = self.output_dir / f"{prefix}.json"
        
        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2, default=int)  # Add default=int
        
        return str(filepath)
    
    def _generate_html(self, result: Dict[str, Any], prefix: str) -> str:
        """Generate HTML report."""
        filepath = self.output_dir / f"{prefix}.html"
        
        html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Statistical Validation Report</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            line-height: 1.6;
            color: #333;
            background: #f5f5f5;
            padding: 20px;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        h1 { color: #2c3e50; margin-bottom: 10px; }
        h2 { color: #34495e; margin-top: 30px; margin-bottom: 15px; border-bottom: 2px solid #3498db; padding-bottom: 5px; }
        h3 { color: #7f8c8d; margin-top: 20px; margin-bottom: 10px; }
        .header { margin-bottom: 30px; }
        .status {
            display: inline-block;
            padding: 8px 16px;
            border-radius: 4px;
            font-weight: bold;
            font-size: 18px;
            margin: 10px 0;
        }
        .status.PASS { background: #2ecc71; color: white; }
        .status.FAIL { background: #e74c3c; color: white; }
        .status.WARNING { background: #f39c12; color: white; }
        .metadata {
            background: #ecf0f1;
            padding: 15px;
            border-radius: 4px;
            margin: 20px 0;
        }
        .metadata p { margin: 5px 0; }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }
        .summary-card {
            background: #3498db;
            color: white;
            padding: 20px;
            border-radius: 4px;
            text-align: center;
        }
        .summary-card.fail { background: #e74c3c; }
        .summary-card.warning { background: #f39c12; }
        .summary-card.pass { background: #2ecc71; }
        .summary-card h3 { color: white; font-size: 32px; margin: 0; }
        .summary-card p { margin: 5px 0 0 0; opacity: 0.9; }
        .test-results {
            margin: 20px 0;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background: #34495e;
            color: white;
            font-weight: 600;
        }
        tr:hover { background: #f8f9fa; }
        .test-PASS { color: #2ecc71; font-weight: bold; }
        .test-FAIL { color: #e74c3c; font-weight: bold; }
        .test-WARNING { color: #f39c12; font-weight: bold; }
        .test-SKIP { color: #95a5a6; font-style: italic; }
        .test-ERROR { color: #e67e22; font-weight: bold; }
        .details {
            font-size: 0.9em;
            color: #7f8c8d;
            font-family: 'Courier New', monospace;
        }
        .footer {
            margin-top: 40px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
            text-align: center;
            color: #7f8c8d;
            font-size: 0.9em;
        }
        .details {
            font-size: 0.9em;
            color: #7f8c8d;
            max-width: 400px;
        }
        .json-details {
            background: #f8f9fa;
            padding: 10px;
            border-radius: 4px;
            border-left: 3px solid #3498db;
            overflow-x: auto;
            font-family: 'Courier New', Consolas, monospace;
            font-size: 0.85em;
            line-height: 1.4;
            margin: 0;
            white-space: pre-wrap;
            word-wrap: break-word;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Statistical Validation Report</h1>
            <div class="status {{ overall_status }}">{{ overall_status }}</div>
        </div>

        <div class="metadata">
            <p><strong>Source Table:</strong> {{ source_table }}</p>
            <p><strong>Destination Table:</strong> {{ dest_table }}</p>
            <p><strong>Timestamp:</strong> {{ timestamp }}</p>
        </div>

        <h2>Summary</h2>
        <div class="summary">
            <div class="summary-card">
                <h3>{{ summary.total_tests }}</h3>
                <p>Total Tests</p>
            </div>
            <div class="summary-card pass">
                <h3>{{ summary.passed }}</h3>
                <p>Passed</p>
            </div>
            {% if summary.failed > 0 %}
            <div class="summary-card fail">
                <h3>{{ summary.failed }}</h3>
                <p>Failed</p>
            </div>
            {% endif %}
            {% if summary.warnings > 0 %}
            <div class="summary-card warning">
                <h3>{{ summary.warnings }}</h3>
                <p>Warnings</p>
            </div>
            {% endif %}
        </div>

        <h2>Test Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Test Name</th>
                    <th>Column</th>
                    <th>Status</th>
                    <th>Details</th>
                </tr>
            </thead>
            <tbody>
                {% for test in tests %}
                <tr>
                    <td>{{ test.test_name }}</td>
                    <td>{{ test.column if test.column else '-' }}</td>
                    <td class="test-{{ test.status }}">{{ test.status }}</td>
                    <td class="details"><pre class="json-details">{{ test.details | tojson(indent=2) }}</pre></td>
                </tr>
                {% endfor %}
            </tbody>
        </table>

        {% if failed_tests %}
        <h2>❌ Failed Tests</h2>
        <table>
            <thead>
                <tr>
                    <th>Test</th>
                    <th>Column</th>
                    <th>Reason</th>
                </tr>
            </thead>
            <tbody>
                {% for test in failed_tests %}
                <tr>
                    <td>{{ test.test_name }}</td>
                    <td>{{ test.column if test.column else '-' }}</td>
                    <td>{{ test.details.get('interpretation', test.details) }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
        {% endif %}

        <div class="footer">
            <p>Generated by Statistical Validator | {{ timestamp }}</p>
        </div>
    </div>
</body>
</html>
        """
        
        template = Template(html_template)
        
        # Prepare data
        failed_tests = [t for t in result['tests'] if t['status'] == 'FAIL']
        
        html_content = template.render(
            overall_status=result['overall_status'],
            source_table=result['source_table'],
            dest_table=result['dest_table'],
            timestamp=result['timestamp'],
            summary=result['summary'],
            tests=result['tests'],
            failed_tests=failed_tests
        )
        
        with open(filepath, 'w') as f:
            f.write(html_content)
        
        return str(filepath)
    
    def _generate_csv(self, result: Dict[str, Any], prefix: str) -> str:
        """Generate CSV report of test results."""
        import csv
        
        filepath = self.output_dir / f"{prefix}.csv"
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Header
            writer.writerow(['Test Name', 'Column', 'Status', 'Details'])
            
            # Rows
            for test in result['tests']:
                writer.writerow([
                    test['test_name'],
                    test.get('column', ''),
                    test['status'],
                    json.dumps(test['details'])
                ])
        
        return str(filepath)
