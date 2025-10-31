"""Report generation for validation results."""

import json
import os
import csv
from datetime import datetime
from typing import Dict, Any, List
from ..utils.logger import get_logger


logger = get_logger('report_generator')


class ReportGenerator:
    """Generates reports in multiple formats (JSON, HTML, CSV)."""
    
    def __init__(self, output_dir: str = './reports'):
        """
        Initialize report generator.
        
        Args:
            output_dir: Directory to save reports
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Report generator initialized. Output dir: {output_dir}")
    
    def generate_report(
        self,
        result: Dict[str, Any],
        formats: List[str] = None
    ) -> Dict[str, str]:
        """
        Generate reports in specified formats.
        
        Args:
            result: Validation result dictionary
            formats: List of formats to generate ['json', 'html', 'csv']
            
        Returns:
            Dictionary mapping format to file path
        """
        if formats is None:
            formats = ['json', 'html']
        
        report_files = {}
        
        # Generate filename prefix
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        source_name = result['source_table'].replace('"', '').replace('.', '_')
        dest_name = result['dest_table'].replace('"', '').replace('.', '_')
        filename_prefix = f"validation_{source_name}_to_{dest_name}"
        
        logger.info(f"Generating reports in formats: {formats}")
        
        if 'json' in formats:
            json_path = self._generate_json(result, filename_prefix)
            report_files['json'] = json_path
        
        if 'html' in formats:
            html_path = self._generate_html_report(result, filename_prefix)
            report_files['html'] = html_path
        
        if 'csv' in formats:
            csv_path = self._generate_csv(result, filename_prefix)
            report_files['csv'] = csv_path
        
        logger.info(f"Reports generated successfully: {list(report_files.keys())}")
        return report_files
    
    def _generate_json(self, result: Dict[str, Any], filename_prefix: str) -> str:
        """Generate JSON report."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        
        logger.info(f"JSON report generated: {filepath}")
        return filepath
    
    def _generate_html_report(self, result: Dict[str, Any], filename_prefix: str) -> str:
        """Generate HTML report file."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.html"
        filepath = os.path.join(self.output_dir, filename)
        
        html_content = self._generate_html(result)
        
        with open(filepath, 'w') as f:
            f.write(html_content)
        
        logger.info(f"HTML report generated: {filepath}")
        return filepath
    
    def _generate_html(self, result: Dict[str, Any]) -> str:
        """Generate HTML report with expandable details."""
        
        # Determine status color
        status_colors = {
            'PASS': '#28a745',
            'FAIL': '#dc3545',
            'WARNING': '#ffc107',
            'ERROR': '#6c757d',
            'SKIP': '#17a2b8'
        }
        
        overall_color = status_colors.get(result['overall_status'], '#6c757d')
        
        # Generate summary section
        summary = result['summary']
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Validation Report - {result['source_table']} vs {result['dest_table']}</title>
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
            border-bottom: 3px solid {overall_color};
            padding-bottom: 10px;
        }}
        .status-badge {{
            display: inline-block;
            padding: 8px 16px;
            border-radius: 4px;
            color: white;
            font-weight: bold;
            background-color: {overall_color};
            font-size: 18px;
        }}
        .info-section {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 6px;
            margin: 20px 0;
        }}
        .info-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #dee2e6;
        }}
        .info-label {{
            font-weight: bold;
            color: #495057;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .summary-card.passed {{ background: linear-gradient(135deg, #28a745 0%, #20c997 100%); }}
        .summary-card.failed {{ background: linear-gradient(135deg, #dc3545 0%, #c82333 100%); }}
        .summary-card.warnings {{ background: linear-gradient(135deg, #ffc107 0%, #ff8c00 100%); }}
        .summary-card.errors {{ background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%); }}
        .summary-card.skipped {{ background: linear-gradient(135deg, #17a2b8 0%, #138496 100%); }}
        .summary-number {{
            font-size: 36px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .summary-label {{
            font-size: 14px;
            opacity: 0.9;
        }}
        
        /* Quick Summary Section */
        .quick-summary {{
            background: #e9ecef;
            padding: 20px;
            border-radius: 6px;
            margin: 20px 0;
        }}
        .quick-summary h2 {{
            margin-top: 0;
            color: #495057;
        }}
        .status-count {{
            display: inline-block;
            padding: 5px 12px;
            margin: 5px;
            border-radius: 4px;
            font-weight: bold;
        }}
        .status-count.pass {{ background: #28a745; color: white; }}
        .status-count.fail {{ background: #dc3545; color: white; }}
        .status-count.warning {{ background: #ffc107; color: #333; }}
        .status-count.error {{ background: #6c757d; color: white; }}
        .status-count.skip {{ background: #17a2b8; color: white; }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            background: white;
        }}
        th {{
            background: #343a40;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        td {{
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status-cell {{
            font-weight: bold;
            padding: 6px 12px;
            border-radius: 4px;
            display: inline-block;
            min-width: 80px;
            text-align: center;
        }}
        .status-PASS {{ background: #d4edda; color: #155724; }}
        .status-FAIL {{ background: #f8d7da; color: #721c24; }}
        .status-WARNING {{ background: #fff3cd; color: #856404; }}
        .status-ERROR {{ background: #e2e3e5; color: #383d41; }}
        .status-SKIP {{ background: #d1ecf1; color: #0c5460; }}
        
        /* Expandable Details */
        .details-toggle {{
            background: #007bff;
            color: white;
            border: none;
            padding: 6px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            transition: background 0.3s;
        }}
        .details-toggle:hover {{
            background: #0056b3;
        }}
        .details-content {{
            display: none;
            margin-top: 10px;
            padding: 15px;
            background: #f8f9fa;
            border-left: 4px solid #007bff;
            border-radius: 4px;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-wrap: break-word;
        }}
        .details-content.show {{
            display: block;
        }}
        
        .footer {{
            margin-top: 40px;
            padding-top: 20px;
            border-top: 2px solid #dee2e6;
            text-align: center;
            color: #6c757d;
            font-size: 12px;
        }}
    </style>
    <script>
        function toggleDetails(id) {{
            const content = document.getElementById('details-' + id);
            const button = document.getElementById('btn-' + id);
            
            if (content.classList.contains('show')) {{
                content.classList.remove('show');
                button.textContent = '▶ Show Details';
            }} else {{
                content.classList.add('show');
                button.textContent = '▼ Hide Details';
            }}
        }}
        
        function expandAll() {{
            const allDetails = document.querySelectorAll('.details-content');
            const allButtons = document.querySelectorAll('.details-toggle');
            allDetails.forEach(detail => detail.classList.add('show'));
            allButtons.forEach(btn => btn.textContent = '▼ Hide Details');
        }}
        
        function collapseAll() {{
            const allDetails = document.querySelectorAll('.details-content');
            const allButtons = document.querySelectorAll('.details-toggle');
            allDetails.forEach(detail => detail.classList.remove('show'));
            allButtons.forEach(btn => btn.textContent = '▶ Show Details');
        }}
    </script>
</head>
<body>
    <div class="container">
        <h1>📊 Data Validation Report</h1>
        <div class="status-badge">{result['overall_status']}</div>
        
        <div class="info-section">
            <div class="info-row">
                <span class="info-label">Source Table:</span>
                <span>{result['source_table']}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Destination Table:</span>
                <span>{result['dest_table']}</span>
            </div>
            <div class="info-row">
                <span class="info-label">Timestamp:</span>
                <span>{result['timestamp']}</span>
            </div>
        </div>
        
        <h2>Summary</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <div class="summary-label">Total Tests</div>
                <div class="summary-number">{summary['total_tests']}</div>
            </div>
            <div class="summary-card passed">
                <div class="summary-label">Passed</div>
                <div class="summary-number">{summary['passed']}</div>
            </div>
            <div class="summary-card failed">
                <div class="summary-label">Failed</div>
                <div class="summary-number">{summary['failed']}</div>
            </div>
            <div class="summary-card warnings">
                <div class="summary-label">Warnings</div>
                <div class="summary-number">{summary['warnings']}</div>
            </div>
            <div class="summary-card errors">
                <div class="summary-label">Errors</div>
                <div class="summary-number">{summary['errors']}</div>
            </div>
            <div class="summary-card skipped">
                <div class="summary-label">Skipped</div>
                <div class="summary-number">{summary['skipped']}</div>
            </div>
        </div>
"""
        
        # Quick Summary by Test Status
        html += """
        <div class="quick-summary">
            <h2>Quick Summary by Test Type</h2>
"""
        
        # Group by test name
        test_names = sorted(set(test['test_name'] for test in result['tests']))
        for test_name in test_names:
            test_statuses = [t for t in result['tests'] if t['test_name'] == test_name]
            status_breakdown = {}
            for t in test_statuses:
                status_breakdown[t['status']] = status_breakdown.get(t['status'], 0) + 1
            
            html += f"<div><strong>{test_name}:</strong> "
            for status in ['PASS', 'FAIL', 'WARNING', 'ERROR', 'SKIP']:
                count = status_breakdown.get(status, 0)
                if count > 0:
                    status_class = status.lower()
                    html += f'<span class="status-count {status_class}">{status}: {count}</span> '
            html += "</div>"
        
        html += """
        </div>
        
        <h2>Test Results</h2>
        <div style="margin-bottom: 15px;">
            <button onclick="expandAll()" class="details-toggle">▼ Expand All</button>
            <button onclick="collapseAll()" class="details-toggle" style="margin-left: 10px;">▶ Collapse All</button>
        </div>
        
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
"""
        
        # Add test results with expandable details
        for idx, test in enumerate(result['tests']):
            column = test.get('column', '-')
            status = test['status']
            details_json = json.dumps(test.get('details', {}), indent=2, default=str)
            
            html += f"""
                <tr>
                    <td>{test['test_name']}</td>
                    <td>{column}</td>
                    <td><span class="status-cell status-{status}">{status}</span></td>
                    <td>
                        <button class="details-toggle" id="btn-{idx}" onclick="toggleDetails({idx})">▶ Show Details</button>
                        <div class="details-content" id="details-{idx}">{details_json}</div>
                    </td>
                </tr>
"""
        
        html += """
            </tbody>
        </table>
        
        <div class="footer">
            Generated by Statistical Validation Tool
        </div>
    </div>
</body>
</html>
"""
        
        return html
    
    def _generate_csv(self, result: Dict[str, Any], filename_prefix: str) -> str:
        """Generate CSV report."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{filename_prefix}_{timestamp}.csv"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write header
            writer.writerow(['test_name', 'column', 'status', 'details'])
            
            # Write test results
            for test in result['tests']:
                writer.writerow([
                    test['test_name'],
                    test.get('column', ''),
                    test['status'],
                    json.dumps(test.get('details', {}))
                ])
        
        logger.info(f"CSV report generated: {filepath}")
        return filepath