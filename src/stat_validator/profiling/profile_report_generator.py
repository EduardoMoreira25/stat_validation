"""Generate profile reports in JSON and HTML formats."""

import json
import os
from datetime import datetime
from typing import Dict, Any
from ..utils.logger import get_logger

logger = get_logger('profile_report_generator')


class ProfileReportGenerator:
    """Generate statistical profile reports in multiple formats."""

    def __init__(self, output_dir: str = './profiles'):
        """
        Initialize profile report generator.

        Args:
            output_dir: Directory to save profile reports
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Profile report generator initialized. Output dir: {output_dir}")

    def generate_report(
        self,
        profile: Dict[str, Any],
        formats: list = None
    ) -> Dict[str, str]:
        """
        Generate profile reports in specified formats.

        Args:
            profile: Profile dictionary from TableProfiler
            formats: List of formats to generate ['json', 'html']

        Returns:
            Dictionary mapping format to file path
        """
        if formats is None:
            formats = ['json', 'html']

        report_files = {}

        # Generate filename prefix
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        table_name = profile['metadata']['table_name'].replace('"', '').replace('.', '_')
        filename_prefix = f"profile_{table_name}"

        logger.info(f"Generating profile reports in formats: {formats}")

        if 'json' in formats:
            json_path = self._generate_json(profile, filename_prefix, timestamp)
            report_files['json'] = json_path

        if 'html' in formats:
            html_path = self._generate_html(profile, filename_prefix, timestamp)
            report_files['html'] = html_path

        logger.info(f"Profile reports generated successfully: {list(report_files.keys())}")
        return report_files

    def _generate_json(self, profile: Dict[str, Any], filename_prefix: str, timestamp: str) -> str:
        """Generate JSON profile report."""
        filename = f"{filename_prefix}_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, 'w') as f:
            json.dump(profile, f, indent=2, default=str)

        logger.info(f"JSON profile generated: {filepath}")
        return filepath

    def _generate_html(self, profile: Dict[str, Any], filename_prefix: str, timestamp: str) -> str:
        """Generate HTML profile report."""
        filename = f"{filename_prefix}_{timestamp}.html"
        filepath = os.path.join(self.output_dir, filename)

        html_content = self._build_html_content(profile)

        with open(filepath, 'w') as f:
            f.write(html_content)

        logger.info(f"HTML profile generated: {filepath}")
        return filepath

    def _build_html_content(self, profile: Dict[str, Any]) -> str:
        """Build HTML content for profile report."""

        metadata = profile['metadata']
        table_metrics = profile['table_metrics']
        columns = profile['columns']

        # Calculate column type distribution
        type_counts = {}
        for col in columns:
            classification = col.get('classification', 'OTHER')
            type_counts[classification] = type_counts.get(classification, 0) + 1

        html = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Profile Report - {metadata['table_name']}</title>
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
            border-bottom: 3px solid #007bff;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #495057;
            margin-top: 30px;
            border-bottom: 2px solid #dee2e6;
            padding-bottom: 8px;
        }}
        .metadata-section {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 6px;
            margin: 20px 0;
        }}
        .metadata-row {{
            display: flex;
            justify-content: space-between;
            padding: 8px 0;
            border-bottom: 1px solid #dee2e6;
        }}
        .metadata-label {{
            font-weight: bold;
            color: #495057;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
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
        .summary-card.quality {{ background: linear-gradient(135deg, #28a745 0%, #20c997 100%); }}
        .summary-card.completeness {{ background: linear-gradient(135deg, #17a2b8 0%, #138496 100%); }}
        .summary-card.rows {{ background: linear-gradient(135deg, #ffc107 0%, #ff8c00 100%); }}
        .summary-card.columns {{ background: linear-gradient(135deg, #dc3545 0%, #c82333 100%); }}
        .summary-number {{
            font-size: 36px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .summary-label {{
            font-size: 14px;
            opacity: 0.9;
        }}

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
            vertical-align: top;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .type-badge {{
            display: inline-block;
            padding: 4px 8px;
            border-radius: 4px;
            font-size: 11px;
            font-weight: bold;
        }}
        .type-NUMERICAL {{ background: #e3f2fd; color: #0d47a1; }}
        .type-CATEGORICAL {{ background: #f3e5f5; color: #4a148c; }}
        .type-TEMPORAL {{ background: #e8f5e9; color: #1b5e20; }}
        .type-OTHER {{ background: #f5f5f5; color: #616161; }}

        .details-toggle {{
            background: #007bff;
            color: white;
            border: none;
            padding: 6px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
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
        }}
        .details-content.show {{
            display: block;
        }}
        .metric-item {{
            margin: 5px 0;
        }}
        .metric-label {{
            font-weight: bold;
            color: #495057;
        }}

        .null-bar {{
            height: 20px;
            background: #e9ecef;
            border-radius: 4px;
            overflow: hidden;
            position: relative;
        }}
        .null-bar-fill {{
            height: 100%;
            background: #dc3545;
            transition: width 0.3s;
        }}
        .null-bar-text {{
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 11px;
            font-weight: bold;
            color: #333;
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
        <h1>📊 Statistical Profile Report</h1>

        <div class="metadata-section">
            <div class="metadata-row">
                <span class="metadata-label">Table:</span>
                <span>{metadata['table_name']}</span>
            </div>
            <div class="metadata-row">
                <span class="metadata-label">Database:</span>
                <span>{metadata['database']}</span>
            </div>
            <div class="metadata-row">
                <span class="metadata-label">Profiled At:</span>
                <span>{metadata['profiled_at']}</span>
            </div>
            <div class="metadata-row">
                <span class="metadata-label">Duration:</span>
                <span>{metadata['duration_seconds']}s</span>
            </div>
            <div class="metadata-row">
                <span class="metadata-label">Sample Size:</span>
                <span>{metadata['sample_size']:,} rows</span>
            </div>
        </div>

        <h2>Table Overview</h2>
        <div class="summary-grid">
            <div class="summary-card completeness">
                <div class="summary-label">Completeness</div>
                <div class="summary-number">{table_metrics['completeness_percentage']}%</div>
            </div>
            <div class="summary-card rows">
                <div class="summary-label">Total Rows</div>
                <div class="summary-number">{table_metrics['total_rows']:,}</div>
            </div>
            <div class="summary-card columns">
                <div class="summary-label">Total Columns</div>
                <div class="summary-number">{table_metrics['total_columns']}</div>
            </div>
        </div>

        <h2>Column Type Distribution</h2>
        <div class="summary-grid">
"""

        # Add type distribution cards
        for col_type, count in sorted(type_counts.items()):
            html += f"""
            <div class="summary-card">
                <div class="summary-label">{col_type}</div>
                <div class="summary-number">{count}</div>
            </div>
"""

        html += """
        </div>

        <h2>Column Details</h2>
        <div style="margin-bottom: 15px;">
            <button onclick="expandAll()" class="details-toggle">▼ Expand All</button>
            <button onclick="collapseAll()" class="details-toggle" style="margin-left: 10px;">▶ Collapse All</button>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Column Name</th>
                    <th>Type</th>
                    <th>Classification</th>
                    <th>Null Rate</th>
                    <th>Distinct</th>
                    <th>Details</th>
                </tr>
            </thead>
            <tbody>
"""

        # Add column rows
        for idx, col in enumerate(columns):
            basic_stats = col.get('basic_stats', {})
            classification = col.get('classification', 'OTHER')
            null_rate = basic_stats.get('null_rate', 0)
            distinct_count = basic_stats.get('distinct_count', 0)

            html += f"""
                <tr>
                    <td><strong>{col['name']}</strong></td>
                    <td><code>{col['type']}</code></td>
                    <td><span class="type-badge type-{classification}">{classification}</span></td>
                    <td>
                        <div class="null-bar">
                            <div class="null-bar-fill" style="width: {null_rate}%"></div>
                            <div class="null-bar-text">{null_rate}%</div>
                        </div>
                    </td>
                    <td>{distinct_count:,}</td>
                    <td>
                        <button class="details-toggle" id="btn-{idx}" onclick="toggleDetails({idx})">▶ Show Details</button>
                        <div class="details-content" id="details-{idx}">
                            {self._format_column_details(col)}
                        </div>
                    </td>
                </tr>
"""

        html += """
            </tbody>
        </table>

        <div class="footer">
            Generated by Statistical Validation Tool - Profile Module
        </div>
    </div>
</body>
</html>
"""

        return html

    def _format_column_details(self, col: Dict[str, Any]) -> str:
        """Format column details for HTML display."""
        details_html = ""

        # Basic stats
        basic_stats = col.get('basic_stats', {})
        if basic_stats:
            details_html += "<strong>Basic Statistics:</strong><br>"
            for key, value in basic_stats.items():
                details_html += f"<div class='metric-item'><span class='metric-label'>{key}:</span> {value}</div>"

        # Numerical stats
        numerical_stats = col.get('numerical_stats', {})
        if numerical_stats:
            details_html += "<br><strong>Numerical Statistics:</strong><br>"
            for key, value in numerical_stats.items():
                if value is not None:
                    details_html += f"<div class='metric-item'><span class='metric-label'>{key}:</span> {value}</div>"

        # Categorical stats
        categorical_stats = col.get('categorical_stats', {})
        if categorical_stats:
            details_html += "<br><strong>Categorical Statistics:</strong><br>"
            for key, value in categorical_stats.items():
                if key == 'top_values' and value:
                    details_html += f"<div class='metric-item'><span class='metric-label'>Top Values:</span></div>"
                    for val_item in value[:5]:  # Show top 5
                        # Better display for empty strings and NULL values
                        if val_item['value'] is None:
                            val_str = '<em style="color: #6c757d;">(NULL)</em>'
                        elif val_item['value'] == '':
                            val_str = '<em style="color: #856404;">(empty string)</em>'
                        else:
                            val_str = val_item['value']
                        details_html += f"<div style='margin-left: 20px;'>{val_str}: {val_item['count']:,} ({val_item['percentage']}%)</div>"
                elif key == 'mode':
                    # Special handling for mode to show empty strings clearly
                    if value is None:
                        mode_display = '<em style="color: #6c757d;">(NULL)</em>'
                    elif value == '':
                        mode_display = '<em style="color: #856404;">(empty string)</em>'
                    else:
                        mode_display = value
                    details_html += f"<div class='metric-item'><span class='metric-label'>{key}:</span> {mode_display}</div>"
                elif value is not None and key != 'top_values':
                    details_html += f"<div class='metric-item'><span class='metric-label'>{key}:</span> {value}</div>"

        # Boolean stats
        boolean_stats = col.get('boolean_stats', {})
        if boolean_stats:
            details_html += "<br><strong>Boolean Statistics:</strong><br>"

            # Create visual bars for True/False distribution
            true_count = boolean_stats.get('true_count', 0)
            false_count = boolean_stats.get('false_count', 0)
            null_count = boolean_stats.get('null_count', 0)
            true_pct = boolean_stats.get('true_percentage', 0)
            false_pct = boolean_stats.get('false_percentage', 0)
            null_pct = boolean_stats.get('null_percentage', 0)

            # True bar
            details_html += f"""
            <div class='metric-item' style='margin-bottom: 10px;'>
                <span class='metric-label'>✓ True:</span> {true_count:,} ({true_pct}%)
                <div style='background: #e9ecef; height: 20px; border-radius: 4px; overflow: hidden; margin-top: 4px;'>
                    <div style='background: #28a745; height: 100%; width: {true_pct}%;'></div>
                </div>
            </div>
            """

            # False bar
            details_html += f"""
            <div class='metric-item' style='margin-bottom: 10px;'>
                <span class='metric-label'>✗ False:</span> {false_count:,} ({false_pct}%)
                <div style='background: #e9ecef; height: 20px; border-radius: 4px; overflow: hidden; margin-top: 4px;'>
                    <div style='background: #dc3545; height: 100%; width: {false_pct}%;'></div>
                </div>
            </div>
            """

            # NULL bar (if any)
            if null_count > 0:
                details_html += f"""
                <div class='metric-item' style='margin-bottom: 10px;'>
                    <span class='metric-label'>∅ NULL:</span> {null_count:,} ({null_pct}%)
                    <div style='background: #e9ecef; height: 20px; border-radius: 4px; overflow: hidden; margin-top: 4px;'>
                        <div style='background: #6c757d; height: 100%; width: {null_pct}%;'></div>
                    </div>
                </div>
                """

            # True/False ratio
            true_ratio = boolean_stats.get('true_ratio', 0)
            false_ratio = boolean_stats.get('false_ratio', 0)
            details_html += f"<div class='metric-item'><span class='metric-label'>True Ratio (excl. NULL):</span> {true_ratio:.2%}</div>"
            details_html += f"<div class='metric-item'><span class='metric-label'>False Ratio (excl. NULL):</span> {false_ratio:.2%}</div>"

        # Temporal stats
        temporal_stats = col.get('temporal_stats', {})
        if temporal_stats:
            details_html += "<br><strong>Temporal Statistics:</strong><br>"
            for key, value in temporal_stats.items():
                if value is not None and key not in ['weekday_distribution', 'hour_distribution', 'gaps']:
                    details_html += f"<div class='metric-item'><span class='metric-label'>{key}:</span> {value}</div>"

        # String stats
        string_stats = col.get('string_stats', {})
        if string_stats:
            details_html += "<br><strong>String Statistics:</strong><br>"
            for key, value in string_stats.items():
                details_html += f"<div class='metric-item'><span class='metric-label'>{key}:</span> {value}</div>"

        return details_html if details_html else "No detailed metrics available"
