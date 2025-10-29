"""Alerting module for sending notifications."""

import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, Any, Optional, List
import requests
from ..utils.logger import get_logger


logger = get_logger('alerting')


class AlertManager:
    """Manage alerts via email and Slack."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize alert manager.
        
        Args:
            config: Configuration dictionary with alerting settings
        """
        self.config = config
        self.enabled = config.get('enabled', False)
        self.on_failure_only = config.get('on_failure_only', True)
        self.channels = config.get('channels', [])
    
    def send_alert(
        self,
        result: Dict[str, Any],
        report_files: Optional[Dict[str, str]] = None
    ):
        """
        Send alert based on configuration.
        
        Args:
            result: Comparison result dictionary
            report_files: Dictionary of generated report file paths
        """
        if not self.enabled:
            logger.debug("Alerting is disabled")
            return
        
        # Check if we should alert
        if self.on_failure_only and result['overall_status'] == 'PASS':
            logger.debug("No alert needed - comparison passed")
            return
        
        logger.info(f"Sending alerts for {result['overall_status']} result")
        
        if 'email' in self.channels:
            self._send_email_alert(result, report_files)
        
        if 'slack' in self.channels:
            self._send_slack_alert(result, report_files)
    
    def _send_email_alert(
        self,
        result: Dict[str, Any],
        report_files: Optional[Dict[str, str]]
    ):
        """Send email alert."""
        try:
            # Email configuration from environment
            smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
            smtp_port = int(os.getenv('SMTP_PORT', '587'))
            smtp_user = os.getenv('SMTP_USER')
            smtp_password = os.getenv('SMTP_PASSWORD')
            alert_email = os.getenv('ALERT_EMAIL')
            
            if not all([smtp_user, smtp_password, alert_email]):
                logger.warning("Email configuration incomplete - skipping email alert")
                return
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = smtp_user
            msg['To'] = alert_email
            msg['Subject'] = f"Statistical Validation {result['overall_status']}: {result['source_table']}"
            
            # Email body
            body = self._format_email_body(result, report_files)
            msg.attach(MIMEText(body, 'html'))
            
            # Send email
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email alert sent to {alert_email}")
        
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
    
    def _send_slack_alert(
        self,
        result: Dict[str, Any],
        report_files: Optional[Dict[str, str]]
    ):
        """Send Slack alert via webhook."""
        try:
            webhook_url = os.getenv('SLACK_WEBHOOK_URL')
            
            if not webhook_url:
                logger.warning("Slack webhook URL not configured - skipping Slack alert")
                return
            
            # Format Slack message
            message = self._format_slack_message(result, report_files)
            
            # Send to Slack
            response = requests.post(
                webhook_url,
                json=message,
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info("Slack alert sent successfully")
            else:
                logger.error(f"Slack alert failed: {response.status_code} - {response.text}")
        
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {str(e)}")
    
    def _format_email_body(
        self,
        result: Dict[str, Any],
        report_files: Optional[Dict[str, str]]
    ) -> str:
        """Format HTML email body."""
        status_color = {
            'PASS': '#2ecc71',
            'FAIL': '#e74c3c',
            'WARNING': '#f39c12'
        }
        
        color = status_color.get(result['overall_status'], '#95a5a6')
        
        failed_tests = [t for t in result['tests'] if t['status'] == 'FAIL']
        failed_list = '<br>'.join([
            f"• {t['test_name']}" + (f" on {t.get('column')}" if t.get('column') else "")
            for t in failed_tests[:10]  # Limit to first 10
        ])
        
        body = f"""
        <html>
        <body style="font-family: Arial, sans-serif; line-height: 1.6;">
            <h2 style="color: {color};">Statistical Validation {result['overall_status']}</h2>
            
            <p><strong>Source:</strong> {result['source_table']}</p>
            <p><strong>Destination:</strong> {result['dest_table']}</p>
            <p><strong>Timestamp:</strong> {result['timestamp']}</p>
            
            <h3>Summary</h3>
            <ul>
                <li>Total Tests: {result['summary']['total_tests']}</li>
                <li>Passed: {result['summary']['passed']}</li>
                <li>Failed: {result['summary']['failed']}</li>
                <li>Warnings: {result['summary']['warnings']}</li>
            </ul>
            
            {f'<h3>Failed Tests</h3><p>{failed_list}</p>' if failed_tests else ''}
            
            {f'<p><strong>Reports:</strong> {", ".join(report_files.values())}</p>' if report_files else ''}
            
            <hr>
            <p style="color: #7f8c8d; font-size: 0.9em;">
                This is an automated alert from the Statistical Validation system.
            </p>
        </body>
        </html>
        """
        
        return body
    
    def _format_slack_message(
        self,
        result: Dict[str, Any],
        report_files: Optional[Dict[str, str]]
    ) -> Dict[str, Any]:
        """Format Slack message payload."""
        status_emoji = {
            'PASS': ':white_check_mark:',
            'FAIL': ':x:',
            'WARNING': ':warning:'
        }
        
        emoji = status_emoji.get(result['overall_status'], ':question:')
        
        failed_tests = [t for t in result['tests'] if t['status'] == 'FAIL']
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{emoji} Statistical Validation {result['overall_status']}"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Source:*\n{result['source_table']}"},
                    {"type": "mrkdwn", "text": f"*Destination:*\n{result['dest_table']}"},
                    {"type": "mrkdwn", "text": f"*Total Tests:*\n{result['summary']['total_tests']}"},
                    {"type": "mrkdwn", "text": f"*Failed:*\n{result['summary']['failed']}"}
                ]
            }
        ]
        
        if failed_tests:
            failed_text = '\n'.join([
                f"• {t['test_name']}" + (f" on `{t.get('column')}`" if t.get('column') else "")
                for t in failed_tests[:10]
            ])
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Failed Tests:*\n{failed_text}"
                }
            })
        
        return {"blocks": blocks}
