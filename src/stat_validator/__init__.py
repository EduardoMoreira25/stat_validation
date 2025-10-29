"""
Statistical Validator - Data Quality Validation Tool

A comprehensive statistical validation framework for comparing datasets
and ensuring data consistency across systems.
"""

__version__ = '1.0.0'
__author__ = 'Your Team'

from .connectors import DremioConnector
from .comparison import TableComparator, StatisticalTests
from .reporting import ReportGenerator, AlertManager
from .utils import ConfigLoader, setup_logging

__all__ = [
    'DremioConnector',
    'TableComparator',
    'StatisticalTests',
    'ReportGenerator',
    'AlertManager',
    'ConfigLoader',
    'setup_logging',
]
