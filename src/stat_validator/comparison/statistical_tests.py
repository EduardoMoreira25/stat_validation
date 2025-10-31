"""Statistical test implementations for data comparison."""

import numpy as np
import pandas as pd
from scipy.stats import ks_2samp, chi2_contingency, ttest_ind
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass


@dataclass
class TestResult:
    """Result of a statistical test."""
    test_name: str
    column: Optional[str]
    status: str  # PASS, FAIL, WARNING, SKIP, ERROR
    details: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        result = {
            'test_name': self.test_name,
            'status': self.status,
            'details': self.details
        }
        if self.column:
            result['column'] = self.column
        return result


class StatisticalTests:
    """Collection of statistical tests for data comparison."""
    
    def __init__(
        self,
        ks_test_pvalue: float = 0.05,
        t_test_pvalue: float = 0.05,
        chi_square_pvalue: float = 0.05,
        psi_threshold: float = 0.1,
        min_sample_size: int = 30
    ):
        """
        Initialize statistical tests.
        
        Args:
            ks_test_pvalue: P-value threshold for KS test (default: 0.05)
            t_test_pvalue: P-value threshold for T-test (default: 0.05)
            chi_square_pvalue: P-value threshold for Chi-square (default: 0.05)
            psi_threshold: PSI threshold (default: 0.1)
            min_sample_size: Minimum sample size for tests (default: 30)
        """
        self.ks_test_pvalue = ks_test_pvalue
        self.t_test_pvalue = t_test_pvalue
        self.chi_square_pvalue = chi_square_pvalue
        self.psi_threshold = psi_threshold
        self.min_sample_size = min_sample_size
    
    def ks_test(
        self,
        source_data: np.ndarray,
        dest_data: np.ndarray,
        column_name: str
    ) -> TestResult:
        """
        Kolmogorov-Smirnov test for distribution similarity.
        
        Tests if two numerical distributions are significantly different.
        High p-value (>= threshold) means distributions are similar (PASS).
        
        Args:
            source_data: Source column data
            dest_data: Destination column data
            column_name: Column name for reporting
            
        Returns:
            TestResult object
        """
        try:
            # Remove NaN values
            source_clean = source_data[~np.isnan(source_data)]
            dest_clean = dest_data[~np.isnan(dest_data)]
            
            if len(source_clean) < self.min_sample_size or len(dest_clean) < self.min_sample_size:
                return TestResult(
                    test_name='ks_test',
                    column=column_name,
                    status='SKIP',
                    details={
                        'reason': 'Insufficient non-null data',
                        'source_size': len(source_clean),
                        'dest_size': len(dest_clean),
                        'min_required': self.min_sample_size
                    }
                )
            
            statistic, p_value = ks_2samp(source_clean, dest_clean)
            
            # High p-value = distributions are similar (good)
            status = 'PASS' if p_value >= self.ks_test_pvalue else 'FAIL'
            
            return TestResult(
                test_name='ks_test',
                column=column_name,
                status=status,
                details={
                    'statistic': round(float(statistic), 4),
                    'p_value': round(float(p_value), 4),
                    'threshold': self.ks_test_pvalue,
                    'interpretation': 'Distributions match' if status == 'PASS' else 'Distributions differ significantly',
                    'source_sample_size': len(source_clean),
                    'dest_sample_size': len(dest_clean)
                }
            )
        
        except Exception as e:
            return TestResult(
                test_name='ks_test',
                column=column_name,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def t_test(
        self,
        source_data: np.ndarray,
        dest_data: np.ndarray,
        column_name: str
    ) -> TestResult:
        """
        Independent t-test for mean comparison.
        
        Tests if two numerical distributions have significantly different means.
        High p-value (>= threshold) means means are similar (PASS).
        
        Args:
            source_data: Source column data
            dest_data: Destination column data
            column_name: Column name for reporting
            
        Returns:
            TestResult object
        """
        try:
            source_clean = source_data[~np.isnan(source_data)]
            dest_clean = dest_data[~np.isnan(dest_data)]
            
            if len(source_clean) < self.min_sample_size or len(dest_clean) < self.min_sample_size:
                return TestResult(
                    test_name='t_test',
                    column=column_name,
                    status='SKIP',
                    details={'reason': 'Insufficient data'}
                )
            
            statistic, p_value = ttest_ind(source_clean, dest_clean)
            
            # High p-value = means are similar (good)
            status = 'PASS' if p_value >= self.t_test_pvalue else 'FAIL'
            
            source_mean = float(np.mean(source_clean))
            dest_mean = float(np.mean(dest_clean))
            
            return TestResult(
                test_name='t_test',
                column=column_name,
                status=status,
                details={
                    'source_mean': round(source_mean, 4),
                    'dest_mean': round(dest_mean, 4),
                    'difference': round(dest_mean - source_mean, 4),
                    'p_value': round(float(p_value), 4),
                    'threshold': self.t_test_pvalue,
                    'interpretation': 'Means match' if status == 'PASS' else 'Means differ significantly'
                }
            )
        
        except Exception as e:
            return TestResult(
                test_name='t_test',
                column=column_name,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def psi_test(
        self,
        source_dist: pd.DataFrame,
        dest_dist: pd.DataFrame,
        column_name: str
    ) -> TestResult:
        """
        Population Stability Index (PSI) for categorical distribution comparison.
        
        PSI measures the shift in distributions:
        - PSI < 0.1: No significant change (PASS)
        - PSI 0.1-0.25: Moderate change (WARNING)
        - PSI > 0.25: Significant change (FAIL)
        
        Args:
            source_dist: DataFrame with columns [value, count]
            dest_dist: DataFrame with columns [value, count]
            column_name: Column name for reporting
            
        Returns:
            TestResult object
        """
        try:
            if len(source_dist) == 0 or len(dest_dist) == 0:
                return TestResult(
                    test_name='psi',
                    column=column_name,
                    status='SKIP',
                    details={'reason': 'No data in one or both distributions'}
                )
            
            # Normalize to percentages
            source_dist = source_dist.copy()
            dest_dist = dest_dist.copy()
            
            source_dist['pct'] = source_dist['cnt'] / source_dist['cnt'].sum()
            dest_dist['pct'] = dest_dist['cnt'] / dest_dist['cnt'].sum()
            
            # Convert value column to string to handle type mismatches
            value_col = source_dist.columns[0]  # First column is the value column
            source_dist[value_col] = source_dist[value_col].astype(str)
            dest_dist[value_col] = dest_dist[value_col].astype(str)
            
            # Merge distributions
            merged = source_dist.merge(
                dest_dist,
                on=value_col,
                how='outer',
                suffixes=('_src', '_dst')
            )
            
            # Fill missing values with small number to avoid log(0)
            merged = merged.fillna(0.0001)
            
            # Calculate PSI
            merged['psi_component'] = (
                (merged['pct_dst'] - merged['pct_src']) * 
                np.log(merged['pct_dst'] / merged['pct_src'])
            )
            psi_value = float(merged['psi_component'].sum())
            
            # Determine status
            if psi_value < 0.1:
                status = 'PASS'
                interpretation = 'No significant change'
            elif psi_value < 0.25:
                status = 'WARNING'
                interpretation = 'Moderate change detected'
            else:
                status = 'FAIL'
                interpretation = 'Significant change detected'
            
            return TestResult(
                test_name='psi',
                column=column_name,
                status=status,
                details={
                    'psi_value': round(psi_value, 4),
                    'threshold': self.psi_threshold,
                    'interpretation': interpretation,
                    'source_cardinality': len(source_dist),
                    'dest_cardinality': len(dest_dist)
                }
            )
        
        except Exception as e:
            return TestResult(
                test_name='psi',
                column=column_name,
                status='ERROR',
                details={'error': str(e)}
            )
    
    def date_range_test(
        self,
        source_data: np.ndarray,
        dest_data: np.ndarray,
        column_name: str
    ) -> TestResult:
        """
        Test if date ranges match between source and destination.
        
        Checks if min and max dates are within acceptable tolerance (1 day).
        
        Args:
            source_data: Source date column data
            dest_data: Destination date column data
            column_name: Column name for reporting
            
        Returns:
            TestResult object
        """
        try:
            import pandas as pd
            
            # Convert to datetime if not already
            src_dates = pd.to_datetime(source_data)
            dst_dates = pd.to_datetime(dest_data)
            
            # Remove NaT values
            src_dates = src_dates[src_dates.notna()]
            dst_dates = dst_dates[dst_dates.notna()]
            
            if len(src_dates) == 0 or len(dst_dates) == 0:
                return TestResult(
                    test_name='date_range',
                    column=column_name,
                    status='SKIP',
                    details={'reason': 'No valid dates in one or both columns'}
                )
            
            src_min, src_max = src_dates.min(), src_dates.max()
            dst_min, dst_max = dst_dates.min(), dst_dates.max()
            
            # Check if ranges are similar (within 1 day tolerance)
            one_day = pd.Timedelta(days=1)
            min_match = abs(src_min - dst_min) <= one_day
            max_match = abs(src_max - dst_max) <= one_day
            
            status = 'PASS' if min_match and max_match else 'FAIL'
            
            return TestResult(
                test_name='date_range',
                column=column_name,
                status=status,
                details={
                    'source_min': str(src_min),
                    'source_max': str(src_max),
                    'dest_min': str(dst_min),
                    'dest_max': str(dst_max),
                    'source_span_days': int((src_max - src_min).days),
                    'dest_span_days': int((dst_max - dst_min).days),
                    'interpretation': 'Date ranges match' if status == 'PASS' else 'Date ranges differ'
                }
            )
        except Exception as e:
            return TestResult(
                test_name='date_range',
                column=column_name,
                status='ERROR',
                details={'error': str(e)}
            )
    def chi_square_test(
        self,
        source_dist: pd.DataFrame,
        dest_dist: pd.DataFrame,
        column_name: str
    ) -> TestResult:
        """
        Chi-square test for categorical distribution independence.
        
        Tests if the distribution of categories is independent between source and dest.
        High p-value (>= threshold) means distributions are similar (PASS).
        
        Args:
            source_dist: DataFrame with columns [value, count]
            dest_dist: DataFrame with columns [value, count]
            column_name: Column name for reporting
            
        Returns:
            TestResult object
        """
        try:
            if len(source_dist) == 0 or len(dest_dist) == 0:
                return TestResult(
                    test_name='chi_square',
                    column=column_name,
                    status='SKIP',
                    details={'reason': 'No data'}
                )
            
            # Convert value column to string to handle type mismatches
            value_col = source_dist.columns[0]
            source_dist = source_dist.copy()
            dest_dist = dest_dist.copy()
            source_dist[value_col] = source_dist[value_col].astype(str)
            dest_dist[value_col] = dest_dist[value_col].astype(str)
            
            # Create contingency table
            merged = source_dist.merge(
                dest_dist,
                on=value_col,
                how='outer',
                suffixes=('_src', '_dst')
            ).fillna(0)
            
            contingency_table = merged[['cnt_src', 'cnt_dst']].values
            
            # Perform chi-square test
            chi2, p_value, dof, expected = chi2_contingency(contingency_table)
            
            status = 'PASS' if p_value >= self.chi_square_pvalue else 'FAIL'
            
            return TestResult(
                test_name='chi_square',
                column=column_name,
                status=status,
                details={
                    'chi2_statistic': round(float(chi2), 4),
                    'p_value': round(float(p_value), 4),
                    'degrees_of_freedom': int(dof),
                    'threshold': self.chi_square_pvalue,
                    'interpretation': 'Distributions match' if status == 'PASS' else 'Distributions differ significantly'
                }
            )
        
        except Exception as e:
            return TestResult(
                test_name='chi_square',
                column=column_name,
                status='ERROR',
                details={'error': str(e)}
            )