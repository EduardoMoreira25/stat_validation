"""Schema validation utilities."""

import pyarrow as pa
from typing import Dict, List, Set, Any
from .statistical_tests import TestResult


class SchemaValidator:
    """Validates schema compatibility between source and destination."""
    
    def __init__(self):
        pass
    
    def compare_schemas(
        self,
        source_schema: pa.Schema,
        dest_schema: pa.Schema,
        source_table: str,
        dest_table: str
    ) -> TestResult:
        """
        Compare two PyArrow schemas.
        
        Args:
            source_schema: Source table schema
            dest_schema: Destination table schema
            source_table: Source table name
            dest_table: Destination table name
            
        Returns:
            TestResult with schema comparison details
        """
        source_cols = {field.name: str(field.type) for field in source_schema}
        dest_cols = {field.name: str(field.type) for field in dest_schema}
        
        source_names = set(source_cols.keys())
        dest_names = set(dest_cols.keys())
        
        missing = list(source_names - dest_names)
        extra = list(dest_names - source_names)
        
        # Check type mismatches for common columns
        common_cols = source_names & dest_names
        type_mismatches = []
        
        for col in common_cols:
            if source_cols[col] != dest_cols[col]:
                type_mismatches.append({
                    'column': col,
                    'source_type': source_cols[col],
                    'dest_type': dest_cols[col]
                })
        
        # Determine status
        if missing or extra:
            status = 'FAIL'
        elif type_mismatches:
            status = 'WARNING'  # Type mismatches might be acceptable in some cases
        else:
            status = 'PASS'
        
        return TestResult(
            test_name='schema_comparison',
            column=None,
            status=status,
            details={
                'source_table': source_table,
                'dest_table': dest_table,
                'source_columns': len(source_cols),
                'dest_columns': len(dest_cols),
                'missing_in_dest': missing,
                'extra_in_dest': extra,
                'type_mismatches': type_mismatches,
                'common_columns': len(common_cols)
            }
        )
    
    def is_numerical_type(self, arrow_type: pa.DataType) -> bool:
        """Check if PyArrow type is numerical."""
        return pa.types.is_integer(arrow_type) or \
               pa.types.is_floating(arrow_type) or \
               pa.types.is_decimal(arrow_type)
    
    def is_categorical_type(self, arrow_type: pa.DataType) -> bool:
        """Check if PyArrow type is categorical/string."""
        return pa.types.is_string(arrow_type) or \
               pa.types.is_large_string(arrow_type) or \
               pa.types.is_binary(arrow_type)
    
    def is_temporal_type(self, arrow_type: pa.DataType) -> bool:
        """Check if PyArrow type is temporal."""
        return pa.types.is_date(arrow_type) or \
               pa.types.is_timestamp(arrow_type) or \
               pa.types.is_time(arrow_type)
    
    def classify_columns(self, schema: pa.Schema) -> Dict[str, List[str]]:
        """
        Classify columns by type.
        
        Args:
            schema: PyArrow schema
            
        Returns:
            Dictionary with keys: numerical, categorical, temporal, other
        """
        classification = {
            'numerical': [],
            'categorical': [],
            'temporal': [],
            'other': []
        }
        
        for field in schema:
            col_name = field.name.lower()
            
            # Check type first
            if self.is_temporal_type(field.type):
                classification['temporal'].append(field.name)
            elif self.is_numerical_type(field.type):
                classification['numerical'].append(field.name)
            elif self.is_categorical_type(field.type):
                # Check if it's a date stored as string (heuristic)
                if any(keyword in col_name for keyword in ['date', '_dt', 'time', '_ts', 'timestamp']):
                    classification['temporal'].append(field.name)
                else:
                    classification['categorical'].append(field.name)
            else:
                classification['other'].append(field.name)
        
        return classification
