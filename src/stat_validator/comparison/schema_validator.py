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
        Compare two PyArrow schemas (case-insensitive).
        
        Args:
            source_schema: Source table schema
            dest_schema: Destination table schema
            source_table: Source table name
            dest_table: Destination table name
            
        Returns:
            TestResult with schema comparison details
        """
        # Create case-insensitive mappings
        source_cols = {field.name.upper(): (field.name, str(field.type)) for field in source_schema}
        dest_cols = {field.name.upper(): (field.name, str(field.type)) for field in dest_schema}
        
        source_names_upper = set(source_cols.keys())
        dest_names_upper = set(dest_cols.keys())
        
        missing = list(source_names_upper - dest_names_upper)
        extra = list(dest_names_upper - source_names_upper)
        
        # Check type mismatches for common columns
        common_cols_upper = source_names_upper & dest_names_upper
        type_mismatches = []
        
        for col_upper in common_cols_upper:
            source_type = source_cols[col_upper][1]
            dest_type = dest_cols[col_upper][1]
            
            if not self._types_compatible(source_type, dest_type):
                type_mismatches.append({
                    'column': source_cols[col_upper][0],  # Use original source name
                    'source_type': source_type,
                    'dest_type': dest_type
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
                'common_columns': len(common_cols_upper)
            }
        )
    
    def _types_compatible(self, type1: str, type2: str) -> bool:
        """
        Check if two types are compatible (allowing for common conversions).
        
        Args:
            type1: First type as string
            type2: Second type as string
            
        Returns:
            True if types are compatible
        """
        # Exact match
        if type1 == type2:
            return True
        
        # Numeric type compatibility
        numeric_types = {
            'int32', 'int64', 'float', 'double', 
            'decimal', 'decimal128'
        }
        
        # Check if both are numeric
        type1_base = type1.split('(')[0]  # Handle decimal128(2,2) -> decimal128
        type2_base = type2.split('(')[0]
        
        if any(nt in type1_base for nt in numeric_types) and \
           any(nt in type2_base for nt in numeric_types):
            return True
        
        # Date/timestamp compatibility
        date_types = {'date', 'timestamp', 'date64', 'datetime'}
        if any(dt in type1.lower() for dt in date_types) and \
           any(dt in type2.lower() for dt in date_types):
            return True
        
        return False
    
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
    
    def get_common_columns(self, source_schema: pa.Schema, dest_schema: pa.Schema) -> List[str]:
        """
        Get common column names (case-insensitive) using source naming.
        
        Args:
            source_schema: Source table schema
            dest_schema: Destination table schema
            
        Returns:
            List of common column names (in source case)
        """
        source_map = {field.name.upper(): field.name for field in source_schema}
        dest_names_upper = {field.name.upper() for field in dest_schema}
        
        common_upper = set(source_map.keys()) & dest_names_upper
        
        # Return in original source case
        return [source_map[col_upper] for col_upper in sorted(common_upper)]