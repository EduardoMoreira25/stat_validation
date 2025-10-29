"""Base connector interface for data sources."""

from abc import ABC, abstractmethod
from typing import Any
import pyarrow as pa


class BaseConnector(ABC):
    """Abstract base class for data connectors."""
    
    @abstractmethod
    def execute_query(self, query: str) -> pa.Table:
        """
        Execute a SQL query and return results as PyArrow table.
        
        Args:
            query: SQL query string
            
        Returns:
            PyArrow Table with query results
        """
        pass
    
    @abstractmethod
    def get_table_schema(self, table_name: str) -> pa.Schema:
        """
        Get schema information for a table.
        
        Args:
            table_name: Fully qualified table name
            
        Returns:
            PyArrow Schema
        """
        pass
    
    @abstractmethod
    def get_row_count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Fully qualified table name
            
        Returns:
            Number of rows
        """
        pass
    
    @abstractmethod
    def close(self):
        """Close connection and cleanup resources."""
        pass
