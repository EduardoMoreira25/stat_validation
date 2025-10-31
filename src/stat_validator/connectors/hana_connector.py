"""SAP HANA connector with DuckDB caching for statistical validation."""

import os
import duckdb
from hdbcli import dbapi
import pyarrow as pa
import polars as pl
import pandas as pd
from typing import Optional, List, Dict, Any
from .base_connector import BaseConnector
from ..utils.logger import get_logger

logger = get_logger('hana_connector')

class DuckDBCache:
    """DuckDB-based local cache for query results."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path

    def cache_table(self, df: pd.DataFrame, table_name: str):
        """Cache Pandas DataFrame to DuckDB."""
        with duckdb.connect(self.db_path) as conn:
            conn.register('temp_df', df)
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")

    def query(self, sql_query: str) -> pl.DataFrame:
        """Query cached data and return as Polars DataFrame."""
        with duckdb.connect(self.db_path) as conn:
            return conn.execute(sql_query).pl()
    
    def get_connection(self):
        """Get DuckDB connection for direct access."""
        return duckdb.connect(self.db_path)


class HanaConnector(BaseConnector):
    """
    SAP HANA connector with DuckDB caching for statistical validation.
    
    This connector executes queries against SAP HANA and caches results 
    locally in DuckDB for efficient analysis.
    """
    
    def __init__(
        self,
        hostname: str = "localhost",
        port: int = 30015,
        username: Optional[str] = None,
        password: Optional[str] = None,
        schema: Optional[str] = None,
        encrypt: bool = True,
        ssl_validate_certificate: bool = False,
        db: str = "cache.duckdb"
    ):
        """
        Initialize SAP HANA connector.
        
        Args:
            hostname: HANA hostname
            port: HANA port (default: 30015)
            username: HANA username
            password: HANA password
            schema: Default schema to use
            encrypt: Enable SSL/TLS encryption
            ssl_validate_certificate: Validate SSL certificate
            db: DuckDB cache file path
        """
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password
        self.schema = schema
        self.encrypt = encrypt
        self.ssl_validate_certificate = ssl_validate_certificate
        
        self._connection = None
        self.duckdb_cache = DuckDBCache(db)
    
    def _get_connection(self):
        """Get or create HANA connection."""
        if self._connection is None or not self._connection.isconnected():
            self._connection = dbapi.connect(
                address=self.hostname,
                port=self.port,
                user=self.username,
                password=self.password,
                encrypt=self.encrypt,
                sslValidateCertificate=self.ssl_validate_certificate,
                currentSchema=self.schema
            )
        return self._connection
    
    def execute_query(self, query: str) -> pa.Table:
        """Execute query against HANA and return PyArrow table."""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            return pa.Table.from_pandas(df)
        finally:
            cursor.close()
    
    def cache_query(self, query: str, table_name: str = "cached_data"):
        """Cache query result to DuckDB."""
        try:
            # Execute query
            result = self.execute_query(query)
            df = result.to_pandas()
            
            # Clean string columns to handle invalid Unicode
            for col in df.columns:
                if df[col].dtype == 'object':  # String columns
                    df[col] = df[col].apply(lambda x: x.encode('utf-8', errors='ignore').decode('utf-8') if isinstance(x, str) else x)
            
            # Store in DuckDB cache
            conn = self.get_cache_connection()
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.register(table_name, df)
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")
            conn.unregister(table_name)
            
            logger.info(f"Cached {len(df)} rows to {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to cache query: {str(e)}")
            raise
    
    def query_cache(self, sql_query: str) -> pl.DataFrame:
        """Query cached data in DuckDB."""
        return self.duckdb_cache.query(sql_query)
    
    def get_table_schema(self, table_name: str) -> pa.Schema:
        """Get schema for a HANA table."""
        query = f"SELECT * FROM {table_name} LIMIT 1"
        result = self.execute_query(query)
        return result.schema
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count for a HANA table."""
        query = f"SELECT COUNT(*) as cnt FROM {table_name}"
        result = self.execute_query(query)
        df = result.to_pandas()
        
        # Handle different column name possibilities
        if 'cnt' in df.columns:
            return int(df['cnt'].iloc[0])
        elif 'CNT' in df.columns:
            return int(df['CNT'].iloc[0])
        else:
            # If column name is different, just get first column
            return int(df.iloc[0, 0])
    
    def get_cache_connection(self):
        """Get direct DuckDB connection for advanced queries."""
        return self.duckdb_cache.get_connection()
    
    def close(self):
        """Close HANA connection."""
        if self._connection and self._connection.isconnected():
            self._connection.close()
    
    @classmethod
    def from_env(cls, db_path: Optional[str] = None) -> 'HanaConnector':
        """Create connector from environment variables."""
        return cls(
            hostname=os.getenv('HANA_HOST', 'localhost'),
            port=int(os.getenv('HANA_PORT', '30015')),
            username=os.getenv('HANA_USER'),
            password=os.getenv('HANA_PASSWORD'),
            schema=os.getenv('HANA_SCHEMA'),
            encrypt=os.getenv('HANA_ENCRYPT', 'true').lower() == 'true',
            ssl_validate_certificate=os.getenv('HANA_SSL_VALIDATE', 'false').lower() == 'true',
            db=db_path or os.getenv('DUCKDB_CACHE_PATH', '_validation_cache.duckdb')
        )