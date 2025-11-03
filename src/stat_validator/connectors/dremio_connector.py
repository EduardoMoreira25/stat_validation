"""Dremio connector with DuckDB caching for statistical validation."""

import os
import certifi
import duckdb
from pyarrow import flight
import pyarrow as pa
import polars as pl
import pandas as pd
from typing import Optional, List, Tuple, Dict, Any
from .base_connector import BaseConnector
from ..utils.logger import get_logger

logger = get_logger('dremio_connector')


class FlightConnector:
    """Arrow Flight connector for Dremio."""
    
    def __init__(
        self,
        host: str,
        port: int,
        tls: bool,
        certs: Optional[str],
        disable_server_verification: bool,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        session_properties: Optional[List[Tuple[bytes, bytes]]] = None,
        engine: Optional[str] = None
    ):
        scheme = "grpc+tls" if tls else "grpc+tcp"
        connection_args = {}
        
        if tls:
            if disable_server_verification:
                connection_args['disable_server_verification'] = True
            elif certs:
                with open(certs, "rb") as cert_file:
                    connection_args["tls_root_certs"] = cert_file.read()

        self.client = flight.FlightClient(f"{scheme}://{host}:{port}", **connection_args)

        headers = session_properties or []
        if engine:
            headers.append((b'engine', engine.encode('utf-8')))

        if user and password:
            token_pair = self.client.authenticate_basic_token(user, password)
            headers.append(token_pair)
            self.options = flight.FlightCallOptions(headers=headers)
        elif token:
            headers.append((b'authorization', f'Bearer {token}'.encode()))
            self.options = flight.FlightCallOptions(headers=headers)
        else:
            raise ValueError("Provide either token or username/password for authentication.")

    def execute_query(self, query: str) -> pa.Table:
        """Execute query via Arrow Flight."""
        flight_info = self.client.get_flight_info(
            flight.FlightDescriptor.for_command(query),
            self.options
        )
        batches = []
        for endpoint in flight_info.endpoints:
            reader = self.client.do_get(endpoint.ticket, self.options)
            batches.extend(reader.read_all().to_batches())
        
        if not batches:
            return pa.table({})
        
        return pa.Table.from_batches(batches)


class DuckDBCache:
    """DuckDB-based local cache for query results."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path

    def cache_table(self, arrow_table: pa.Table, table_name: str):
        """Cache PyArrow table to DuckDB."""
        with duckdb.connect(self.db_path) as conn:
            conn.register('arrow_table', arrow_table)
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM arrow_table")

    def query(self, sql_query: str) -> pl.DataFrame:
        """Query cached data and return as Polars DataFrame."""
        with duckdb.connect(self.db_path) as conn:
            return conn.execute(sql_query).pl()
    
    def get_connection(self):
        """Get DuckDB connection for direct access."""
        return duckdb.connect(self.db_path)


class DremioConnector(BaseConnector):
    """
    Dremio connector with DuckDB caching for statistical validation.
    
    This connector executes queries against Dremio via Arrow Flight
    and caches results locally in DuckDB for efficient analysis.
    """
    
    def __init__(
        self,
        hostname: str = "localhost",
        flightport: int = 32010,
        username: Optional[str] = None,
        password: Optional[str] = None,
        pat_or_auth_token: Optional[str] = None,
        tls: bool = False,
        disable_server_verification: bool = False,
        trusted_certificates: Optional[str] = None,
        session_properties: Optional[List[Tuple[bytes, bytes]]] = None,
        engine: Optional[str] = None,
        db: str = "cache.duckdb"
    ):
        """
        Initialize Dremio connector.

        Args:
            hostname: Dremio hostname
            flightport: Arrow Flight port (default: 32010)
            username: Dremio username (if not using PAT)
            password: Dremio password (if not using PAT)
            pat_or_auth_token: Personal Access Token (preferred)
            tls: Enable TLS
            disable_server_verification: Disable TLS verification
            trusted_certificates: Path to certificate file
            session_properties: Additional session properties
            engine: Dremio engine name
            db: DuckDB cache file path
        """
        super().__init__()  # Initialize BaseConnector

        if trusted_certificates is None:
            trusted_certificates = certifi.where()

        self.flight_connector = FlightConnector(
            host=hostname,
            port=flightport,
            tls=tls,
            certs=trusted_certificates,
            disable_server_verification=disable_server_verification,
            user=username,
            password=password,
            token=pat_or_auth_token,
            session_properties=session_properties,
            engine=engine
        )
        self.duckdb_cache = DuckDBCache(db)
    
    def execute_query(self, query: str) -> pa.Table:
        """Execute query against Dremio and return PyArrow table."""
        return self.flight_connector.execute_query(query)
    
    def direct_query(self, sql_query: str, engine: str = "polars") -> Any:
        """
        Execute query and return results directly (no caching).
        
        Args:
            sql_query: SQL query string
            engine: Output format ('polars' or 'pandas')
            
        Returns:
            Polars or Pandas DataFrame
        """
        arrow_table = self.execute_query(sql_query)
        
        if engine == "polars":
            return pl.from_arrow(arrow_table)
        elif engine == "pandas":
            return arrow_table.to_pandas()
        else:
            raise ValueError(f"Unsupported engine: {engine}")
    
    # cache_query is now inherited from BaseConnector - no override needed
    # The shared implementation handles all the caching logic
    
    def query_cache(self, sql_query: str) -> pl.DataFrame:
        """
        Query cached data in DuckDB.
        
        Args:
            sql_query: SQL query string
            
        Returns:
            Polars DataFrame
        """
        return self.duckdb_cache.query(sql_query)
    
    def get_table_schema(self, table_name: str) -> pa.Schema:
        """Get schema for a Dremio table."""
        query = f"SELECT * FROM {table_name} LIMIT 1"
        result = self.execute_query(query)
        return result.schema
    
    def get_row_count(self, table_name: str) -> int:
        """Get row count for a Dremio table."""
        query = f"SELECT COUNT(*) as cnt FROM {table_name}"
        result = self.execute_query(query)
        return result.to_pandas()['cnt'].iloc[0]
    
    def get_cache_connection(self):
        """Get direct DuckDB connection for advanced queries."""
        return self.duckdb_cache.get_connection()
    
    def close(self):
        """Close connections (Flight client doesn't need explicit close)."""
        pass
    
    @classmethod
    def from_env(cls, db_path: Optional[str] = None) -> 'DremioConnector':
        """
        Create connector from environment variables.
        
        Args:
            db_path: Override default DuckDB path
            
        Returns:
            Configured DremioConnector instance
        """
        return cls(
            hostname=os.getenv('DREMIO_HOSTNAME', 'localhost'),
            flightport=int(os.getenv('DREMIO_PORT', '32010')),
            username=os.getenv('DREMIO_USERNAME'),
            password=os.getenv('DREMIO_PASSWORD'),
            pat_or_auth_token=os.getenv('DREMIO_PAT'),
            tls=os.getenv('DREMIO_TLS', 'true').lower() == 'true',
            disable_server_verification=os.getenv('DREMIO_DISABLE_SERVER_VERIFICATION', 'true').lower() == 'true',
            db=db_path or os.getenv('DUCKDB_CACHE_PATH', '_validation_cache.duckdb')
        )
