"""Base connector interface for data sources."""

from abc import ABC, abstractmethod
from typing import Any
import pyarrow as pa
import pandas as pd
import duckdb
from ..utils.logger import get_logger

logger = get_logger('base_connector')


class BaseConnector(ABC):
    """Abstract base class for data connectors with shared DuckDB caching logic."""

    def __init__(self):
        """Initialize base connector."""
        self._duckdb_conn = None

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
    def get_cache_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get DuckDB connection for caching.

        Returns:
            DuckDB connection object
        """
        pass

    @abstractmethod
    def close(self):
        """Close connection and cleanup resources."""
        pass

    def _clean_dataframe_for_cache(self, df: pd.DataFrame, arrow_schema: pa.Schema = None) -> pd.DataFrame:
        """
        Clean pandas DataFrame for DuckDB caching - shared logic.

        Handles:
        - Binary column detection and removal
        - String encoding issues
        - Decimal to float conversion

        Args:
            df: Pandas DataFrame to clean
            arrow_schema: Optional PyArrow schema for type information

        Returns:
            Cleaned DataFrame with problematic columns tracked
        """
        cols_to_drop = []

        # Clean each column individually with error handling
        for col in df.columns:
            try:
                if df[col].dtype == 'object':
                    # Check if it's binary data
                    if df[col].notna().any():
                        sample = df[col].dropna().iloc[0] if len(df[col].dropna()) > 0 else None
                        if sample and isinstance(sample, (bytes, bytearray)):
                            cols_to_drop.append(col)
                            logger.warning(f"Column {col} contains binary data - will drop")
                            continue

                    # Try to clean string encoding
                    try:
                        df[col] = df[col].apply(
                            lambda x: x.encode('utf-8', errors='ignore').decode('utf-8') if isinstance(x, str) else x
                        )
                    except Exception as e:
                        logger.warning(f"Failed to clean encoding for column {col}: {str(e)} - will drop")
                        cols_to_drop.append(col)
                        continue

                # Convert decimal columns to float to avoid precision errors
                if arrow_schema:
                    for field in arrow_schema:
                        if field.name == col and pa.types.is_decimal(field.type):
                            try:
                                df[col] = df[col].astype(float)
                                logger.debug(f"Converted {col} from DECIMAL to FLOAT")
                            except Exception as e:
                                logger.warning(f"Failed to convert {col} to float: {str(e)} - will drop")
                                cols_to_drop.append(col)
                                continue

            except Exception as e:
                logger.warning(f"Error processing column {col}: {str(e)} - will drop")
                cols_to_drop.append(col)

        # Drop problematic columns
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)
            logger.warning(f"Dropped {len(cols_to_drop)} problematic columns: {cols_to_drop}")

        if len(df.columns) == 0:
            raise Exception("All columns were dropped - cannot cache empty table")

        return df

    def _cache_to_duckdb(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Cache DataFrame to DuckDB with error handling - shared logic.

        Args:
            df: Pandas DataFrame to cache
            table_name: Target table name in DuckDB

        Raises:
            Exception: If caching fails
        """
        conn = self.get_cache_connection()

        try:
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.register(table_name, df)
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")
            conn.unregister(table_name)

            logger.info(f"Successfully cached {len(df)} rows, {len(df.columns)} columns to {table_name}")

        except Exception as duckdb_error:
            # If DuckDB registration fails, try column-by-column
            logger.error(f"DuckDB caching failed: {str(duckdb_error)}")
            logger.info("Attempting column-by-column caching...")

            successful_cols = []
            for col in df.columns:
                try:
                    test_df = df[[col]].copy()
                    conn.register(f"test_{col}", test_df)
                    conn.execute(f"SELECT * FROM test_{col} LIMIT 1")
                    conn.unregister(f"test_{col}")
                    successful_cols.append(col)
                except Exception as col_error:
                    logger.warning(f"Column {col} failed DuckDB test: {str(col_error)}")

            if not successful_cols:
                raise Exception("No columns could be cached successfully")

            # Cache only successful columns
            df = df[successful_cols]
            logger.info(f"Caching {len(successful_cols)} successful columns")

            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.register(table_name, df)
            conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {table_name}")
            conn.unregister(table_name)

            logger.info(f"Successfully cached {len(df)} rows, {len(df.columns)} columns to {table_name}")

    def cache_query(self, query: str, table_name: str = "cached_data"):
        """
        Cache query result to DuckDB - shared implementation.

        Subclasses can override this if they need custom behavior, but most
        should use this default implementation.

        Args:
            query: SQL query to execute
            table_name: Target table name in DuckDB cache
        """
        try:
            # Execute query (connector-specific)
            result = self.execute_query(query)
            df = result.to_pandas()

            logger.info(f"Retrieved {len(df)} rows, {len(df.columns)} columns")

            # Clean dataframe (shared logic)
            df = self._clean_dataframe_for_cache(df, result.schema if hasattr(result, 'schema') else None)

            # Cache to DuckDB (shared logic)
            self._cache_to_duckdb(df, table_name)

        except Exception as e:
            logger.error(f"Failed to cache query: {str(e)}")
            raise
