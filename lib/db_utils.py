# db_utils.py
# db_utils.py
import oracledb
import os
from pathlib import Path
import json
import pandas as pd
import logging
import typing as t
from typing import Optional, Dict, Any, Generator
from dataclasses import dataclass
from contextlib import contextmanager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class OracleConfig:
    service_name: str
    user: str = None
    password: str = None
    host: str = 'exaccrjdr-scan.saude.gov'
    port: str = '1521'
    min_connections: int = 1
    max_connections: int = 5
    increment: int = 1

    @property
    def dsn(self) -> str:
        return f"""(DESCRIPTION=
                (ADDRESS=(PROTOCOL=TCP)(HOST={self.host})(PORT={self.port}))
                (CONNECT_DATA=(SERVICE_NAME={self.service_name})))"""

    def __post_init__(self):
        self.user = self.user or os.environ.get('ORACLE_USR')
        self.password = self.password or os.environ.get('ORACLE_PWD')
        if not self.user or not self.password:
            raise ValueError("ORACLE_USR and ORACLE_PWD must be provided either directly or through environment variables")


class DatabasePool:
    _instance: Optional['DatabasePool'] = None
    _pool: Optional[oracledb.ConnectionPool] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabasePool, cls).__new__(cls)
        return cls._instance

    def initialize(self, config: OracleConfig) -> None:
        """Initialize the database pool"""
        if not self._pool:
            try:
                oracledb.init_oracle_client()
                self._pool = oracledb.create_pool(
                    user=config.user,
                    password=config.password,
                    dsn=config.dsn,
                    min=config.min_connections,
                    max=config.max_connections,
                    increment=config.increment
                )
                logger.info("Database pool initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize database pool: {str(e)}")
                raise

    @contextmanager
    def get_connection(self) -> Generator[oracledb.Connection, None, None]:
        """Get a connection from the pool"""
        if not self._pool:
            raise RuntimeError("Database pool not initialized")
        
        connection = None
        try:
            connection = self._pool.acquire()
            yield connection
        finally:
            if connection:
                self._pool.release(connection)

    def close(self) -> None:
        """Close the database pool"""
        if self._pool:
            try:
                self._pool.close()
                self._pool = None
                logger.info("Database pool closed successfully")
            except Exception as e:
                logger.error(f"Error closing database pool: {str(e)}")
                raise


class QueryCheckpoint:
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True, parents=True)
        self.data_dir = self.checkpoint_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        self.metadata_file = self.checkpoint_dir / "metadata.json"
        self.current_offset = 0
        self.processed_chunks = set()
        self.load_checkpoint()

    def load_checkpoint(self) -> None:
        """Load checkpoint data from disk"""
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r') as f:
                    metadata = json.load(f)
                    self.current_offset = metadata.get('offset', 0)
                    self.processed_chunks = set(metadata.get('processed_chunks', []))
                logger.info(f"Loaded checkpoint: offset={self.current_offset}, chunks={len(self.processed_chunks)}")
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            raise

    def save_checkpoint(self) -> None:
        """Save checkpoint data to disk"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump({
                    'offset': self.current_offset,
                    'processed_chunks': list(self.processed_chunks)
                }, f)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
            raise

    def save_chunk(self, chunk: pd.DataFrame, chunk_number: int) -> None:
        """Save a data chunk to disk"""
        try:
            chunk_file = self.data_dir / f"chunk_{chunk_number}.parquet"
            chunk.to_parquet(chunk_file)
            self.processed_chunks.add(chunk_number)
            self.current_offset += len(chunk)
            self.save_checkpoint()
            logger.debug(f"Saved chunk {chunk_number} with {len(chunk)} rows")
        except Exception as e:
            logger.error(f"Error saving chunk {chunk_number}: {str(e)}")
            raise

    def get_processed_data(self) -> pd.DataFrame:
        """Retrieve all processed data chunks"""
        try:
            all_chunks = []
            for chunk_num in sorted(self.processed_chunks):
                chunk_file = self.data_dir / f"chunk_{chunk_num}.parquet"
                if chunk_file.exists():
                    chunk_data = pd.read_parquet(chunk_file)
                    all_chunks.append(chunk_data)
            if all_chunks:
                logger.info(f"Retrieved {len(all_chunks)} previously processed chunks")
                return pd.concat(all_chunks)
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error retrieving processed data: {str(e)}")
            raise

    def cleanup(self) -> None:
        """Clean up all checkpoint files"""
        try:
            if self.checkpoint_dir.exists():
                for file in self.data_dir.glob("*.parquet"):
                    file.unlink()
                self.data_dir.rmdir()
                self.metadata_file.unlink()
                self.checkpoint_dir.rmdir()
                logger.info("Checkpoint files cleaned up successfully")
        except Exception as e:
            logger.error(f"Error cleaning up checkpoint files: {str(e)}")
            raise


class QueryExecutor:
    def __init__(self, config: OracleConfig, checkpoint_dir: str = 'data/checkpoint'):
        self.config = config
        self.checkpoint = QueryCheckpoint(checkpoint_dir)
        self.db_pool = DatabasePool()
        self.db_pool.initialize(config)
        self.logger = logging.getLogger(__name__)

    def __enter__(self) -> 'QueryExecutor':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.db_pool.close()

    def execute_query(
        self,
        query: str,
        params: Dict[str, Any],
        chunksize: int = 4_000_000
    ) -> Generator[pd.DataFrame, None, None]:
        """Execute a query with checkpoint support"""
        success = False
        total_rows_expected = None
        chunk_number = len(self.checkpoint.processed_chunks)

        try:
            # Yield previously processed data
            previous_data = self.checkpoint.get_processed_data()
            if not previous_data.empty:
                self.logger.info(f"Yielding {len(previous_data)} previously processed rows")
                yield previous_data

            # Get total row count
            with self.db_pool.get_connection() as connection:
                count_query = f"""
                    WITH base_query AS (
                        {query.split('ORDER BY')[0]}
                    )
                    SELECT COUNT(*) as total FROM base_query
                """
                total_rows_expected = pd.read_sql(
                    count_query, 
                    con=connection, 
                    params=params
                ).iloc[0]['total']
                self.logger.info(f"Total rows to process: {total_rows_expected}")

            # Process data in chunks
            with self.db_pool.get_connection() as connection:
                for chunk in pd.read_sql(
                    query,
                    con=connection,
                    params=params,
                    chunksize=chunksize
                ):
                    if 'rn' in chunk.columns:
                        chunk = chunk.drop('rn', axis=1)
                    
                    self.logger.info(
                        f"Processing chunk {chunk_number}: "
                        f"{self.checkpoint.current_offset}/{total_rows_expected} "
                        f"({(self.checkpoint.current_offset/total_rows_expected)*100:.2f}%)"
                    )
                    
                    self.checkpoint.save_chunk(chunk, chunk_number)
                    chunk_number += 1
                    yield chunk

                    if self.checkpoint.current_offset >= total_rows_expected:
                        success = True
                        break

            if self.checkpoint.current_offset >= total_rows_expected:
                success = True

        except Exception as e:
            self.logger.error(f"Error during query execution: {str(e)}")
            self.logger.info(f"Processed {self.checkpoint.current_offset} rows out of {total_rows_expected}")
            raise
        finally:
            if success:
                self.logger.info(f"Successfully processed all {total_rows_expected} rows")
                self.checkpoint.cleanup()
            else:
                self.logger.warning(
                    f"Query interrupted. Processed {self.checkpoint.current_offset} rows "
                    f"out of {total_rows_expected}. To resume, run the query again with "
                    f"the same checkpoint_dir: {self.checkpoint.checkpoint_dir}"
                )