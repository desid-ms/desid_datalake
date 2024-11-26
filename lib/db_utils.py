import os
import oracledb
import pandas as pd
from typing import Iterator, Optional, Tuple, Dict, Any
from pathlib import Path
from contextlib import contextmanager
import hashlib
from datetime import datetime
import json
from filelock import FileLock
from itertools import islice, chain

# Configure logging


class CheckpointError(Exception):
    """Custom exception for checkpoint-related errors"""
    pass

class DatabaseError(Exception):
    """Custom exception for database-related errors"""
    pass

@contextmanager
def get_db_connection(service_name: str = 'RJPO1DR') -> oracledb.Connection:
    """
    Establishes and returns an Oracle database connection using environment variables.
    """
    connection = None
    
    # First validate credentials
    username = os.environ.get('ORACLE_USR')
    password = os.environ.get('ORACLE_RJPO1DR_PWD')
    
    if not all([username, password]):
        raise ValueError("Database credentials not found in environment variables")
        
    try:
        # Create connection only after validating credentials
        host = 'exaccdfdr-scan.saude.gov'
        port = '1521'
        dsn = f"{host}:{port}/{service_name}"
        
        connection = oracledb.connect(user=username, password=password, dsn=dsn)
        print("Database connection established successfully")
        yield connection
        
    except Exception as e:
        print(f"Database connection error: {str(e)}")
        raise DatabaseError(f"Failed to handle database connection: {str(e)}")
        
    finally:
        if connection is not None:
            try:
                connection.close()
                print("Database connection closed successfully")
            except Exception as e:
                print(f"Error closing database connection: {str(e)}")

class CheckpointManager:
    """Manages checkpoint operations with proper locking and verification"""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.lock_file = self.checkpoint_dir / "checkpoint.lock"
        self.metadata_file = self.checkpoint_dir / "checkpoint_metadata.json"
    
    def _get_checkpoint_path(self, chunk_number: int) -> Path:
        return self.checkpoint_dir / f"chunk_{chunk_number:06d}.parquet"
    
    def _get_metadata_path(self, chunk_number: int) -> Path:
        return self.checkpoint_dir / f"chunk_{chunk_number:06d}.meta"
    
    def _calculate_chunk_hash(self, chunk: pd.DataFrame) -> str:
        """
        Calculates a consistent hash of a DataFrame chunk
        """
        try:
            # Sort by all columns to ensure consistent ordering
            sorted_chunk = chunk.sort_values(by=list(chunk.columns))
            
            # Reset index to ensure consistent index values
            sorted_chunk = sorted_chunk.reset_index(drop=True)
            
            # Fill NaN values with a consistent value
            sorted_chunk = sorted_chunk.fillna('__NA__')
            
            # Calculate hash using both values and column names
            values_hash = pd.util.hash_pandas_object(sorted_chunk).values
            columns_hash = pd.util.hash_pandas_object(pd.Index(sorted_chunk.columns)).values
            
            # Combine both hashes
            combined_hash = hashlib.sha256(values_hash).digest() + hashlib.sha256(columns_hash).digest()
            return hashlib.sha256(combined_hash).hexdigest()
            
        except Exception as e:
            print(f"Error calculating chunk hash: {str(e)}")
            raise CheckpointError(f"Failed to calculate chunk hash: {str(e)}")
    
    def save_checkpoint(self, chunk: pd.DataFrame, chunk_number: int) -> None:
        """Saves a checkpoint with proper locking and metadata"""
        with FileLock(self.lock_file):
            try:
                # Prepare metadata
                metadata = {
                    'chunk_number': chunk_number,
                    'timestamp': datetime.now().isoformat(),
                    'row_count': len(chunk),
                    'columns': list(chunk.columns),
                    'hash': self._calculate_chunk_hash(chunk)
                }
                
                # Save chunk and metadata
                chunk_path = self._get_checkpoint_path(chunk_number)
                metadata_path = self._get_metadata_path(chunk_number)
                
                chunk.to_parquet(chunk_path, index=False, engine='pyarrow')
                with open(metadata_path, 'w') as f:
                    json.dump(metadata, f)
                
                # Update global metadata
                self._update_global_metadata(chunk_number, metadata)
                
                print(f"Saved checkpoint {chunk_number} with {len(chunk)} rows")
                
            except Exception as e:
                print(f"Failed to save checkpoint {chunk_number}: {str(e)}")
                self._cleanup_failed_checkpoint(chunk_number)
                raise CheckpointError(f"Checkpoint save failed: {str(e)}")
    
    def _update_global_metadata(self, chunk_number: int, chunk_metadata: Dict[str, Any]) -> None:
        """Updates the global metadata file with information about the latest checkpoint"""
        try:
            if self.metadata_file.exists():
                with open(self.metadata_file, 'r') as f:
                    metadata = json.load(f)
            else:
                metadata = {'checkpoints': {}}
            
            metadata['checkpoints'][str(chunk_number)] = chunk_metadata
            metadata['last_checkpoint'] = chunk_number
            metadata['last_update'] = datetime.now().isoformat()
            
            with open(self.metadata_file, 'w') as f:
                json.dump(metadata, f)
                
        except Exception as e:
            print(f"Failed to update global metadata: {str(e)}")
            raise CheckpointError(f"Metadata update failed: {str(e)}")
    
    def _cleanup_failed_checkpoint(self, chunk_number: int) -> None:
        """Cleans up files from a failed checkpoint save"""
        try:
            chunk_path = self._get_checkpoint_path(chunk_number)
            metadata_path = self._get_metadata_path(chunk_number)
            
            if chunk_path.exists():
                chunk_path.unlink()
            if metadata_path.exists():
                metadata_path.unlink()
                
        except Exception as e:
            print(f"Failed to cleanup checkpoint {chunk_number}: {str(e)}")
    
    def verify_checkpoint(self, chunk: pd.DataFrame, chunk_number: int) -> bool:
        """Verifies the integrity of a checkpoint with detailed logging"""
        with FileLock(self.lock_file):
            try:
                chunk_path = self._get_checkpoint_path(chunk_number)
                metadata_path = self._get_metadata_path(chunk_number)
                
                if not (chunk_path.exists() and metadata_path.exists()):
                    print(f"Missing checkpoint files for chunk {chunk_number}")
                    return False
                
                # Read saved data and metadata
                saved_chunk = pd.read_parquet(chunk_path)
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                # Verify hash
                current_hash = self._calculate_chunk_hash(chunk)
                saved_hash = metadata['hash']
                
                if current_hash != saved_hash:
                    print(
                        f"Hash mismatch for chunk {chunk_number}:\n"
                        f"Expected: {saved_hash}\n"
                        f"Got: {current_hash}\n"
                        f"Chunk shape: {chunk.shape}\n"
                        f"Saved shape: {saved_chunk.shape}"
                    )
                    return False
                
                # Verify row count
                if len(chunk) != metadata['row_count']:
                    print(
                        f"Row count mismatch for chunk {chunk_number}:\n"
                        f"Expected: {metadata['row_count']}\n"
                        f"Got: {len(chunk)}"
                    )
                    return False
                
                # Verify columns
                if list(chunk.columns) != metadata['columns']:
                    print(
                        f"Column mismatch for chunk {chunk_number}:\n"
                        f"Expected: {metadata['columns']}\n"
                        f"Got: {list(chunk.columns)}"
                    )
                    return False
                
                # Add data sample comparison for debugging
                sample_size = min(5, len(chunk))
                if not chunk.head(sample_size).equals(saved_chunk.head(sample_size)):
                    logger.warning(
                        f"Data sample mismatch in first {sample_size} rows "
                        f"for chunk {chunk_number}"
                    )
                
                return True
                
            except Exception as e:
                print(f"Error verifying checkpoint {chunk_number}: {str(e)}")
                return False
    
    def get_last_checkpoint(self) -> Tuple[int, Optional[pd.DataFrame]]:
        """Returns the last valid checkpoint number and data"""
        with FileLock(self.lock_file):
            try:
                if not self.metadata_file.exists():
                    return -1, None
                
                with open(self.metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                last_checkpoint = metadata.get('last_checkpoint')
                if last_checkpoint is None:
                    return -1, None
                
                chunk_path = self._get_checkpoint_path(last_checkpoint)
                if not chunk_path.exists():
                    return -1, None
                
                data = pd.read_parquet(chunk_path)
                return last_checkpoint, data
                
            except Exception as e:
                print(f"Error reading last checkpoint: {str(e)}")
                return -1, None
    
    def clear_checkpoints(self) -> None:
        """Safely removes all checkpoint files"""
        with FileLock(self.lock_file):
            try:
                # Remove all checkpoint and metadata files
                for f in self.checkpoint_dir.glob("chunk_*.*"):
                    try:
                        f.unlink()
                    except Exception as e:
                        print(f"Error removing file {f}: {str(e)}")
                
                # Remove metadata file
                if self.metadata_file.exists():
                    self.metadata_file.unlink()
                
                # Remove lock file
                if self.lock_file.exists():
                    self.lock_file.unlink()
                
                # Try to remove the directory
                try:
                    self.checkpoint_dir.rmdir()
                except OSError:
                    pass
                    
                print("Checkpoint directory cleared successfully")
                
            except Exception as e:
                print(f"Error clearing checkpoints: {str(e)}")
                raise CheckpointError(f"Failed to clear checkpoints: {str(e)}")

def fetch_data_chunks(query: str, chunksize: int, checkpoint_dir: str) -> Iterator[pd.DataFrame]:
    """
    Fetches data from the database in chunks, with checkpoint support
    """
    checkpoint_manager = CheckpointManager(checkpoint_dir)
    last_chunk_number, _ = checkpoint_manager.get_last_checkpoint()
    start_chunk = last_chunk_number + 1
    
    with get_db_connection() as connection:
        try:
            # Create base iterator
            base_iterator = pd.read_sql_query(
                query,
                connection,
                chunksize=chunksize
            )
            
            # Use islice to skip to the correct position
            chunk_iterator = islice(base_iterator, start_chunk, None)
            
            # Verify we can get at least one chunk
            try:
                first_chunk = next(chunk_iterator)
            except StopIteration:
                raise CheckpointError(
                    f"Not enough chunks to resume from checkpoint {start_chunk}"
                )
                
            # Chain the first chunk with the rest of the iterator
            return chain([first_chunk], chunk_iterator)
            
        except Exception as e:
            print(f"Error in fetch_data_chunks: {str(e)}")
            raise