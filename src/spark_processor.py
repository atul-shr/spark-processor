"""
Data processor module for handling file processing and database operations.
"""
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional, Dict, Any
import logging
import os
from .performance import measure_performance

class DataProcessor:
    """Main class for processing delimited files and loading them into a database."""
    
    def __init__(self):
        """Initialize the data processor."""
        self.logger = logging.getLogger(__name__)

    @measure_performance
    def read_delimited_file(self, 
                          file_path: str, 
                          delimiter: str = ",",
                          header: bool = True) -> pd.DataFrame:
        """Read a delimited file into a pandas DataFrame.
        
        Args:
            file_path (str): Path to the delimited file
            delimiter (str): Delimiter used in the file (default: ",")
            header (bool): Whether the file has a header row (default: True)
            
        Returns:
            DataFrame: Pandas DataFrame containing the file data
        """
        try:
            header_row = 0 if header else None
            df = pd.read_csv(file_path, delimiter=delimiter, header=header_row)
            self.logger.info(f"Successfully read file {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    @measure_performance
    def write_to_database(self,
                         df: pd.DataFrame,
                         table_name: str,
                         db_url: str,
                         mode: str = "append",
                         chunksize: int = 10000) -> None:
        """Write a DataFrame to a database table.
        
        Args:
            df: Pandas DataFrame to write
            table_name (str): Name of the target table
            db_url (str): Database URL
            mode (str): Write mode (default: "append")
            chunksize: Number of rows to write at a time (default: 10000)
        """
        try:
            engine = create_engine(db_url)
            
            # Convert Spark-style modes to pandas modes
            if_exists = "replace" if mode == "overwrite" else mode
            
            # Write data in chunks to reduce memory usage
            df.to_sql(table_name, engine, if_exists=if_exists, 
                     index=False, chunksize=chunksize)
            
            # Create indexes for better query performance
            if "sqlite" in db_url:
                with engine.connect() as conn:
                    # Index commonly queried columns
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_department ON {table_name}(department)"))
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_level ON {table_name}(level)"))
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_salary ON {table_name}(salary)"))
            
            self.logger.info(f"Successfully wrote data to table {table_name}")
        except Exception as e:
            self.logger.error(f"Error writing to database table {table_name}: {str(e)}")
            raise

    def stop(self):
        """No-op for compatibility."""
        pass