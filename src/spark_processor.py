"""
Main Spark processor module for handling file processing and database operations.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from typing import Optional, Dict, Any
import logging

class SparkProcessor:
    """Main class for processing delimited files and loading them into a database."""
    
    def __init__(self, app_name: str = "DelimitedFileProcessor"):
        """Initialize the Spark processor.
        
        Args:
            app_name (str): Name of the Spark application
        """
        self.spark = self._create_spark_session(app_name)
        self.logger = logging.getLogger(__name__)

    def _create_spark_session(self, app_name: str) -> SparkSession:
        """Create and configure a Spark session.
        
        Args:
            app_name (str): Name of the Spark application
            
        Returns:
            SparkSession: Configured Spark session
        """
        return (SparkSession.builder
                .appName(app_name)
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .getOrCreate())

    def read_delimited_file(self, 
                          file_path: str, 
                          delimiter: str = ",",
                          header: bool = True,
                          schema: Optional[StructType] = None) -> Any:
        """Read a delimited file into a Spark DataFrame.
        
        Args:
            file_path (str): Path to the delimited file
            delimiter (str): Delimiter used in the file (default: ",")
            header (bool): Whether the file has a header row (default: True)
            schema (StructType, optional): Custom schema for the DataFrame
            
        Returns:
            DataFrame: Spark DataFrame containing the file data
        """
        try:
            reader = self.spark.read.format("csv")\
                .option("delimiter", delimiter)\
                .option("header", str(header).lower())
                
            if schema:
                reader = reader.schema(schema)
                
            return reader.load(file_path)
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            raise

    def write_to_database(self,
                         df: Any,
                         table_name: str,
                         db_url: str,
                         mode: str = "append",
                         properties: Optional[Dict[str, str]] = None) -> None:
        """Write a DataFrame to a database table.
        
        Args:
            df: Spark DataFrame to write
            table_name (str): Name of the target table
            db_url (str): JDBC URL for the database
            mode (str): Write mode (default: "append")
            properties (dict, optional): Additional JDBC properties
        """
        try:
            df.write\
                .format("jdbc")\
                .option("url", db_url)\
                .option("dbtable", table_name)\
                .mode(mode)
            
            if properties:
                for key, value in properties.items():
                    df = df.option(key, value)
                    
            df.save()
            self.logger.info(f"Successfully wrote data to table {table_name}")
        except Exception as e:
            self.logger.error(f"Error writing to database table {table_name}: {str(e)}")
            raise

    def stop(self):
        """Stop the Spark session."""
        if self.spark:
            self.spark.stop()