"""
Main script to run the data processor.
"""
import logging
from pathlib import Path
from src.spark_processor import DataProcessor
from src.config_handler import Config

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    """Main function to run the Spark processor."""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = Config(str(Path("config/config.yaml")))
        
        # Initialize data processor
        processor = DataProcessor()
        
        # Read source file
        source_config = config.source_config
        df = processor.read_delimited_file(
            file_path=source_config['file_path'],
            delimiter=source_config['delimiter'],
            header=source_config['header']
        )
        
        # Write to database
        target_config = config.target_config
        processor.write_to_database(
            df=df,
            table_name=target_config['table'],
            db_url=config.get_db_url(),
            mode=target_config['mode']
        )
        
        logger.info("Data processing completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing data: {str(e)}")
        raise
    finally:
        processor.stop()

if __name__ == "__main__":
    main()