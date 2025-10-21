#!/usr/bin/env python3
"""
Script to run employee queries.
"""
import logging
import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.employee_queries import EmployeeQueries
from src.config_handler import Config

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    """Main function to run employee queries."""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = Config(str(Path("config/config.yaml")))
        
        # Initialize query handler
        query_handler = EmployeeQueries(config.get_db_url())
        
        # Run example queries
        print("\nEmployees by Department:")
        print(query_handler.query_by_criteria({'department': 'Engineering'}))
        
        print("\nEmployees Above Salary:")
        criteria = {'salary': 100000}
        print(query_handler.query_by_criteria(criteria, 'salary', ascending=False))
        
        print("\nEmployees by Level:")
        print(query_handler.query_by_criteria({'level': 'Senior'}))
        
        logger.info("Queries completed successfully")
        
    except Exception as e:
        logger.error(f"Error running queries: {str(e)}")
        raise

if __name__ == "__main__":
    main()