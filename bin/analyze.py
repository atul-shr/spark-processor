#!/usr/bin/env python3
"""
Script to run analysis on the processed data.
"""
import logging
import sys
from pathlib import Path

# Add parent directory to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.analysis import Analysis
from src.config_handler import Config

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    """Main function to run the analysis."""
    setup_logging()
    logger = logging.getLogger(__name__)

    try:
        # Load configuration
        config = Config(str(Path("config/config.yaml")))
        
        # Initialize analyzer
        analyzer = Analysis(config.get_db_url())
        
        # Run all analyses
        dept_metrics = analyzer.department_metrics()
        level_metrics = analyzer.level_metrics()
        salary_ranges = analyzer.salary_ranges()
        
        # Print results
        print("\nDepartment Metrics:")
        print(dept_metrics)
        
        print("\nLevel Metrics:")
        print(level_metrics)
        
        print("\nSalary Ranges:")
        print(salary_ranges)
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error running analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()