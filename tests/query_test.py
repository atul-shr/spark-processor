"""
Script to test employee queries.
"""
import logging
import pandas as pd
from pathlib import Path
from src.config_handler import Config
from src.employee_queries import EmployeeQueries

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def main():
    """Test various employee queries."""
    setup_logging()
    logger = logging.getLogger(__name__)

    # Load configuration
    config = Config(str(Path("config/config.yaml")))
    queries = EmployeeQueries(config.get_db_url())

    # Test different queries
    logger.info("\n1. Engineers with salary >= 90000:")
    high_paid_engineers = queries.query_by_criteria(
        {"occupation": ["Software Engineer", "DevOps Engineer", "Security Engineer"]},
        sort_by="salary",
        ascending=False
    )
    print(high_paid_engineers[["name", "occupation", "salary"]].to_string())

    logger.info("\n2. Salary statistics by occupation:")
    salary_stats = queries.get_salary_stats_by_occupation()
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    print(salary_stats.to_string())

    logger.info("\n3. Employees in tech hubs:")
    tech_hub_employees = queries.get_employees_by_city(
        ["San Francisco", "Seattle", "Austin"]
    )
    print(tech_hub_employees[["name", "city", "occupation", "salary"]].to_string())

    logger.info("\n4. High earners (>110000):")
    high_earners = queries.get_employees_above_salary(110000)
    print(high_earners[["name", "occupation", "salary"]].to_string())

if __name__ == "__main__":
    main()