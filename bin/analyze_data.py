"""
Script to analyze employee data with transformations.
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

def analyze_department_metrics(queries: EmployeeQueries):
    """Analyze metrics by department."""
    query = """
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        SUM(salary) as total_payroll
    FROM employees
    GROUP BY department
    ORDER BY total_payroll DESC
    """
    return pd.read_sql(query, queries.engine)

def analyze_level_metrics(queries: EmployeeQueries):
    """Analyze metrics by level."""
    query = """
    SELECT 
        level,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary
    FROM employees
    GROUP BY level
    ORDER BY avg_salary DESC
    """
    return pd.read_sql(query, queries.engine)

def analyze_department_level_distribution(queries: EmployeeQueries):
    """Analyze employee distribution by department and level."""
    query = """
    SELECT 
        department,
        level,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary
    FROM employees
    GROUP BY department, level
    ORDER BY department, avg_salary DESC
    """
    return pd.read_sql(query, queries.engine)

def analyze_salary_ranges(queries: EmployeeQueries):
    """Analyze salary ranges and distributions."""
    query = """
    SELECT 
        CASE 
            WHEN salary >= 120000 THEN 'Very High (120k+)'
            WHEN salary >= 100000 THEN 'High (100k-120k)'
            WHEN salary >= 80000 THEN 'Medium (80k-100k)'
            ELSE 'Entry (Below 80k)'
        END as salary_range,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        GROUP_CONCAT(name) as employees
    FROM employees
    GROUP BY salary_range
    ORDER BY avg_salary DESC
    """
    return pd.read_sql(query, queries.engine)

def main():
    """Run various data transformations and analytics."""
    setup_logging()
    logger = logging.getLogger(__name__)
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

    # Initialize queries
    config = Config(str(Path("config/config.yaml")))
    queries = EmployeeQueries(config.get_db_url())

    # Run analyses
    logger.info("\n1. Department Metrics:")
    dept_metrics = analyze_department_metrics(queries)
    print(dept_metrics.to_string())

    logger.info("\n2. Level Metrics:")
    level_metrics = analyze_level_metrics(queries)
    print(level_metrics.to_string())

    logger.info("\n3. Department-Level Distribution:")
    dept_level_dist = analyze_department_level_distribution(queries)
    print(dept_level_dist.to_string())

    logger.info("\n4. Salary Range Analysis:")
    salary_ranges = analyze_salary_ranges(queries)
    print(salary_ranges.to_string())

if __name__ == "__main__":
    main()