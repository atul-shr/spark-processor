"""
Class for analyzing employee data.
"""
import pandas as pd
from typing import Any
from sqlalchemy import text, create_engine
from .employee_queries import EmployeeQueries

class Analysis:
    """Class for analyzing employee data."""
    
    def __init__(self, db_url: str):
        """Initialize with database URL.
        
        Args:
            db_url: Database connection URL
        """
        self.db_url = db_url
        self.engine = create_engine(db_url)
    
    def department_metrics(self) -> pd.DataFrame:
        """Analyze metrics by department.
        
        Returns:
            DataFrame: Department metrics
        """
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
        return pd.read_sql(query, self.engine)
    
    def level_metrics(self) -> pd.DataFrame:
        """Analyze metrics by level.
        
        Returns:
            DataFrame: Level metrics
        """
        query = """
    SELECT 
        level,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary,
        SUM(salary) as total_payroll
    FROM employees
    GROUP BY level
    ORDER BY avg_salary DESC
    """
        return pd.read_sql(query, self.engine)
    
    def department_level_distribution(self) -> pd.DataFrame:
        """Analyze distribution across departments and levels.
        
        Returns:
            DataFrame: Department-level distribution
        """
        query = """
        SELECT 
            department,
            level,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary
        FROM employees
        GROUP BY department, level
        ORDER BY department, level
        """
        return pd.read_sql(query, self.engine)
    
    def salary_ranges(self) -> pd.DataFrame:
        """Analyze salary ranges.
        
        Returns:
            DataFrame: Salary range metrics
        """
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
            MIN(salary) as min_salary,
            MAX(salary) as max_salary
        FROM employees
        GROUP BY salary_range
        ORDER BY min_salary
        """
        return pd.read_sql(query, self.engine)