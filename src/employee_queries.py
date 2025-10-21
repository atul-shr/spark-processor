"""
Query functionality for employee data.
"""
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Dict, Any, List, Optional

class EmployeeQueries:
    def __init__(self, db_url: str):
        """Initialize query handler.
        
        Args:
            db_url (str): Database URL
        """
        self.engine = create_engine(db_url)

    def query_by_criteria(self, 
                         criteria: Dict[str, Any],
                         sort_by: Optional[str] = None,
                         ascending: bool = True) -> pd.DataFrame:
        """Query employees by various criteria.
        
        Args:
            criteria (dict): Dictionary of column names and values to filter by
            sort_by (str, optional): Column to sort by
            ascending (bool): Sort order (default: True)
            
        Returns:
            DataFrame: Matching employee records
        """
        conditions = []
        params = {}
        
        for column, value in criteria.items():
            if isinstance(value, (list, tuple)):
                placeholders = [f":val_{column}_{i}" for i in range(len(value))]
                conditions.append(f"{column} IN ({','.join(placeholders)})")
                for i, val in enumerate(value):
                    params[f"val_{column}_{i}"] = val
            else:
                conditions.append(f"{column} = :val_{column}")
                params[f"val_{column}"] = value
        
        query = "SELECT * FROM employees"
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        if sort_by:
            query += f" ORDER BY {sort_by} {'ASC' if ascending else 'DESC'}"
        
        return pd.read_sql(text(query), self.engine, params=params)

    def get_salary_stats_by_occupation(self) -> pd.DataFrame:
        """Get salary statistics grouped by occupation.
        
        Returns:
            DataFrame: Salary statistics by occupation
        """
        query = """
        SELECT 
            occupation,
            COUNT(*) as count,
            AVG(salary) as avg_salary,
            MIN(salary) as min_salary,
            MAX(salary) as max_salary
        FROM employees
        GROUP BY occupation
        ORDER BY avg_salary DESC
        """
        return pd.read_sql(query, self.engine)

    def get_employees_above_salary(self, threshold: float) -> pd.DataFrame:
        """Get employees with salary above threshold.
        
        Args:
            threshold (float): Salary threshold
            
        Returns:
            DataFrame: Matching employee records
        """
        query = "SELECT * FROM employees WHERE salary > :threshold ORDER BY salary DESC"
        return pd.read_sql(text(query), self.engine, params={"threshold": threshold})

    def get_employees_by_city(self, cities: List[str]) -> pd.DataFrame:
        """Get employees from specific cities.
        
        Args:
            cities (list): List of city names
            
        Returns:
            DataFrame: Matching employee records
        """
        placeholders = [f":city_{i}" for i in range(len(cities))]
        query = f"SELECT * FROM employees WHERE city IN ({','.join(placeholders)}) ORDER BY city, salary DESC"
        params = {f"city_{i}": city for i, city in enumerate(cities)}
        return pd.read_sql(text(query), self.engine, params=params)