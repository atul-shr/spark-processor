"""
Unit tests for the EmployeeQueries class.
"""
import unittest
import pandas as pd
import tempfile
import os
from sqlalchemy import create_engine
from src.employee_queries import EmployeeQueries

class TestEmployeeQueries(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests."""
        # Create test database with sample data
        cls.temp_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.temp_dir, 'test.db')
        cls.db_url = f"sqlite:///{cls.db_path}"
        
        # Sample test data
        cls.test_data = pd.DataFrame({
            'id': range(1, 6),
            'name': ['Test User ' + str(i) for i in range(1, 6)],
            'age': [30, 35, 40, 45, 50],
            'city': ['New York', 'San Francisco', 'New York', 'Seattle', 'Boston'],
            'occupation': ['Engineer', 'Engineer', 'Manager', 'Engineer', 'Analyst'],
            'department': ['Engineering', 'Engineering', 'Operations', 'Engineering', 'Finance'],
            'level': ['Senior', 'Principal', 'Senior', 'Mid-Level', 'Senior'],
            'salary': [90000, 120000, 95000, 85000, 80000]
        })
        
        # Create database and populate with test data
        engine = create_engine(cls.db_url)
        cls.test_data.to_sql('employees', engine, if_exists='replace', index=False)
        
        # Initialize queries
        cls.queries = EmployeeQueries(cls.db_url)

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment after all tests."""
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        os.rmdir(cls.temp_dir)

    def test_query_by_criteria(self):
        """Test querying by various criteria."""
        # Test single criterion
        result = self.queries.query_by_criteria(
            {'department': 'Engineering'},
            sort_by='salary',
            ascending=False
        )
        self.assertEqual(len(result), 3)
        self.assertEqual(result.iloc[0]['salary'], 120000)
        
        # Test multiple criteria
        result = self.queries.query_by_criteria({
            'department': 'Engineering',
            'level': 'Senior'
        })
        self.assertEqual(len(result), 1)
        
        # Test list criterion
        result = self.queries.query_by_criteria({
            'city': ['New York', 'San Francisco']
        })
        self.assertEqual(len(result), 3)

    def test_get_salary_stats_by_occupation(self):
        """Test salary statistics by occupation."""
        result = self.queries.get_salary_stats_by_occupation()
        self.assertTrue('Engineer' in result['occupation'].values)
        self.assertEqual(len(result), 3)  # Engineer, Manager, Analyst
        
        # Verify statistics
        engineer_stats = result[result['occupation'] == 'Engineer'].iloc[0]
        self.assertEqual(engineer_stats['count'], 3)
        self.assertEqual(engineer_stats['min_salary'], 85000)
        self.assertEqual(engineer_stats['max_salary'], 120000)

    def test_get_employees_above_salary(self):
        """Test filtering employees by salary threshold."""
        result = self.queries.get_employees_above_salary(90000)
        self.assertEqual(len(result), 2)
        self.assertTrue(all(result['salary'] > 90000))

    def test_get_employees_by_city(self):
        """Test filtering employees by cities."""
        result = self.queries.get_employees_by_city(['New York', 'Boston'])
        self.assertEqual(len(result), 3)
        self.assertTrue(all(result['city'].isin(['New York', 'Boston'])))
        
        # Test with non-existent city
        result = self.queries.get_employees_by_city(['NonExistentCity'])
        self.assertEqual(len(result), 0)

    def test_invalid_queries(self):
        """Test handling of invalid queries."""
        # Test with invalid column name
        with self.assertRaises(Exception):
            self.queries.query_by_criteria({'invalid_column': 'value'})
        
        # Test with invalid sort column
        with self.assertRaises(Exception):
            self.queries.query_by_criteria(
                {'department': 'Engineering'},
                sort_by='invalid_column'
            )

if __name__ == '__main__':
    unittest.main()