"""
Unit tests for data analysis functions.
"""
import unittest
import pandas as pd
import tempfile
import os
from sqlalchemy import create_engine
from src.employee_queries import EmployeeQueries
from src.analysis import Analysis

class TestDataAnalysis(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment once for all tests."""
        # Create test database with sample data
        cls.temp_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.temp_dir, 'test.db')
        cls.db_url = f"sqlite:///{cls.db_path}"
        
        # Sample test data
        cls.test_data = pd.DataFrame({
            'id': range(1, 8),
            'name': ['Test User ' + str(i) for i in range(1, 8)],
            'age': [30, 35, 40, 45, 50, 32, 38],
            'city': ['New York', 'San Francisco', 'New York', 'Seattle', 'Boston', 'Seattle', 'New York'],
            'occupation': ['Engineer', 'Engineer', 'Manager', 'Engineer', 'Analyst', 'Designer', 'Engineer'],
            'department': ['Engineering', 'Engineering', 'Operations', 'Engineering', 'Finance', 'Design', 'Engineering'],
            'level': ['Senior', 'Principal', 'Senior', 'Mid-Level', 'Senior', 'Mid-Level', 'Principal'],
            'salary': [90000, 120000, 95000, 85000, 80000, 75000, 115000]
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

    def test_analyze_department_metrics(self):
        """Test department metrics analysis."""
        analyzer = Analysis(self.db_url)
        metrics = analyzer.department_metrics()
        
        # Check basic structure
        self.assertIn('department', metrics.columns)
        self.assertIn('employee_count', metrics.columns)
        self.assertIn('avg_salary', metrics.columns)
        
        # Verify engineering department metrics
        eng_metrics = metrics[metrics['department'] == 'Engineering'].iloc[0]
        self.assertEqual(eng_metrics['employee_count'], 4)
        self.assertEqual(eng_metrics['min_salary'], 85000)
        self.assertEqual(eng_metrics['max_salary'], 120000)

    def test_analyze_level_metrics(self):
        """Test level metrics analysis."""
        analyzer = Analysis(self.db_url)
        metrics = analyzer.level_metrics()
        
        # Check basic structure
        self.assertIn('level', metrics.columns)
        self.assertIn('employee_count', metrics.columns)
        self.assertIn('avg_salary', metrics.columns)
        
        # Verify principal level metrics
        principal_metrics = metrics[metrics['level'] == 'Principal'].iloc[0]
        self.assertEqual(principal_metrics['employee_count'], 2)
        self.assertTrue(principal_metrics['avg_salary'] > 100000)

    def test_analyze_department_level_distribution(self):
        """Test department-level distribution analysis."""
        analyzer = Analysis(self.db_url)
        distribution = analyzer.department_level_distribution()
        
        # Check basic structure
        self.assertIn('department', distribution.columns)
        self.assertIn('level', distribution.columns)
        self.assertIn('employee_count', distribution.columns)
        
        # Verify engineering senior count
        eng_senior = distribution[
            (distribution['department'] == 'Engineering') & 
            (distribution['level'] == 'Senior')
        ]
        self.assertEqual(len(eng_senior), 1)
        self.assertEqual(eng_senior.iloc[0]['employee_count'], 1)

    def test_analyze_salary_ranges(self):
        """Test salary range analysis."""
        analyzer = Analysis(self.db_url)
        ranges = analyzer.salary_ranges()
        
        # Check basic structure
        self.assertIn('salary_range', ranges.columns)
        self.assertIn('employee_count', ranges.columns)
        self.assertIn('avg_salary', ranges.columns)
        
        # Verify high salary range
        high_range = ranges[ranges['salary_range'] == 'High (100k-120k)']
        self.assertTrue(len(high_range) > 0)
        
        # Verify all employees are accounted for
        total_employees = ranges['employee_count'].sum()
        self.assertEqual(total_employees, len(self.test_data))

if __name__ == '__main__':
    unittest.main()