"""
Unit tests for the DataProcessor class.
"""
import unittest
import pandas as pd
import tempfile
import os
from sqlalchemy import create_engine
from src.spark_processor import DataProcessor

class TestDataProcessor(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test."""
        self.processor = DataProcessor()
        
        # Create a temporary CSV file
        self.test_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['Test User 1', 'Test User 2'],
            'age': [30, 35],
            'city': ['Test City 1', 'Test City 2'],
            'occupation': ['Test Job 1', 'Test Job 2'],
            'department': ['Dept 1', 'Dept 2'],
            'level': ['Senior', 'Mid-Level'],
            'salary': [90000, 85000]
        })
        
        self.temp_dir = tempfile.mkdtemp()
        self.csv_path = os.path.join(self.temp_dir, 'test.csv')
        self.test_data.to_csv(self.csv_path, index=False)
        
        # Set up SQLite database
        self.db_path = os.path.join(self.temp_dir, 'test.db')
        self.db_url = f"sqlite:///{self.db_path}"

    def tearDown(self):
        """Clean up test environment after each test."""
        # Remove temporary files
        os.remove(self.csv_path)
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        os.rmdir(self.temp_dir)

    def test_read_delimited_file(self):
        """Test reading a delimited file."""
        # Test with default parameters
        df = self.processor.read_delimited_file(self.csv_path)
        self.assertEqual(len(df), 2)
        self.assertEqual(list(df.columns), 
                        ['id', 'name', 'age', 'city', 'occupation', 'department', 'level', 'salary'])
        
        # Test without header
        df = self.processor.read_delimited_file(self.csv_path, header=False)
        self.assertEqual(len(df), 3)  # Including header row as data
        
        # Test with different delimiter
        self.test_data.to_csv(self.csv_path, sep='|', index=False)
        df = self.processor.read_delimited_file(self.csv_path, delimiter='|')
        self.assertEqual(len(df), 2)

    def test_read_file_not_found(self):
        """Test reading a non-existent file."""
        with self.assertRaises(Exception):
            self.processor.read_delimited_file('nonexistent.csv')

    def test_write_to_database(self):
        """Test writing data to database."""
        # Test append mode
        df = self.test_data.copy()
        self.processor.write_to_database(df, 'test_table', self.db_url, mode='append')
        
        # Verify data was written
        engine = create_engine(self.db_url)
        result = pd.read_sql('SELECT * FROM test_table', engine)
        self.assertEqual(len(result), 2)
        
        # Test replace mode
        new_data = pd.DataFrame({
            'id': [3],
            'name': ['Test User 3'],
            'age': [40],
            'city': ['Test City 3'],
            'occupation': ['Test Job 3'],
            'department': ['Dept 3'],
            'level': ['Principal'],
            'salary': [95000]
        })
        self.processor.write_to_database(new_data, 'test_table', self.db_url, mode='replace')
        
        # Verify data was replaced
        result = pd.read_sql('SELECT * FROM test_table', engine)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['name'], 'Test User 3')

    def test_write_invalid_mode(self):
        """Test writing with invalid mode."""
        with self.assertRaises(Exception):
            self.processor.write_to_database(
                self.test_data, 'test_table', self.db_url, mode='invalid_mode')

    def test_write_invalid_database_url(self):
        """Test writing with invalid database URL."""
        with self.assertRaises(Exception):
            self.processor.write_to_database(
                self.test_data, 'test_table', 'invalid://url', mode='append')

if __name__ == '__main__':
    unittest.main()