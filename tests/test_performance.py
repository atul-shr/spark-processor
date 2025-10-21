"""
Performance tests for data processing functions.
"""
import unittest
import tempfile
import os
from pathlib import Path
import time
import logging
import pandas as pd
from src.spark_processor import DataProcessor
from src.performance import measure_performance

class TestPerformance(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test environment with large dataset."""
        # Set up logging
        logging.basicConfig(level=logging.INFO)
        cls.logger = logging.getLogger(__name__)
        
        # Create temp directory
        cls.temp_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.temp_dir, 'test.db')
        cls.db_url = f"sqlite:///{cls.db_path}"
        
        # Create test data
        cls.rows = 100000
        cls.test_data = pd.DataFrame({
            'id': range(cls.rows),
            'name': [f'Employee_{i}' for i in range(cls.rows)],
            'department': ['Engineering'] * cls.rows,
            'level': ['Senior'] * cls.rows,
            'salary': [100000] * cls.rows,
            'occupation': ['Software Engineer'] * cls.rows
        })
        
        # Save test data
        cls.csv_path = os.path.join(cls.temp_dir, 'test.csv')
        cls.test_data.to_csv(cls.csv_path, index=False)
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test files."""
        if os.path.exists(cls.csv_path):
            os.remove(cls.csv_path)
        if os.path.exists(cls.db_path):
            os.remove(cls.db_path)
        os.rmdir(cls.temp_dir)
    
    def test_read_performance(self):
        """Test file reading performance."""
        processor = DataProcessor()
        
        # Measure read time
        start_time = time.time()
        df = processor.read_delimited_file(self.csv_path)
        duration = time.time() - start_time
        
        # Log metrics
        self.logger.info(f"Read {len(df):,} rows in {duration:.2f} seconds")
        self.logger.info(f"Read throughput: {len(df)/duration:,.0f} rows/second")
        
        # Verify read speed
        self.assertLess(duration, 1.0, "File reading took too long (>1s)")
        
    def test_write_performance(self):
        """Test database write performance."""
        processor = DataProcessor()
        df = processor.read_delimited_file(self.csv_path)
        
        # Measure write time
        start_time = time.time()
        processor.write_to_database(df, 'employees', self.db_url)
        duration = time.time() - start_time
        
        # Log metrics
        self.logger.info(f"Wrote {len(df):,} rows in {duration:.2f} seconds")
        self.logger.info(f"Write throughput: {len(df)/duration:,.0f} rows/second")
        
        # Verify write speed
        self.assertLess(duration, 2.0, "Database write took too long (>2s)")
    
    def test_memory_usage(self):
        """Test memory usage during processing."""
        import psutil
        process = psutil.Process()
        
        # Get baseline memory
        baseline_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Process data
        processor = DataProcessor()
        df = processor.read_delimited_file(self.csv_path)
        processor.write_to_database(df, 'employees', self.db_url)
        
        # Get peak memory
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - baseline_memory
        
        # Log metrics
        self.logger.info(f"Baseline memory: {baseline_memory:.1f} MB")
        self.logger.info(f"Peak memory: {peak_memory:.1f} MB")
        self.logger.info(f"Memory increase: {memory_increase:.1f} MB")
        
        # Verify memory usage
        self.assertLess(memory_increase, 200.0, 
                       "Memory usage too high (>200MB increase)")

if __name__ == '__main__':
    unittest.main()