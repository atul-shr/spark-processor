"""
Unit tests for the Config class.
"""
import unittest
import os
import tempfile
from src.config_handler import Config

class TestConfig(unittest.TestCase):
    def setUp(self):
        """Set up test environment before each test."""
        self.temp_dir = tempfile.mkdtemp()
        
        # Create a temporary config file
        self.config_content = """
source:
  file_path: "data/source/test.csv"
  delimiter: ","
  header: true
  file_format: "csv"

target:
  type: "sqlite"
  database: "data/test.db"
  table: "employees"
  mode: "append"
"""
        self.config_path = os.path.join(self.temp_dir, 'test_config.yaml')
        with open(self.config_path, 'w') as f:
            f.write(self.config_content)

    def tearDown(self):
        """Clean up test environment after each test."""
        os.remove(self.config_path)
        # Clean up any additional files in the temp directory
        for filename in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, filename))
        os.rmdir(self.temp_dir)

    def test_load_config(self):
        """Test loading configuration from file."""
        config = Config(self.config_path)
        
        # Test source config
        self.assertEqual(config.source_config['file_path'], "data/source/test.csv")
        self.assertEqual(config.source_config['delimiter'], ",")
        self.assertTrue(config.source_config['header'])
        
        # Test target config
        self.assertEqual(config.target_config['type'], "sqlite")
        self.assertEqual(config.target_config['database'], "data/test.db")
        self.assertEqual(config.target_config['table'], "employees")

    def test_get_db_url_sqlite(self):
        """Test getting database URL for SQLite."""
        config = Config(self.config_path)
        db_url = config.get_db_url()
        self.assertEqual(db_url, "sqlite:///data/test.db")

    def test_get_db_url_postgres(self):
        """Test getting database URL for PostgreSQL."""
        # Create temporary config with PostgreSQL settings
        postgres_config = """
source:
  file_path: "data/source/test.csv"
  delimiter: ","
  header: true
  file_format: "csv"

target:
  type: "postgresql"
  host: "localhost"
  port: 5432
  database: "testdb"
  table: "employees"
  mode: "append"
"""
        config_path = os.path.join(self.temp_dir, 'postgres_config.yaml')
        with open(config_path, 'w') as f:
            f.write(postgres_config)

        # Set environment variables
        os.environ['DB_USER'] = 'testuser'
        os.environ['DB_PASSWORD'] = 'testpass'

        # Test URL generation
        config = Config(config_path)
        db_url = config.get_db_url()
        expected_url = "postgresql://testuser:testpass@localhost:5432/testdb"
        self.assertEqual(db_url, expected_url)

        # Clean up
        os.remove(config_path)
        del os.environ['DB_USER']
        del os.environ['DB_PASSWORD']

    def test_invalid_config_file(self):
        """Test handling of invalid configuration file."""
        invalid_path = os.path.join(self.temp_dir, 'nonexistent.yaml')
        with self.assertRaises(Exception):
            Config(invalid_path)

    def test_invalid_config_format(self):
        """Test handling of invalid configuration format."""
        # Create config with invalid YAML
        invalid_config = """
source:
  file_path: "data/source/test.csv"
  delimiter:
    - invalid
    - format
  header: [true, false]  # Invalid header format
"""
        config_path = os.path.join(self.temp_dir, 'invalid_config.yaml')
        with open(config_path, 'w') as f:
            f.write(invalid_config)

        with self.assertRaises(ValueError):
            config = Config(config_path)
            # Force config access to trigger validation
            _ = config.source_config

        os.remove(config_path)

if __name__ == '__main__':
    unittest.main()