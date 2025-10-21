"""
Configuration handler for the Spark processor.
"""
import yaml
from typing import Dict, Any
from pathlib import Path
from dotenv import load_dotenv
import os

class Config:
    """Configuration handler for the application."""
    
    def __init__(self, config_path: str):
        """Initialize the configuration handler.
        
        Args:
            config_path (str): Path to the YAML configuration file
        """
        self.config_path = config_path
        self.config = self._load_config()
        load_dotenv()  # Load environment variables from .env file

    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file.
        
        Returns:
            dict: Configuration dictionary
        """
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    @property
    def source_config(self) -> Dict[str, Any]:
        """Get source file configuration.
        
        Returns:
            dict: Source configuration
        """
        return self.config.get('source', {})

    @property
    def target_config(self) -> Dict[str, Any]:
        """Get target database configuration.
        
        Returns:
            dict: Target configuration
        """
        return self.config.get('target', {})

    def get_db_url(self) -> str:
        """Construct database URL from configuration and environment variables.
        
        Returns:
            str: Database URL
        """
        db_config = self.target_config
        return f"{db_config['type']}://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{db_config['host']}:{db_config['port']}/{db_config['database']}"