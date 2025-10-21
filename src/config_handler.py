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

    def _validate_source_config(self, config: Dict[str, Any]) -> None:
        """Validate source configuration.
        
        Args:
            config: Source configuration dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        required_fields = ['file_path', 'delimiter', 'header']
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field '{field}' in source configuration")
            
        if not isinstance(config['delimiter'], str):
            raise ValueError("Delimiter must be a string")
        if not isinstance(config['header'], bool):
            raise ValueError("Header must be a boolean")

    @property
    def source_config(self) -> Dict[str, Any]:
        """Get source file configuration.
        
        Returns:
            dict: Source configuration
            
        Raises:
            ValueError: If configuration is invalid
        """
        config = self.config.get('source', {})
        self._validate_source_config(config)
        return config

    @property
    def target_config(self) -> Dict[str, Any]:
        """Get target database configuration.
        
        Returns:
            dict: Target configuration
        """
        return self.config.get('target', {})

    def get_db_url(self) -> str:
        """Construct database URL from configuration.
        
        Returns:
            str: Database URL
        """
        db_config = self.target_config
        if db_config['type'] == 'sqlite':
            return f"sqlite:///{db_config['database']}"
        else:
            return f"{db_config['type']}://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{db_config['host']}:{db_config['port']}/{db_config['database']}"