"""Configuration loader for YAML and environment variables."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv


class ConfigLoader:
    """Loads and manages configuration from YAML and environment variables."""
    
    def __init__(self, config_path: Optional[str] = None, env_path: Optional[str] = None):
        """
        Initialize configuration loader.
        
        Args:
            config_path: Path to YAML config file (default: config/config.yaml)
            env_path: Path to .env file (default: .env in project root)
        """
        # Load environment variables
        if env_path:
            load_dotenv(env_path)
        else:
            load_dotenv()  # Load from default .env location
        
        # Load YAML config
        if config_path is None:
            config_path = self._find_config_file()
        
        self.config = self._load_yaml(config_path)
        self._merge_env_overrides()
    
    def _find_config_file(self) -> str:
        """Find config.yaml in project structure."""
        possible_paths = [
            Path(__file__).parent.parent.parent.parent / 'config' / 'config.yaml',
            Path('config/config.yaml'),
            Path('../config/config.yaml'),
        ]
        
        for path in possible_paths:
            if path.exists():
                return str(path)
        
        raise FileNotFoundError("Could not find config/config.yaml")
    
    def _load_yaml(self, path: str) -> Dict[str, Any]:
        """Load YAML configuration file."""
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    
    def _merge_env_overrides(self):
        """Override config values with environment variables if present."""
        # Threshold overrides
        if os.getenv('ROW_COUNT_THRESHOLD_PCT'):
            self.config.setdefault('thresholds', {})['row_count_tolerance_pct'] = float(os.getenv('ROW_COUNT_THRESHOLD_PCT'))
        
        if os.getenv('KS_TEST_PVALUE'):
            self.config.setdefault('thresholds', {})['ks_test_pvalue'] = float(os.getenv('KS_TEST_PVALUE'))
        
        if os.getenv('PSI_THRESHOLD'):
            self.config.setdefault('thresholds', {})['psi_threshold'] = float(os.getenv('PSI_THRESHOLD'))
        
        if os.getenv('NULL_RATE_THRESHOLD_PCT'):
            self.config.setdefault('thresholds', {})['null_rate_tolerance_pct'] = float(os.getenv('NULL_RATE_THRESHOLD_PCT'))
        
        # Sampling overrides
        if os.getenv('SAMPLE_SIZE'):
            self.config.setdefault('sampling', {})['max_sample_size'] = int(os.getenv('SAMPLE_SIZE'))
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Example: config.get('thresholds.ks_test_pvalue')
        """
        keys = key.split('.')
        value = self.config
        
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default
            
            if value is None:
                return default
        
        return value
    
    def get_dremio_config(self) -> Dict[str, Any]:
        """Get Dremio connection configuration from environment."""
        return {
            'hostname': os.getenv('DREMIO_HOSTNAME', 'localhost'),
            'flightport': int(os.getenv('DREMIO_PORT', '32010')),
            'username': os.getenv('DREMIO_USERNAME'),
            'password': os.getenv('DREMIO_PAT'),  # Changed: Use PAT as password
            'pat_or_auth_token': None,  # Changed: Don't use token parameter
            'tls': os.getenv('DREMIO_TLS', 'true').lower() == 'true',
            'disable_server_verification': os.getenv('DREMIO_DISABLE_SERVER_VERIFICATION', 'true').lower() == 'true',
            'db': os.getenv('DUCKDB_CACHE_PATH', '_validation_cache.duckdb')
        }
    
    def get_all(self) -> Dict[str, Any]:
        """Get entire configuration dictionary."""
        return self.config
    
    def get_hana_config(self) -> Dict[str, Any]:
        """Get HANA connection configuration from environment."""
        return {
            'hostname': os.getenv('HANA_HOST', 'localhost'),
            'port': int(os.getenv('HANA_PORT', '30015')),
            'username': os.getenv('HANA_USER'),
            'password': os.getenv('HANA_PASSWORD'),
            'schema': os.getenv('HANA_SCHEMA'),
            'encrypt': os.getenv('HANA_ENCRYPT', 'true').lower() == 'true',
            'ssl_validate_certificate': os.getenv('HANA_SSL_VALIDATE', 'false').lower() == 'true',
            'db': os.getenv('DUCKDB_CACHE_PATH', '_validation_cache.duckdb')
        }
