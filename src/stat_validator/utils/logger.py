"""Logging setup for the statistical validator."""

import logging
import logging.config
import yaml
from pathlib import Path
from typing import Optional


def setup_logging(
    config_path: Optional[str] = None,
    default_level: int = logging.INFO
) -> logging.Logger:
    """
    Setup logging configuration.
    
    Args:
        config_path: Path to logging YAML config
        default_level: Default logging level if config not found
    
    Returns:
        Logger instance
    """
    if config_path is None:
        # Find logging config
        possible_paths = [
            Path(__file__).parent.parent.parent.parent / 'config' / 'logging.yaml',
            Path('config/logging.yaml'),
        ]
        
        for path in possible_paths:
            if path.exists():
                config_path = str(path)
                break
    
    # Ensure logs directory exists
    Path('logs').mkdir(exist_ok=True)
    
    if config_path and Path(config_path).exists():
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(
            level=default_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    return logging.getLogger('stat_validator')


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance."""
    return logging.getLogger(f'stat_validator.{name}')
