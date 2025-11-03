import os

structure = {
    'config': ['__init__.py', 'config.yaml', 'logging.yaml'],
    'src/stat_validator': ['__init__.py', 'cli.py'],
    'src/stat_validator/connectors': ['__init__.py', 'dremio_connector.py', 'base_connector.py'],
    'src/stat_validator/comparison': ['__init__.py', 'comparator.py', 'statistical_tests.py', 'schema_validator.py'],
    'src/stat_validator/reporting': ['__init__.py', 'report_generator.py', 'alerting.py'],
    'src/stat_validator/utils': ['__init__.py', 'config_loader.py', 'logger.py'],
    'tests': ['__init__.py', 'conftest.py', 'test_comparator.py', 'test_connectors.py', 'test_statistical_tests.py'],
    'examples': ['basic_comparison.py', 'advanced_config.yaml'],
    'docs': ['architecture.md', 'usage_guide.md', 'api_reference.md'],
    'reports': ['.gitkeep']
}

root_files = ['.env.example', '.gitignore', 'README.md', 'requirements.txt', 'setup.py', 'pyproject.toml']

# Create directories and files
for directory, files in structure.items():
    os.makedirs(directory, exist_ok=True)
    for file in files:
        open(os.path.join(directory, file), 'a').close()

# Create root files
for file in root_files:
    open(file, 'a').close()

print("✅ Project structure created successfully!")