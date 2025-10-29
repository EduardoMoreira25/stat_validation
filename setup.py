"""Setup configuration for statistical validator package."""

from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / 'README.md'
long_description = readme_file.read_text() if readme_file.exists() else ''

setup(
    name='stat-validator',
    version='1.0.0',
    description='Statistical validation tool for data quality assurance',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Your Team',
    author_email='your.email@company.com',
    url='https://github.com/yourcompany/statistical-validation',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    python_requires='>=3.8',
    install_requires=[
        'pyarrow>=14.0.0',
        'duckdb>=0.9.0',
        'polars>=0.19.0',
        'pandas>=2.0.0',
        'numpy>=1.24.0',
        'scipy>=1.11.0',
        'certifi>=2023.0.0',
        'pyyaml>=6.0',
        'python-dotenv>=1.0.0',
        'click>=8.1.0',
        'rich>=13.0.0',
        'jinja2>=3.1.0',
        'requests>=2.31.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.4.0',
            'pytest-cov>=4.1.0',
            'black>=23.0.0',
            'flake8>=6.0.0',
            'mypy>=1.5.0',
        ],
    },
    entry_points={
        'console_scripts': [
            'stat-validator=stat_validator.cli:cli',
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Quality Assurance',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
)
