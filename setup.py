# setup.py
from setuptools import setup, find_packages

setup(
    name="spark-processor",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "python-dotenv>=1.0.0",
        "PyYAML>=6.0.1",
        "sqlalchemy>=2.0.23",
        "pandas>=2.1.2"
    ],
    author="Atul Sharma",
    description="A PySpark application for processing delimited files and loading them into a database",
    python_requires=">=3.8",
)