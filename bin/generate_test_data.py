#!/usr/bin/env python3
"""
Script to generate large test datasets for performance testing.
"""
import pandas as pd
import numpy as np
from pathlib import Path
import time
import logging

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def generate_employee_data(num_records: int) -> pd.DataFrame:
    """Generate synthetic employee data.
    
    Args:
        num_records: Number of records to generate
        
    Returns:
        DataFrame with synthetic employee data
    """
    # Lists for random selection
    departments = ['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 
                  'Product', 'Operations', 'Data Science', 'Design', 'Legal']
    levels = ['Junior', 'Mid-Level', 'Senior', 'Lead', 'Principal', 'Director']
    cities = ['New York', 'San Francisco', 'Chicago', 'Los Angeles', 'Boston',
              'Seattle', 'Austin', 'Denver', 'Miami', 'Portland', 'Atlanta',
              'Dallas', 'Houston', 'Phoenix', 'Minneapolis', 'Detroit']
    
    # Generate random data
    np.random.seed(42)  # For reproducibility
    
    data = {
        'id': range(1, num_records + 1),
        'name': [f'Employee_{i}' for i in range(num_records)],
        'age': np.random.randint(22, 65, num_records),
        'city': np.random.choice(cities, num_records),
        'department': np.random.choice(departments, num_records),
        'level': np.random.choice(levels, num_records),
        'salary': np.random.normal(90000, 20000, num_records).astype(int)
    }
    
    # Create occupations based on department
    occupation_map = {
        'Engineering': ['Software Engineer', 'DevOps Engineer', 'System Architect'],
        'Sales': ['Sales Representative', 'Account Manager', 'Sales Director'],
        'Marketing': ['Marketing Specialist', 'Content Manager', 'Brand Manager'],
        'HR': ['HR Specialist', 'Recruiter', 'HR Manager'],
        'Finance': ['Financial Analyst', 'Accountant', 'Controller'],
        'Product': ['Product Manager', 'Product Owner', 'Product Director'],
        'Operations': ['Operations Manager', 'Business Analyst', 'Project Manager'],
        'Data Science': ['Data Scientist', 'ML Engineer', 'Data Analyst'],
        'Design': ['UX Designer', 'UI Designer', 'Product Designer'],
        'Legal': ['Legal Counsel', 'Compliance Officer', 'Contract Manager']
    }
    
    data['occupation'] = [
        np.random.choice(occupation_map[dept])
        for dept in data['department']
    ]
    
    return pd.DataFrame(data)

def main():
    """Main function to generate test data."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    try:
        # Create output directory if it doesn't exist
        output_dir = Path('data/source')
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate data
        start_time = time.time()
        logger.info("Generating 100,000 records...")
        
        df = generate_employee_data(100000)
        
        # Save to CSV
        output_file = output_dir / 'employees_large.csv'
        df.to_csv(output_file, index=False)
        
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"Generated {len(df):,} records in {duration:.2f} seconds")
        logger.info(f"Data saved to {output_file}")
        logger.info(f"File size: {output_file.stat().st_size / (1024*1024):.2f} MB")
        
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
        raise

if __name__ == "__main__":
    main()