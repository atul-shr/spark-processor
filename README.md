# Employee Data Processor

A Python application for processing employee data from delimited files and loading it into databases with advanced querying and analysis capabilities.

## Features

- **File Processing**
  - Read delimited files (CSV, TSV, etc.)
  - Configurable delimiter and header options
  - Error handling and logging

- **Database Integration**
  - Support for multiple database types (SQLite, PostgreSQL, etc.)
  - Flexible write modes (append, replace)
  - Secure credential management

- **Advanced Querying**
  - Filter by multiple criteria
  - Sort and order results
  - Aggregate statistics

- **Data Analysis**
  - Department-level metrics
  - Salary range analysis
  - Employee distribution analysis
  - Level-based statistics

## Project Structure

```
spark-processor/
├── src/
│   ├── __init__.py
│   ├── spark_processor.py    # Main data processing class
│   ├── employee_queries.py   # Query functionality
│   └── config_handler.py     # Configuration management
├── tests/
│   ├── test_data_processor.py
│   ├── test_employee_queries.py
│   ├── test_config.py
│   └── test_analysis.py
├── config/
│   ├── config.yaml          # Main configuration
│   └── .env.example        # Environment variables template
├── data/
│   └── source/             # Source data files
├── main.py                 # Main execution script
├── analyze_data.py         # Data analysis script
└── requirements.txt        # Project dependencies
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/atul-shr/spark-processor.git
   cd spark-processor
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure the application:
   - Copy `config/.env.example` to `config/.env`
   - Update `config/config.yaml` with your settings

## Usage

### Basic Data Processing

Run the main processor:
```bash
python main.py
```

### Data Analysis

Run the analysis script:
```bash
python analyze_data.py
```

### Configuration

The `config.yaml` file supports the following options:

```yaml
source:
  file_path: "path/to/your/file.csv"
  delimiter: ","
  header: true
  file_format: "csv"

target:
  type: "sqlite"  # or postgresql, mysql, etc.
  database: "path/to/database"
  table: "table_name"
  mode: "append"  # or replace
```

For database types other than SQLite, configure credentials in `.env`:
```
DB_USER=your_username
DB_PASSWORD=your_password
```

## Testing

Run all tests:
```bash
python -m unittest discover tests
```

Run specific test suite:
```bash
python -m unittest tests/test_data_processor.py
```

## Code Examples

### Reading and Processing Data

```python
from src.spark_processor import DataProcessor
from src.config_handler import Config

# Initialize processor
processor = DataProcessor()

# Read data
df = processor.read_delimited_file('data/source/employees.csv')

# Write to database
processor.write_to_database(
    df,
    table_name='employees',
    db_url='sqlite:///data/employees.db',
    mode='append'
)
```

### Querying Data

```python
from src.employee_queries import EmployeeQueries

# Initialize queries
queries = EmployeeQueries('sqlite:///data/employees.db')

# Find engineers with high salaries
engineers = queries.query_by_criteria(
    {'occupation': ['Software Engineer', 'DevOps Engineer'],
     'salary': 100000},
    sort_by='salary',
    ascending=False
)

# Get salary statistics
stats = queries.get_salary_stats_by_occupation()
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built with Python 3.8+
- Uses pandas for data processing
- SQLAlchemy for database operations