# Generic Data Processor

A Python application for processing structured data from delimited files and loading it into databases with advanced querying and analysis capabilities. While initially built for employee data, it can be adapted for any type of structured data.

## Features

- **File Processing**
  - Read any delimited files (CSV, TSV, etc.)
  - Configurable delimiter and header options
  - Robust error handling and logging

- **Database Integration**
  - Support for multiple database types (SQLite, PostgreSQL, etc.)
  - Configurable write modes (append, replace)
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
data-processor/
├── src/
│   ├── __init__.py
│   ├── spark_processor.py    # Core data processing class
│   ├── employee_queries.py   # Data querying functionality
│   ├── analysis.py          # Data analysis module
│   └── config_handler.py     # Configuration management
├── bin/
│   ├── process.py           # Data processing script
│   ├── analyze.py           # Analysis execution script
│   └── query.py            # Query execution script
├── tests/
│   ├── test_data_processor.py
│   ├── test_employee_queries.py
│   ├── test_config.py
│   └── test_analysis.py
├── config/
│   ├── config.yaml          # Main configuration
│   └── .env.example         # Environment variables template
├── data/
│   └── source/             # Source data files
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

# Read data from any delimited file
df = processor.read_delimited_file(
    file_path='data/source/data.csv',
    delimiter=',',  # or '\t' for TSV, '|' for pipe-delimited, etc.
    header=True     # set to False if no header row
)

# Write to any supported database
processor.write_to_database(
    df,
    table_name='data_table',
    db_url='sqlite:///data/database.db',  # or postgresql://user:pass@host/db
    mode='append'  # or 'replace' to overwrite
)
```

### Querying and Analyzing Data

```python
from src.employee_queries import EmployeeQueries
from src.analysis import Analysis

# Initialize components
queries = EmployeeQueries('sqlite:///data/database.db')
analyzer = Analysis('sqlite:///data/database.db')

# Query data with multiple criteria
results = queries.query_by_criteria(
    criteria={'column1': 'value1', 'column2': ['value2', 'value3']},
    sort_by='column3',
    ascending=False
)

# Run analysis
metrics = analyzer.department_metrics()
distributions = analyzer.department_level_distribution()
ranges = analyzer.salary_ranges()
```

### Using Command-Line Tools

Process data:
```bash
./bin/process.py
```

Run analysis:
```bash
./bin/analyze.py
```

Execute queries:
```bash
./bin/query.py
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