## Data Modeling with Postgres

### Summary

Scenario: Sparkify is a music streaming company. The data of users activity and metadata of songs is in json files. In this project, our goal is transform this data into a database with emphasis on star-based schema.

### How to Run?

- Insall Postgres
- Install Python (Above 3.6)

1. Create a python virtual environment (optional)
2. Install dependencies `$pip install -r requirements.txt`
3. Run `$python create_tables.py`. This script creates a database named `sparkify` (also removes sparkify database if exists) and tables based on `sql_queries.py` script.
4. Run `$python etl.py` . This script populates database with user activity data and songs metadata. A stet-by-step approach is implemented in `etl.ipynb`.
