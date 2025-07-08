data-pipeline

A lightweight ETL pipeline that fetches movie data from a CSV file into a MySQL database.

Note: src/queries_execution.py is the main orchestrator script for the entire ETL pipeline.

Requirements

Python 3.8 or higher

See requirements.txt for required libraries

Usage

python src/queries_execution.py

This will:

Create the database and tables (if missing)

Retrieve or update data/tmdb_movies_data.csv

Transform the CSV with pandas

Load data into MySQL
