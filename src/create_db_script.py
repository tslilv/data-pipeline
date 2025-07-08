"""
This script manages the creation and deletion of a MySQL database and its schema.

Functions:
create_database(conn, cursor, db_name): Creates a MySQL database if it does not exist and switches to it.
drop_database(conn, cursor, db_name): Drops a specified MySQL database if it exists.
create_db(conn, cursor): Executes SQL script from 'create_tables.sql' to create database tables.

Parameters:
conn: MySQL database connection.
cursor: MySQL cursor to execute queries.
db_name: (str) Name of the database to create or drop.

Error Handling:
Handles MySQL errors and rolls back transactions.
"""
import mysql.connector


def create_database(conn, cursor, db_name):
    # Creates a database if it does not exist.
    try:
        # Create the database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        conn.commit()
        print(f"Database '{db_name}' has been created or already exists.")

        # Switch to the created database
        cursor.execute(f"USE {db_name}")
        print(f"Using database '{db_name}'.")
    except mysql.connector.Error as err:
        print(f"Error: {err.msg}")
        conn.rollback()


def drop_database(conn, cursor, db_name):
    # Drops the database if it exists.
    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        conn.commit()
        print(f"Database '{db_name}' has been dropped successfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err.msg}")
        conn.rollback()


def create_db(conn, cursor):
    # Executes SQL script from 'create_tables.sql' to create database tables

    file_path = "create_tables.sql"
    with open(file_path, 'r', encoding='utf-8') as sql_file:
        sql_script = sql_file.read()

    for statement in sql_script.split(';'):
        if statement.strip():
            try:
                cursor.execute(statement)
                print(f"Executed: {statement.strip()[:50]}...")
            except mysql.connector.Error as err:
                print(f"Error: {err.msg}")
                print(f"Failed statement: {statement.strip()[:50]}...")

    print("SQL script executed successfully.")
