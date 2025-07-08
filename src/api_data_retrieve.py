import mysql.connector
import pandas as pd

# GLOBAL VARIABLES FOR INSERTION PROCESS
CSV_NAME = "tmdb_movies_data.csv"

# Load CSV data into a pandas DataFrame
movies_table = pd.read_csv(CSV_NAME)
movies_table = movies_table.where(pd.notnull(movies_table), None)
# movies_table['ReleaseDate'] = pd.to_datetime(movies_table['ReleaseDate'], errors='coerce')
movies_table = movies_table.dropna(subset=['Id'])  # Remove rows with NaN in 'id' column
movies_table = movies_table.drop_duplicates(subset=['Id'])  # Remove duplicate rows based on 'id'

# Dictionary to store SQL INSERT statements
TABLES = {}

# Dictionary to store processed data for insertion
DATA = {}

TABLES['Movies'] = """INSERT INTO Movies (movie_id, title, release_year, runtime, tagline, overview, popularity, budget, revenue) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
DATA['Movies'] = movies_table[
    ['Id', 'Title', 'ReleaseYear', 'Runtime', 'Tagline', 'Overview', 'Popularity', 'Budget', 'Revenue']].values.tolist()
TABLES['MoviesImdb'] = """INSERT INTO MoviesImdb (movie_id, imdb_id) VALUES (%s, %s)"""
DATA['MoviesImdb'] = movies_table[['Id','ImdbId']].values.tolist()
TABLES['Genres'] = """INSERT INTO Genres (genre_id ,genre_name) VALUES (%s, %s)"""
TABLES['MovieGenres'] = """INSERT INTO MovieGenres (movie_id ,genre_id) VALUES (%s, %s)"""
TABLES['Actors'] = """INSERT INTO Actors (actor_id ,actor_name) VALUES (%s, %s)"""
TABLES['MovieActors'] = """INSERT INTO MovieActors (movie_id ,actor_id) VALUES (%s, %s)"""
TABLES['Directors'] = """INSERT INTO Directors (director_id ,director_name) VALUES (%s, %s)"""
TABLES['MovieDirectors'] = """INSERT INTO MovieDirectors (movie_id ,director_id) VALUES (%s, %s)"""
TABLES['ProductionCompanies'] = """INSERT INTO ProductionCompanies (company_id ,company_name) VALUES (%s, %s)"""
TABLES['MovieProductionCompanies'] = """INSERT INTO MovieProductionCompanies (movie_id ,company_id) VALUES (%s, %s)"""


def insert_table(conn, cursor, table):
    """
    Inserts data into the specified database table using global TABLES dict and DATA dict.

    :param conn: MySQL database connection
    :param cursor: Database cursor
    :param table: string name of table
    """
    try:
        insert_query = TABLES[table]
        data_to_insert = DATA[table]
        cursor.executemany(insert_query, data_to_insert)
        conn.commit()
        print(f"Successfully inserted {cursor.rowcount} rows")

    except mysql.connector.Error as err:
        if err.errno == 1062:  # Duplicate entry error
            print(f"Duplicate entry error: {err.msg}")
            conn.rollback()  # Skip duplicates and rollback the batch
        else:
            conn.rollback()
            print(f"Error inserting data into {table}: {err}")


def process_text(col_name):
    """
    Processes text fields containing multiple values separated by '|'.
    This function:
        - Splits text into separate rows.
        - Creates unique identifiers for each unique value.
        - Establishes relationships between movies and these attributes.
        - Sets data in DATA dictionary for 'Movie'+col_name and col_name

    :param col_name: (str): The name of the column to process.
    """
    # in-place split text
    movies_table[col_name] = movies_table[col_name].str.split('|')
    # all movies table after split
    # ensure data is ok
    exploded_movies = movies_table.explode(col_name)
    exploded_movies = exploded_movies[exploded_movies[col_name].notnull()]
    exploded_movies = exploded_movies[exploded_movies[col_name] != '']
    exploded_movies = exploded_movies.drop_duplicates(subset=['Id', col_name])
    exploded_movies[col_name] = exploded_movies[col_name].str.strip().str.lower()

    # id movie and col_name table
    id_name_table = exploded_movies[['Id', col_name]]
    # list of unique name
    unique = exploded_movies[col_name].unique()

    # col_name and id_col_name
    # Create the DataFrame with genre_name
    col_name_df = pd.DataFrame(unique, columns=[col_name + ' name'])

    # Set the index starting from 1 and reset it to create col_id
    col_name_df.index = range(1, len(col_name_df) + 1)
    col_name_df = col_name_df.reset_index().rename(columns={'index': col_name + ' id'})
    col_name_df[col_name + ' name'] = col_name_df[col_name + ' name'].astype(str)
    col_name_df = col_name_df.drop_duplicates(subset=[col_name + ' name'])

    # JOIN - Map movie IDs to attribute IDs
    movie_id_col_id = pd.merge(id_name_table, col_name_df, left_on=col_name, right_on=col_name + ' name')

    # Drop the 'genres_list' and 'genre_name' columns as they are now redundant
    movie_id_col_id = movie_id_col_id.drop(columns=[col_name, col_name + ' name'])
    movie_id_col_id = movie_id_col_id.drop_duplicates()

    # store processed data for insertion
    movies_col_table = 'Movie'+col_name
    DATA[movies_col_table] = movie_id_col_id.values.tolist()
    DATA[col_name] = col_name_df.values.tolist()


def insert_data(conn, cursor):
    """
        Inserts all movie-related data into the database by calling `insert_table()`
        for each table in the correct order.
    """

    insert_table(conn, cursor, 'Movies')

    insert_table(conn, cursor, 'MoviesImdb')

    process_text('Genres')
    insert_table(conn, cursor, 'Genres')
    insert_table(conn, cursor, 'MovieGenres')

    process_text('Actors')
    insert_table(conn, cursor, 'Actors')
    insert_table(conn, cursor, 'MovieActors')

    process_text('Directors')
    insert_table(conn, cursor, 'Directors')
    insert_table(conn, cursor, 'MovieDirectors')

    process_text('ProductionCompanies')
    insert_table(conn, cursor, 'ProductionCompanies')
    insert_table(conn, cursor, 'MovieProductionCompanies')

