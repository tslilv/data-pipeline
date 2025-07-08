"""
This file includes all queries.
In all functions, `cursor` is a Database cursor object.
Each function returns structured query results as lists of tuples.
"""


def query_1(cursor, keyword):
    # Search movie titles by keyword using full-text index, ordered by popularity (DESC)
    # Benefits from index on `popularity` for efficient sorting.
    query = """
        SELECT title, release_year, popularity
        FROM Movies
        WHERE MATCH(title) AGAINST (%s)
        ORDER BY popularity DESC;
    """
    cursor.execute(query, (keyword,))
    return cursor.fetchall()


def query_2(cursor, keyword):
    # Search movies using full-text search on movie overviews by keyword, ordered by release_year
    # Benefits from index on 'release_year' for efficient sorting.
    query = """
        SELECT title, release_year, overview
        FROM Movies
        WHERE MATCH(overview) AGAINST (%s)
        ORDER BY release_year DESC;
    """
    cursor.execute(query, (keyword,))
    return cursor.fetchall()


def query_3(cursor, genre_name):
    # Search the top 5 movies with the highest budgets in a specific genre ordered by budget (DESC)
    # Benefits from index on 'budget' for efficient sorting.
    cursor.execute("""
        SELECT m.title, m.budget, g.genre_name
        FROM Movies m
        JOIN MovieGenres mg ON m.movie_id = mg.movie_id
        JOIN Genres g ON mg.genre_id = g.genre_id
        WHERE g.genre_name = %s AND m.budget IS NOT NULL
        ORDER BY m.budget DESC
        LIMIT 5
    """, (genre_name,))
    return cursor.fetchall()


def query_4(cursor):
    # Compute the average rating of movies by release year.
    # Benefits from the index on 'release_year' for grouping and ordering.
    cursor.execute("""
        SELECT release_year, AVG(popularity)
        FROM Movies
        WHERE popularity IS NOT NULL
        GROUP BY release_year
        ORDER BY release_year DESC
    """)
    return cursor.fetchall()


def query_5(cursor, actor_name):
    # Search all movies where a specific actor appears and find his most common genre
    # not using full text index, the actor name is not a keyword
    # returns a tuple (movies list output, most common genre)
    query = """
        WITH ActorMovies AS (
            SELECT m.movie_id, m.title, m.release_year, a.actor_name
            FROM Movies m
            JOIN MovieActors ma ON m.movie_id = ma.movie_id
            JOIN Actors a ON ma.actor_id = a.actor_id
            WHERE a.actor_name = %s
        ),
        MovieGenresCount AS (
            SELECT g.genre_name, COUNT(*) AS genre_count
                FROM MovieGenres mg
                JOIN Genres g ON mg.genre_id = g.genre_id
                JOIN ActorMovies am ON mg.movie_id = am.movie_id
                GROUP BY g.genre_name
                ORDER BY genre_count DESC
                LIMIT 1
        )
        SELECT 
            am.title, am.release_year, am.actor_name, 
            mgc.genre_name, mgc.genre_count
        FROM ActorMovies am
        LEFT JOIN MovieGenresCount mgc ON 1=1;
    """

    try:
        cursor.execute(query, (actor_name,))
        results = cursor.fetchall()

        if not results:
            return [], []

        # Extract movies list
        movies = [(row[0], row[1], row[2]) for row in results]
        # Extract the most common genre
        most_common_genre = (results[0][3], results[0][4]) if results[0][3] else []
        return movies, most_common_genre

    except Exception as e:
        print("Error executing query 5:", e)
        return [], []


def query_6(cursor, movie_name):
    # Search movies IMdb ID by its title using full-text search.
    query = """
        SELECT m.title, mi.imdb_id
        FROM Movies m
        JOIN MoviesImdb mi ON m.movie_id = mi.movie_id
        WHERE MATCH(m.title) AGAINST (%s);
    """
    cursor.execute(query, (movie_name,))
    return cursor.fetchall()


def query_7(cursor):
    # Finds the top 5 companies that appear the most in the Movies table
    # using a window function
    query = """
        WITH company_counts AS (
            SELECT 
                pc.company_name,
                COUNT(mpc.movie_id) AS movie_count
            FROM ProductionCompanies pc
            JOIN MovieProductionCompanies mpc ON pc.company_id = mpc.company_id
            GROUP BY pc.company_name
        )
        SELECT company_name, movie_count
        FROM (
            SELECT 
                company_name,
                movie_count,
                ROW_NUMBER() OVER (ORDER BY movie_count DESC) AS row_rank
            FROM company_counts
        ) ranked
        WHERE row_rank <= 5;
    """
    cursor.execute(query)
    return cursor.fetchall()