import mysql.connector
from create_db_script import *
from api_data_retrieve import *
from queries_db_script import *

CSV_NAME = "tmdb_movies_data.csv"
cursor = None


def running_query_1(keyword_for_name):
    results = query_1(cursor, keyword_for_name)
    print_results(results, 1)


def running_query_2(keyword_for_overview):
    results = query_2(cursor, keyword_for_overview)
    print_results(results, 2)


def running_query_3(genre_name):
    results = query_3(cursor, genre_name)
    print_results(results, 3)


def running_query_4():
    results = query_4(cursor)
    print_results(results, 4)


def running_query_5(actor_name):
    results = query_5(cursor, actor_name)
    print_results(results[0], 5)
    print_results(results[1], 5)


def running_query_6(movie_name):
    results = query_6(cursor, movie_name)
    print_results(results, 6)


def running_query_7():
    results = query_7(cursor)
    print_results(results, 7)


def print_results(results, num):
    print("---------- The results of query num: " + str(num) + "----------")
    if not results:
        print("Not Found")
    else:
        for row in results:
            print(row)


def main():
    try:
        conn = mysql.connector.connect(
            host="localhost",
            user="abc",
            password="abc",
            database="abc",
            port="000"
        )
        global cursor
        cursor = conn.cursor()
    except mysql.connector.Error as err:
        print(f"Error: {err.msg}")
        exit(1)

    # --- Database Initialization (Commented Out for Testing) ---
    # user_choice = input("Do you want to rebuild the database? (yes/no): ").strip().lower()
    # if user_choice == "yes":
    #     drop_database(conn, cursor, 'topazsofer')
    #     create_database(conn, cursor, 'topazsofer')
    #     create_db(conn, cursor)
    #     insert_data(conn, cursor)
    # else:
    #     print("Skipping database rebuild.")

    # -----Running examples of the queries----
    # handles Invalid input
    running_query_1("randomtextnotindb")
    running_query_1("")
    running_query_1("love")
    running_query_1("world")
    running_query_1("day")

    running_query_2("randomtextnotindb")
    running_query_2("search")
    running_query_2("happy")
    running_query_2("sad")

    running_query_3("Action")
    running_query_3("Comedy")
    running_query_3("Horror")
    running_query_3("")

    running_query_4()

    running_query_5("Chris Pratt")
    running_query_5("Brad Pitt")
    running_query_5("Angelina Jolie")
    running_query_5("RANDOM")

    running_query_6("Mad Max: Fury Road")
    running_query_6("Brooklyn")
    running_query_6("Joy")

    running_query_7()

    # ---closing connection---
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
