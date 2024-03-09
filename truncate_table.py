import psycopg2
from psycopg2 import Error
from pyconfig import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, TABLE_DESTINATION


def truncate_table():
    
    try:
        # Connection to the database is established here
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT, 
            database=DB_NAME,
        )

        # A cursor object is created here to interact with the PostgreSQL database
        cursor = connection.cursor()

        # Build and execute the table truncation query
        truncate_query = f"TRUNCATE TABLE {TABLE_DESTINATION};"

        # The SQL command is executed here
        cursor.execute(truncate_query)
        # Any changes are committed to the database here
        connection.commit()
        print("Table truncation successfully")

    except (Exception, Error) as error:
        # Any exceptions that occur during execution are caught and printed here
        print("Error truncation table:", error)

    finally:
        # The database connection is always closed after use
        if connection:
            cursor.close()
            connection.close()
            print("Closed connection")


# The create_table function is called here when the script is run directly
if __name__ == "__main__":
    truncate_table()