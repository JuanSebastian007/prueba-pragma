import psycopg2
from psycopg2 import Error
from pyconfig import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, TABLE_DESTINATION


def create_table():
    """
    This function is responsible for creating a table in the PostgreSQL database.
    The table structure includes three columns: timestamp, price, and user_id.
    """
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

        # SQL query for creating a table if it does not exist already
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_DESTINATION} (
            timestamp TIMESTAMP,
            price NUMERIC,
            user_id NUMERIC
        );
        """

        # The SQL command is executed here
        cursor.execute(create_table_query)
        # Any changes are committed to the database here
        connection.commit()
        print("Table created successfully")

    except (Exception, Error) as error:
        # Any exceptions that occur during execution are caught and printed here
        print("Error creating table:", error)

    finally:
        # The database connection is always closed after use
        if connection:
            cursor.close()
            connection.close()
            print("Closed connection")


# The create_table function is called here when the script is run directly
if __name__ == "__main__":
    create_table()