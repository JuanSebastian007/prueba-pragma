import psycopg2
from upload_data_in_batches import load_csv_to_postgres
from pyconfig import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, TABLE_DESTINATION

# Function to query minimum, maximum and mean values from a PostgreSQL database table.
def query_min_max_mean():
    """
    This function connects to a PostgreSQL database and performs a query to fetch 
    the minimum, maximum, and average values of a specified column from a table.
    """
    try:
        # Establish a connection to the PostgreSQL database
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
        )
        
        # Create a cursor object
        cursor = connection.cursor()
        column = "price"  # The column we are interested in

        # Query to select minimum, maximum and average value from the specified table column
        cursor.execute(
            f"SELECT MIN({column}), MAX({column}), AVG({column}) FROM {TABLE_DESTINATION}"
        )

        # Get the result of the query
        row = cursor.fetchone()

        # Unpack the result into respective variables and print the results
        minimum_value, maximum_value, mean_value = row
        print(f"Mean Value: {mean_value}")
        print(f"Minimum Value: {minimum_value}")
        print(f"Maximum Value: {maximum_value}")

    # Handle any exception that might occur during the process
    except Exception as e:
        print(f"Error loading CSV files into the database: {str(e)}")

    # Ensure that the cursor and connection objects are closed, regardless of whether an exception occurred or not
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Main driver code
if __name__ == "__main__":
    # List of CSV files to be loaded into the database
    csv_files = ["validation.csv"]
    load_csv_to_postgres(csv_files)  # Load the CSV data into PostgreSQL database
    query_min_max_mean()  # Query the minimum, maximum and mean value from the table 