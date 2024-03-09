import psycopg2
import csv
import statistics
from pyconfig import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, TABLE_DESTINATION


def load_csv_to_postgres(csv_files):
    """
    This function loads data from a list of CSV files into a PostgreSQL database.

    Parameters:
    csv_files (list of str): List of CSV file names to load.
    table_name (str): Name of the table in the PostgreSQL database where the data will be loaded.
    connection (psycopg2.extensions.connection): The connection to the PostgreSQL database.

    The function reads each CSV file, skips the header row, and inserts the remaining rows into the specified table.
    It also calculates and prints the mean, minimum, and maximum of the 'price' column (assumed to be at index 1 in each row)
    after each file is loaded. The total number of rows loaded is also printed after each file.

    If an error occurs during the process, an exception is caught and its message is printed.
    Regardless of whether an error occurs, the cursor and the connection to the database are closed before the function ends.

    Note: The function assumes that the data folder is named 'dataPruebaDataEngineer/' and is located in the same directory
    as the Python script that calls this function.
    """
    
    # Function to load data from CSV files to PostgreSQL DB
    folder = "dataPruebaDataEngineer/"  # Directory that contains the CSV files
    total_rows_loaded = 0  # Counter for total rows loaded into the DB
    price_values = []  # List to store 'price' column values for statistics
    try:
        # Establish connection with the PostgreSQL DB
        connection = psycopg2.connect(user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT, database=DB_NAME)
        cursor = connection.cursor() 
        for csv_file in csv_files:
            with open(folder + csv_file, "r") as file:
                reader = csv.reader(file)
                next(reader)  # Skip the header row
                for row in reader:
                    # Replace empty strings in the row with None
                    row = [None if value == "" else value for value in row]
                    # Insert the row into the table in the DB
                    cursor.execute(f"INSERT INTO {TABLE_DESTINATION} VALUES ({', '.join(['%s']*len(row))})", row)
                    total_rows_loaded += 1
                    # If 'price' column value is not None, add it to the list for statistics
                    if row[1] is not None:
                        price_values.append(float(row[1]))
            connection.commit()
            # Calculate and print the statistics for the 'price' column after each file is loaded
            mean_value = statistics.mean(price_values)
            minimum_value = min(price_values)
            maximum_value = max(price_values)
            print(f"Price Statistics:\nMean Value: {mean_value}\nMinimum Value: {minimum_value}\nMaximum Value: {maximum_value}")
            print(f"File {csv_file} loaded successfully. Total rows loaded: {total_rows_loaded}\n")
    except Exception as e:
        print(f"Error loading CSV files into the database: {str(e)}")
    finally:
        # Close the cursor and the connection to the DB
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    csv_files = [
        "2012-1.csv",
        "2012-2.csv",
        "2012-3.csv",
        "2012-4.csv",
        "2012-5.csv",
    ]
    load_csv_to_postgres(csv_files)
