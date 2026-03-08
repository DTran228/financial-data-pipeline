"""
load_to_postgres.py

Purpose
-------
This script loads cleaned financial price data from the processed data layer
into a PostgreSQL warehouse table.

In the ETL pipeline, this file represents the "Load" step.

Pipeline role
-------------
Processed CSV data
        ↓
Python loading script
        ↓
PostgreSQL warehouse table
"""

# pandas is used to read processed CSV files
import pandas as pd

# psycopg2 is the PostgreSQL database adapter for Python
import psycopg2

# os is used for file system operations
import os


# ---------------------------------------------------------
# CONFIGURATION SECTION
# ---------------------------------------------------------

# Folder containing cleaned CSV files
PROCESSED_DATA_FOLDER = "data/processed"

# PostgreSQL connection settings
# Replace these values with your actual database credentials
DB_CONFIG = {
    "host": "localhost",
    "database": "finance_db",
    "user": "postgres",
    "password": "password"
}


# ---------------------------------------------------------
# DATABASE CONNECTION
# ---------------------------------------------------------

def get_connection():
    """
    Create and return a PostgreSQL database connection.

    Returns
    -------
    psycopg2 connection object
        Active database connection
    """
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"]
    )


# ---------------------------------------------------------
# LOAD FUNCTION
# ---------------------------------------------------------

def load_file_to_postgres(file_path: str, cursor):
    """
    Load one processed CSV file into the fact_prices_daily table.

    Parameters
    ----------
    file_path : str
        Path to processed CSV file
    cursor : psycopg2 cursor
        Active database cursor used for executing SQL inserts

    Steps
    -----
    1. Read processed CSV
    2. Iterate through rows
    3. Insert rows into PostgreSQL
    4. Skip duplicates using ON CONFLICT
    """

    # Read processed CSV into DataFrame
    df = pd.read_csv(file_path)

    # Loop through each row in the dataset
    for _, row in df.iterrows():

        # Insert each record into the warehouse table
        cursor.execute(
            """
            INSERT INTO fact_prices_daily
            (date, ticker, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, ticker) DO NOTHING
            """,
            (
                row["date"],
                row["ticker"],
                row["open"],
                row["high"],
                row["low"],
                row["close"],
                row["volume"]
            )
        )


# ---------------------------------------------------------
# MAIN PIPELINE EXECUTION
# ---------------------------------------------------------

def main():
    """
    Main load pipeline.

    Steps
    -----
    1. Connect to PostgreSQL
    2. Loop through processed CSV files
    3. Load each file into warehouse table
    4. Commit transaction
    5. Close connection
    """

    # Create database connection
    conn = get_connection()

    # Create cursor for executing SQL statements
    cursor = conn.cursor()

    # Loop through processed files
    for file_name in os.listdir(PROCESSED_DATA_FOLDER):

        # Skip non-CSV files just in case
        if not file_name.endswith(".csv"):
            continue

        # Build full file path
        file_path = os.path.join(PROCESSED_DATA_FOLDER, file_name)

        # Load file into database
        load_file_to_postgres(file_path, cursor)

        print(f"[LOAD] Loaded data from {file_path}")

    # Commit all inserts to database
    conn.commit()

    # Close cursor and connection
    cursor.close()
    conn.close()

    print("[LOAD] All processed files loaded into PostgreSQL successfully.")


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":
    """
    Run load step when the script is executed directly.
    """
    main()