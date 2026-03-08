"""
clean_prices.py

Purpose
-------
This script reads raw financial price data from the raw data layer,
cleans and standardizes it, and writes the cleaned datasets into the
processed data layer.

In the ETL pipeline, this file represents the "Transform" step.

Pipeline role
-------------
Raw data (data/raw/)
        ↓
Cleaning + standardization
        ↓
Processed data (data/processed/)
"""

# pandas is used for data manipulation and cleaning
import pandas as pd

# os is used for file and folder operations
import os


# ---------------------------------------------------------
# CONFIGURATION SECTION
# ---------------------------------------------------------

# Folder containing raw ingested CSV files
RAW_DATA_FOLDER = "data/raw"

# Folder where cleaned datasets will be stored
PROCESSED_DATA_FOLDER = "data/processed"


# ---------------------------------------------------------
# TRANSFORMATION FUNCTION
# ---------------------------------------------------------

def clean_price_file(file_path: str) -> pd.DataFrame:
    """
    Read and clean a single raw price file.

    Parameters
    ----------
    file_path : str
        Path to the raw CSV file

    Returns
    -------
    pandas.DataFrame
        Cleaned and standardized DataFrame

    Cleaning steps
    --------------
    1. Read CSV
    2. Standardize column names to lowercase
    3. Remove duplicate rows
    4. Convert date column to datetime
    5. Sort rows by date
    6. Drop rows missing key fields
    """

    # Read raw CSV file into DataFrame
    df = pd.read_csv(file_path)

    # Standardize all column names to lowercase
    # This helps avoid case mismatches later during SQL loading
    df.columns = [col.lower() for col in df.columns]

    # Remove exact duplicate rows
    df = df.drop_duplicates()

    # Convert date column from string to datetime format
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Drop rows with missing required fields
    # These are essential for warehouse loading and analytics
    df = df.dropna(subset=["date", "open", "high", "low", "close", "volume", "ticker"])

    # Sort rows by date so time-series analysis is consistent
    df = df.sort_values(by="date")

    # Reset row index after cleaning
    df = df.reset_index(drop=True)

    return df


# ---------------------------------------------------------
# MAIN PIPELINE EXECUTION
# ---------------------------------------------------------

def main():
    """
    Main transformation pipeline.

    Steps
    -----
    1. Ensure processed folder exists
    2. Read each raw file from raw data layer
    3. Clean and standardize it
    4. Save cleaned file into processed data layer
    """

    # Create processed folder if it doesn't exist
    os.makedirs(PROCESSED_DATA_FOLDER, exist_ok=True)

    # Loop through every file in raw folder
    for file_name in os.listdir(RAW_DATA_FOLDER):

        # Build full input path
        input_path = os.path.join(RAW_DATA_FOLDER, file_name)

        # Skip non-CSV files just in case
        if not file_name.endswith(".csv"):
            continue

        # Clean the file
        cleaned_df = clean_price_file(input_path)

        # Build output path in processed folder
        output_path = os.path.join(PROCESSED_DATA_FOLDER, file_name)

        # Save cleaned dataset
        cleaned_df.to_csv(output_path, index=False)

        print(f"[TRANSFORM] Cleaned dataset saved → {output_path}")


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":
    """
    Run transformation step when the script is executed directly.
    """
    main()