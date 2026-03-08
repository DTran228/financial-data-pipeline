"""
ingest_prices.py

Purpose
-------
This script downloads historical stock price data from a public financial data source
and stores the raw datasets locally in the raw data lake layer.

In a production data pipeline this step represents the "Extract" phase of ETL.

Pipeline role
-------------
Financial API / CSV source
        ↓
Python ingestion script
        ↓
Raw data storage (data/raw/)
"""

# pandas is used for tabular data processing
import pandas as pd

# requests is used for downloading data from external APIs
import requests

# os is used for file system operations such as creating directories
import os


# ---------------------------------------------------------
# CONFIGURATION SECTION
# ---------------------------------------------------------

# List of financial assets we want to ingest.
# In real pipelines this list could come from a database table or config file.
TICKERS = ["AAPL", "MSFT", "JPM", "SPY"]


# Folder where raw data will be stored.
# Raw layer means unmodified source data.
RAW_DATA_FOLDER = "data/raw"


# ---------------------------------------------------------
# DATA INGESTION FUNCTION
# ---------------------------------------------------------

def download_price_data(ticker: str) -> pd.DataFrame:
    """
    Download daily historical price data for a given ticker.

    Parameters
    ----------
    ticker : str
        Stock ticker symbol (e.g., AAPL, MSFT)

    Returns
    -------
    pandas.DataFrame
        DataFrame containing historical OHLCV data.

    Data fields returned
    --------------------
    date
    open
    high
    low
    close
    volume
    """

    # Stooq provides free CSV downloads for stock price data
    url = f"https://stooq.com/q/d/l/?s={ticker.lower()}&i=d"

    # Read CSV directly into a DataFrame
    df = pd.read_csv(url)

    # Add ticker column for downstream warehouse loading
    df["ticker"] = ticker

    return df


# ---------------------------------------------------------
# MAIN PIPELINE EXECUTION
# ---------------------------------------------------------

def main():
    """
    Main pipeline execution.

    Steps
    -----
    1. Ensure raw data folder exists
    2. Download datasets for each ticker
    3. Save each dataset as CSV in raw data lake
    """

    # Create raw folder if it doesn't exist
    os.makedirs(RAW_DATA_FOLDER, exist_ok=True)

    # Loop through ticker list
    for ticker in TICKERS:

        # Download data from source
        df = download_price_data(ticker)

        # Output file path
        file_path = f"{RAW_DATA_FOLDER}/{ticker}.csv"

        # Save raw dataset
        df.to_csv(file_path, index=False)

        print(f"[INGEST] Saved raw dataset for {ticker} → {file_path}")


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":
    """
    Run ingestion step when the script is executed directly.
    """
    main()