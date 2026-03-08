/*
schema.sql

Purpose
-------
This file defines the relational warehouse schema for storing cleaned
financial market price data.

In the ETL pipeline, this schema represents the destination table
for processed daily price records.

Pipeline role
-------------
Processed CSV data
        ↓
PostgreSQL warehouse table
        ↓
SQL analytics and downstream reporting
*/


-- ---------------------------------------------------------
-- TABLE: fact_prices_daily
-- ---------------------------------------------------------
-- This table stores one row per ticker per trading day.
-- It is the main fact table for financial time-series analysis.

CREATE TABLE IF NOT EXISTS fact_prices_daily (
    
    -- Trading date for the market record
    date DATE NOT NULL,

    -- Stock or asset ticker symbol (e.g., AAPL, MSFT, SPY)
    ticker TEXT NOT NULL,

    -- Opening price of the asset for that date
    open DOUBLE PRECISION NOT NULL,

    -- Highest price reached during the trading day
    high DOUBLE PRECISION NOT NULL,

    -- Lowest price reached during the trading day
    low DOUBLE PRECISION NOT NULL,

    -- Closing price of the asset for that date
    close DOUBLE PRECISION NOT NULL,

    -- Number of shares or units traded during the day
    volume BIGINT NOT NULL,

    -- Composite primary key ensures one record per ticker per date
    PRIMARY KEY (date, ticker)
);


-- ---------------------------------------------------------
-- INDEX: idx_fact_prices_ticker
-- ---------------------------------------------------------
-- This index improves query performance when filtering by ticker.

CREATE INDEX IF NOT EXISTS idx_fact_prices_ticker
ON fact_prices_daily (ticker);


-- ---------------------------------------------------------
-- INDEX: idx_fact_prices_date
-- ---------------------------------------------------------
-- This index improves query performance for date-based filtering.

CREATE INDEX IF NOT EXISTS idx_fact_prices_date
ON fact_prices_daily (date);


-- ---------------------------------------------------------
-- INDEX: idx_fact_prices_ticker_date
-- ---------------------------------------------------------
-- This composite index improves time-series queries such as:
-- "get all dates for one ticker ordered by date"

CREATE INDEX IF NOT EXISTS idx_fact_prices_ticker_date
ON fact_prices_daily (ticker, date);