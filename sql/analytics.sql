/*
analytics.sql

Purpose
-------
This file contains analytical SQL queries built on top of the
fact_prices_daily warehouse table.

These queries are designed to demonstrate:
- time-series analysis
- SQL window functions
- financial metrics computation
- warehouse query design

Pipeline role
-------------
PostgreSQL warehouse table
        ↓
Analytical SQL queries
        ↓
Financial insights / dashboard-ready outputs
*/


-- =========================================================
-- QUERY 1: Preview the latest records
-- =========================================================
-- Purpose:
-- Quickly inspect the most recent market records in the warehouse.

CREATE DATABASE finance_db;

SELECT
    date,
    ticker,
    open,
    high,
    low,
    close,
    volume
FROM fact_prices_daily
ORDER BY date DESC, ticker
LIMIT 20;


-- =========================================================
-- QUERY 2: Daily price change
-- =========================================================
-- Purpose:
-- Calculate the day-over-day absolute price change for each ticker.
--
-- Formula:
-- price_change = close_today - close_yesterday

SELECT
    ticker,
    date,
    close,
    close - LAG(close) OVER (
        PARTITION BY ticker
        ORDER BY date
    ) AS daily_price_change
FROM fact_prices_daily
ORDER BY ticker, date;


-- =========================================================
-- QUERY 3: Daily return percentage
-- =========================================================
-- Purpose:
-- Calculate daily return for each ticker.
--
-- Formula:
-- daily_return = (close_today - close_yesterday) / close_yesterday

SELECT
    ticker,
    date,
    close,
    ROUND(
        (
            close - LAG(close) OVER (
                PARTITION BY ticker
                ORDER BY date
            )
        )
        /
        NULLIF(
            LAG(close) OVER (
                PARTITION BY ticker
                ORDER BY date
            ),
            0
        ),
        6
    ) AS daily_return
FROM fact_prices_daily
ORDER BY ticker, date;


-- =========================================================
-- QUERY 4: 5-day moving average
-- =========================================================
-- Purpose:
-- Smooth short-term fluctuations by calculating a rolling average.

SELECT
    ticker,
    date,
    close,
    ROUND(
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ),
        4
    ) AS moving_avg_5d
FROM fact_prices_daily
ORDER BY ticker, date;


-- =========================================================
-- QUERY 5: 20-day rolling volatility proxy
-- =========================================================
-- Purpose:
-- Estimate short-term volatility using the standard deviation of close prices.
--
-- Note:
-- This is a simplified volatility proxy for demonstration purposes.

SELECT
    ticker,
    date,
    close,
    ROUND(
        STDDEV(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ),
        4
    ) AS rolling_volatility_20d
FROM fact_prices_daily
ORDER BY ticker, date;


-- =========================================================
-- QUERY 6: Highest closing price by ticker
-- =========================================================
-- Purpose:
-- Find the maximum closing price observed for each asset.

SELECT
    ticker,
    MAX(close) AS highest_close
FROM fact_prices_daily
GROUP BY ticker
ORDER BY ticker;


-- =========================================================
-- QUERY 7: Average daily trading volume by ticker
-- =========================================================
-- Purpose:
-- Summarize average trading activity for each asset.

SELECT
    ticker,
    ROUND(AVG(volume), 0) AS avg_daily_volume
FROM fact_prices_daily
GROUP BY ticker
ORDER BY avg_daily_volume DESC;


-- =========================================================
-- QUERY 8: Daily ranking by trading volume
-- =========================================================
-- Purpose:
-- Rank tickers by volume for each trading day.

SELECT
    date,
    ticker,
    volume,
    RANK() OVER (
        PARTITION BY date
        ORDER BY volume DESC
    ) AS volume_rank
FROM fact_prices_daily
ORDER BY date, volume_rank;


-- =========================================================
-- QUERY 9: Create an analytics view for daily returns
-- =========================================================
-- Purpose:
-- Build a reusable SQL view for downstream dashboarding or reporting.

CREATE OR REPLACE VIEW vw_daily_returns AS
SELECT
    ticker,
    date,
    close,
    ROUND(
        (
            close - LAG(close) OVER (
                PARTITION BY ticker
                ORDER BY date
            )
        )
        /
        NULLIF(
            LAG(close) OVER (
                PARTITION BY ticker
                ORDER BY date
            ),
            0
        ),
        6
    ) AS daily_return
FROM fact_prices_daily;


-- =========================================================
-- QUERY 10: Create an analytics view for 5-day moving average
-- =========================================================
-- Purpose:
-- Build a reusable SQL view for trend analysis.

CREATE OR REPLACE VIEW vw_moving_avg_5d AS
SELECT
    ticker,
    date,
    close,
    ROUND(
        AVG(close) OVER (
            PARTITION BY ticker
            ORDER BY date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ),
        4
    ) AS moving_avg_5d
FROM fact_prices_daily;