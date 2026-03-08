# Financial Market Data Pipeline

## Overview

This project implements an end-to-end data engineering pipeline for financial market data.

The system ingests historical stock price data, processes and cleans the datasets, and loads them into a PostgreSQL warehouse for analytical querying.

The goal of this project is to demonstrate practical data engineering skills including:

- ETL pipeline design
- data lake layer separation
- relational warehouse schema design
- SQL analytics using window functions
- financial time-series analysis

---

## Architecture


Financial Data Source (CSV / API)
↓
Python Ingestion Script
↓
Raw Data Layer (data/raw/)
↓
Data Cleaning & Transformation
↓
Processed Data Layer (data/processed/)
↓
PostgreSQL Data Warehouse
↓
SQL Analytics Queries


---

## Tech Stack

Python  
Pandas  
PostgreSQL  
SQL  
Git / GitHub  

Optional future extensions:

AWS S3  
Docker  
Airflow  

---

## Project Structure


financial-data-pipeline
│
├── data
│ ├── raw
│ └── processed
│
├── docs
│
├── sql
│ ├── schema.sql
│ └── analytics.sql
│
├── src
│ ├── ingest
│ │ └── ingest_prices.py
│ │
│ ├── transform
│ │ └── clean_prices.py
│ │
│ ├── load
│ │ └── load_to_postgres.py
│
├── tests
│
├── requirements.txt
└── README.md


---

## Pipeline Stages

### 1 Ingestion

The ingestion script downloads historical price data for selected tickers.

Output files are stored in the raw data layer.


data/raw/AAPL.csv
data/raw/MSFT.csv


Script:


src/ingest/ingest_prices.py


---

### 2 Transformation

Raw datasets are cleaned and standardized.

Cleaning steps include:

- removing duplicate records
- converting date formats
- dropping incomplete rows
- sorting time-series records

Output files are written to the processed data layer.


data/processed/AAPL.csv


Script:


src/transform/clean_prices.py


---

### 3 Warehouse Loading

Cleaned datasets are loaded into the PostgreSQL warehouse table.

The table schema is defined in:


sql/schema.sql


Data loading script:


src/load/load_to_postgres.py


Duplicate rows are prevented using a composite primary key and conflict handling.

---

### 4 Analytical Queries

Analytical SQL queries are stored in:


sql/analytics.sql


Example analytics include:

- daily price change
- daily returns
- moving averages
- rolling volatility
- trading volume ranking

Views are also created for reusable analytics.

---

## Example SQL Query

Daily return calculation:


SELECT
ticker,
date,
close,
(close - LAG(close) OVER (PARTITION BY ticker ORDER BY date))
/ LAG(close) OVER (PARTITION BY ticker ORDER BY date)
AS daily_return
FROM fact_prices_daily;


---

## How to Run the Pipeline

### Step 1 Install dependencies


pip install -r requirements.txt


---

### Step 2 Run ingestion


python src/ingest/ingest_prices.py


---

### Step 3 Run transformation


python src/transform/clean_prices.py


---

### Step 4 Create warehouse table

Run:


sql/schema.sql


inside PostgreSQL.

---

### Step 5 Load data into warehouse


python src/load/load_to_postgres.py


---

## Future Improvements

Possible enhancements include:

- AWS S3 data lake integration
- Airflow DAG orchestration
- Docker containerization
- automated data quality checks
- streaming ingestion with Kafka

---

## Learning Outcomes

This project demonstrates core data engineering capabilities:

- designing ETL pipelines
- handling structured financial datasets
- writing analytical SQL queries
- building reproducible data workflows
- structuring data engineering projects for maintainability