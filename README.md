# 💰🛢️📈 Cross-Market Analysis: Crypto, Oil & Stocks

### SQL-Powered Financial Analytics Platform with Streamlit

---

## 📌 Project Overview

This project builds a **cross-market analytics platform** that compares:

* 🪙 Top Cryptocurrencies (Bitcoin, Ethereum, etc.)
* 🛢️ WTI Crude Oil Prices
* 📊 Global Stock Indices (S&P 500, NASDAQ, NIFTY)

Using **Python, SQL, and Streamlit**, the platform collects, transforms, stores, analyzes, and visualizes financial market data to uncover patterns, correlations, and relative performance across asset classes.

The goal is to answer:

> Is crypto behaving like digital gold, or is it an entirely different asset class?

---

## 🎯 Skills Demonstrated

* API Integration
* ETL Pipeline Development
* Data Cleaning & Transformation
* Relational Database Design
* Advanced SQL Analytics
* Multi-Table Joins
* Financial Data Analysis
* Streamlit Dashboard Development
* Business Intelligence (BI) Reporting

---

## 🌍 Domain

**Financial Analytics | Business Intelligence | Data Engineering**

---

# ❓ Problem Statement

Cryptocurrency markets are often compared with traditional assets like oil and stock indices.

But:

* Does Bitcoin move with the stock market?
* Does oil price volatility impact crypto?
* Can traditional assets hedge crypto risk?

This project creates a **SQL-powered analytical framework** to explore cross-market relationships from 2020–2026.

---

# 💼 Real-World Business Use Cases

1. **Investment Research**
   Evaluate if crypto correlates with oil or equities.

2. **Risk Management**
   Measure volatility across asset classes.

3. **Macro-Economic Analysis**
   Study cross-market behavior during global events.

4. **Cross-Market Trading Strategies**
   Test hedging and correlation hypotheses.

5. **Educational BI Tool**
   Demonstrates ETL + SQL + Dashboard integration in one project.

---

# 🏗️ Project Architecture

```
API / Dataset Sources
        ↓
Data Extraction (Python)
        ↓
Data Cleaning (Pandas)
        ↓
SQL Database (Relational Schema)
        ↓
Advanced SQL Queries
        ↓
Streamlit Dashboard (Visualization Layer)
```

---

# 📥 Data Sources

### 🪙 Cryptocurrency Data

* Source: CoinGecko API
* Data:

  * Coin metadata
  * 1-year historical prices (Top 3 coins by market cap)

---

### 🛢️ Oil Prices

* WTI Crude Oil Daily Prices
* Period: 2020 – 2026

---

### 📊 Stock Market Data

* Tickers:

  * ^GSPC (S&P 500)
  * ^IXIC (NASDAQ)
  * ^NSEI (NIFTY)
* Period: 2020 – 2025

---

# 🗄️ Database Design

## 1️⃣ Table: `cryptocurrencies`

| Column          | Type           | Description        |
| --------------- | -------------- | ------------------ |
| id              | VARCHAR(50) PK | CoinGecko ID       |
| symbol          | VARCHAR(10)    | Coin symbol        |
| name            | VARCHAR(100)   | Coin name          |
| market_cap_rank | INT            | Rank by market cap |

---

## 2️⃣ Table: `crypto_prices`

| Column    | Type           | Description                   |
| --------- | -------------- | ----------------------------- |
| coin_id   | VARCHAR(50) FK | Reference to cryptocurrencies |
| date      | DATE           | Price date                    |
| price_usd | DECIMAL(18,6)  | Daily price                   |

---

## 3️⃣ Table: `oil_prices`

| Column    | Type          |
| --------- | ------------- |
| date      | DATE (PK)     |
| price_usd | DECIMAL(18,6) |

---

## 4️⃣ Table: `stock_prices`

| Column | Type          |
| ------ | ------------- |
| date   | DATE          |
| ticker | VARCHAR(20)   |
| open   | DECIMAL(18,6) |
| high   | DECIMAL(18,6) |
| low    | DECIMAL(18,6) |
| close  | DECIMAL(18,6) |
| volume | BIGINT        |

---

# 🔄 ETL Workflow

### Step 1: Extract

* Fetch API data
* Load CSV datasets

### Step 2: Transform

* Clean null values
* Convert timestamps
* Normalize date formats
* Filter required date ranges

### Step 3: Load

* Create SQL tables
* Insert cleaned data using Python DB connectors

---

# 🔎 SQL Analytics

## 🔹 Cryptocurrency Analysis

* Top 3 coins by market cap
* Coins near ATH
* Average market cap rank
* Supply ratio analysis

## 🔹 Crypto Price Analysis

* Highest BTC price in last year
* Ethereum average price
* Monthly trend analysis
* % price change YoY

## 🔹 Oil Analysis

* Highest oil price (5 years)
* COVID crash analysis
* Yearly volatility

## 🔹 Stock Analysis

* Monthly average closing prices
* Highest NASDAQ close
* Top 5 volatile days

---

# 🔗 Cross-Market Join Analysis

* Bitcoin vs Oil (same date comparison)
* BTC vs S&P 500 correlation idea
* Ethereum vs NASDAQ trend comparison
* Oil spike vs BTC movement
* Multi-join: Crypto + Oil + Stocks daily comparison

This demonstrates advanced SQL JOIN capabilities.

---

# 📊 Streamlit Application

The dashboard contains **3 main pages**:

---

## 🔹 Page 1: Filters & Market Snapshot

* Date range selector
* Average:

  * Bitcoin price
  * Oil price
  * S&P 500 close
  * NIFTY close
* Combined daily comparison table (SQL JOIN)

---

## 🔹 Page 2: SQL Query Runner

* Dropdown for predefined SQL queries
* Executes query on button click
* Displays results in table format
* Demonstrates live SQL analytics

---

## 🔹 Page 3: Crypto Trend Analysis

* Select coin
* Apply date filter
* View:

  * Line chart (daily trend)
  * Tabular data

---

# 🛠️ Tech Stack

* Python
* Pandas
* SQL (MySQL / PostgreSQL / SQLite)
* Streamlit
* CoinGecko API
* Yahoo Finance
* Relational Database Design
* ETL Pipeline

---

# 🚀 Installation Guide

## 1️⃣ Clone Repository

```bash
git clone https://github.com/yourusername/cross-market-analysis.git
cd cross-market-analysis
```

---

## 2️⃣ Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate   # Mac/Linux
venv\Scripts\activate      # Windows
```

---

## 3️⃣ Install Requirements

```bash
pip install -r requirements.txt
```

---

## 4️⃣ Setup Database

* Create database
* Run schema creation script
* Run data ingestion script

---

## 5️⃣ Run Streamlit App

```bash
streamlit run app.py
```

---

# 📈 Expected Outcomes

✅ Clean relational SQL database
✅ Automated ETL pipeline
✅ Cross-market analytical queries
✅ Interactive financial dashboard
✅ Multi-asset comparison framework

---

# 📌 Key Learnings

* Designing scalable SQL schemas
* Handling financial time-series data
* Performing cross-table joins
* Integrating backend SQL with frontend dashboards
* Building complete BI solutions end-to-end

---

# 👨‍💻 Author

**Sathya**
Data Analyst | Python | SQL

---

If you want, I can now also:

* 🔥 Create a **professional GitHub project description**
* 📊 Create a **LinkedIn project post**
* 🎤 Create a **Capstone explanation script**
* 🏆 Create a **resume-ready project description**
* 🗂️ Generate a clean folder structure for GitHub**

Just tell me which one you want.
