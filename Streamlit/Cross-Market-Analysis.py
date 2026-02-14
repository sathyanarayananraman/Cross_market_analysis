import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Page configuration
st.set_page_config(page_title="Cross-Market Overview", layout="wide", initial_sidebar_state="expanded")

# Date range selector
st.markdown("**Date Range**")
start_date = st.date_input("Start Date", datetime.now() - timedelta(days=30))
end_date = st.date_input("End Date", datetime.now())
start_date_str = start_date.strftime("%Y-%m-%d")
end_date_str = end_date.strftime("%Y-%m-%d")

# Main content
st.title(" Cross-Market Overview")
st.markdown(f"*Crypto / Oil / Stock Market  -  SQL Powered analytics*")

#sql connection:
import pymysql

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Sathya1234',
    database='guvi_project'
)
cursor = connection.cursor()
#SQL Querry
# --------- CRYPTO ---------
cursor.execute("""
    SELECT AVG(current_price) 
    FROM Cryptocurrencies
    WHERE date BETWEEN %s AND %s
""", (start_date_str, end_date_str))

result = cursor.fetchone()
crypto_price = round(result[0], 2) if result[0] is not None else 0
# --------- OIL ---------
cursor.execute("""
    SELECT AVG(price_inr)   
    FROM Oil_prices 
    WHERE date BETWEEN %s AND %s
""", (start_date_str, end_date_str))

result = cursor.fetchone()
oil_price = round(result[0], 2) if result[0] is not None else 0
# --------- S&P 500 ---------
cursor.execute("""
    SELECT AVG(close)  
    FROM Stock_prices 
    WHERE ticker = '^GSPC'
    AND date BETWEEN %s AND %s
""", (start_date_str, end_date_str))

result = cursor.fetchone()
GSPC_price = round(result[0], 2) if result[0] is not None else 0
# --------- NIFTY ---------
cursor.execute("""
    SELECT AVG(close)  
    FROM Stock_prices 
    WHERE ticker = '^NSEI'
    AND date BETWEEN %s AND %s
""", (start_date_str, end_date_str))

result = cursor.fetchone()
NSEI_price = round(result[0], 2) if result[0] is not None else 0

# Metrics row
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="metric-label"> Crypto (AVG) </div>', unsafe_allow_html=True)
    st.markdown( f'<div class="metric-value">{crypto_price}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="metric-label"> Oil (AVG)</div>', unsafe_allow_html=True)
    st.markdown( f'<div class="metric-value">{oil_price}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="metric-label"> S&P 500 (AVG) </div>', unsafe_allow_html=True)
    st.markdown( f'<div class="metric-value">{GSPC_price}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.markdown('<div class="metric-label"> NIFTY (AVG) </div>', unsafe_allow_html=True)
    st.markdown( f'<div class="metric-value">{NSEI_price}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Daily Market Snapshot section
st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
st.markdown('<p class="section-title">Daily Market Snapshot</p>', unsafe_allow_html=True)

crypto_query = """
    SELECT date, AVG(current_price) AS avg_price
    FROM Cryptocurrencies
    WHERE date BETWEEN %s AND %s
    GROUP BY date
    ORDER BY date
"""
cursor.execute(crypto_query, (start_date_str, end_date_str))

crypto_data = cursor.fetchall()
crypto_trend_df = pd.DataFrame(crypto_data, columns=["Date", "Average Price"])

if not crypto_trend_df.empty:
    crypto_trend_df["Date"] = pd.to_datetime(crypto_trend_df["Date"])
    st.subheader(f"Crypto Trend from {start_date_str} to {end_date_str}")
    st.line_chart(crypto_trend_df.set_index("Date"))
else:
    st.warning("No Crypto data available for selected date range")


st.markdown('</div>', unsafe_allow_html=True)

# Footer
st.markdown("<br><br>", unsafe_allow_html=True)
st.markdown("---")
st.caption("Cross-Market Overview Dashboard | Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))