import streamlit as st
import pymysql
import pandas as pd

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Sathya1234',
    database='guvi_project'
)
cursor = connection.cursor()



st.title("SQL Querry Runner")
st.title("Predefined Querries on Crypto, stocks and oils")
st.image(r"C:\Users\sathy\guvi_project\cross_market\homepage.png")
st.markdown("""
    <style>
    .stApp {
        background-color: #A5D6A7 ;
        color: #FFFAFA;
    }
    </style>
""", unsafe_allow_html=True)

# First dropdown
market = st.selectbox(
    "Select Querries",
    ["SQL Querries on  Crypto_Currencies", "SQL Querries on Crypto_Prices", "SQL Querries on Oil_Prices","SQL Querries on Stock_Prices",
     "SQL Querries on Cross_Market_Analysis" ]
)

# Second dropdown depends on first
if market == "SQL Querries on  Crypto_Currencies":
    asset = st.selectbox(
        "Select sub Querries",
        ["Find the top 3 cryptocurrencies by market cap", 
         "List all coins where circulating supply exceeds 90% of total supply",
         "Get coins that are within 10% of their all-time-high (ATH)",
         "Find the average market cap rank of coins with volume above $1B",
         "Get the most recently updated coin"
       ]
    )

elif market == "SQL Querries on Crypto_Prices":
    asset = st.selectbox(
        "Select sub Querries",
        ["Find the highest daily price of Bitcoin in the last 365 days",
         "Calculate the average daily price of Ethereum in the past 1 year", 
         "Show the daily price trend of Bitcoin in January 2025",
         "Find the coin with the highest average price over 1 year",
         "Get the % change in Bitcoin’s price between Sep 2024 and Sep 2025"]
    )

elif market == "SQL Querries on Oil_Prices":
    asset = st.selectbox(
        "Select sub Querries",
        ["Find the highest oil price in the last 5 years",
         "Get the average oil price per year", 
         "Show oil prices during COVID crash (March–April 2020)",
         "Find the lowest price of oil in the last 10 years",
         "Calculate the volatility of oil prices (max-min difference per year)"]
    )

elif market == "SQL Querries on Stock_Prices":
    asset = st.selectbox(
        "Select sub Querries",
        ["Get all stock prices for a given ticker",
         "Find the highest closing price for NASDAQ (^IXIC)", 
         "List top 5 days with highest price difference (high - low) for S&P 500 (^GSPC)",
         "Get monthly average closing price for each ticker",
         "Get average trading volume of NSEI in 2024"]
    )

elif market == "SQL Querries on Cross_Market_Analysis":
    asset = st.selectbox(
        "Select sub Querries",
        ["Compare Bitcoin vs Oil average price in 2025",
         "Check if Bitcoin moves with S&P 500 (correlation idea)", 
         "Compare Ethereum and NASDAQ daily prices for 2025",
         "Find days when oil price spiked and compare with Bitcoin price change",
         "Compare top 3 coins daily price trend vs Nifty (^NSEI)",
         "Compare stock prices (^GSPC) with crude oil prices on the same dates",
         "Correlate Bitcoin closing price with crude oil closing price (same date)",
         "Compare NASDAQ (^IXIC) with Ethereum price trends",
         "Join top 3 crypto coins with stock indices for 2025",
         "stock prices, oil prices, and Bitcoin prices for daily comparison"     
        ]
    )
# answer for SQL Qurries on  Crypto Currencies:
if st.button("Run Querry"):
    if market == "SQL Querries on  Crypto_Currencies":
        if asset == "Find the top 3 cryptocurrencies by market cap":
            cursor.execute('''select id,name,symbol,market_cap from Cryptocurrencies 
            order by market_cap desc
            limit 3''')
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)
  #----------------------
        elif asset == "List all coins where circulating supply exceeds 90% of total supply":
            cursor.execute("""
            SELECT id,symbol,name,current_price,circulating_supply,total_supply FROM Cryptocurrencies
             WHERE circulating_supply > total_supply * 0.9
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)
#--------------------------
        elif asset == "Get coins that are within 10% of their all-time-high (ATH)":
            cursor.execute("""
            SELECT id,symbol,name,current_price,ath FROM Cryptocurrencies
            WHERE current_price <= ath * 0.1
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)

#--------------------------
        elif asset == "Find the average market cap rank of coins with volume above $1B":
            cursor.execute("""
            SELECT name, market_cap_rank, total_volume
            FROM Cryptocurrencies  
            WHERE total_volume >= 1000000000
            order by market_cap_rank
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)
#--------------------------
        else :
            cursor.execute("""
            SELECT * FROM Cryptocurrencies
            order by date desc
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)
#-------------------------- crypto prices ----------- 
    elif market == "SQL Querries on Crypto_Prices":
        if asset =="Find the highest daily price of Bitcoin in the last 365 days":
            cursor.execute("""
            SELECT coin_id,date,max(price_inr) FROM Crypto_prices
            group by date,coin_id
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)
#--------------------------
        elif asset =="Calculate the average daily price of Ethereum in the past 1 year":
            cursor.execute("""
            SELECT coin_id,date,avg(price_inr) FROM Crypto_prices
            where coin_id='ethereum'
            group by date,coin_id
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)
#--------------------------
        elif asset =="Show the daily price trend of Bitcoin in January 2025":
                    cursor.execute("""
                    SELECT coin_id, date, price_inr 
                    FROM Crypto_prices
                    WHERE coin_id = 'bitcoin'
                    AND date BETWEEN '2026-01-01' AND '2026-01-31'
                    ORDER BY date ASC
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="Find the coin with the highest average price over 1 year":
                    cursor.execute("""
                    SELECT coin_id,avg(price_inr) 
                    FROM Crypto_prices group by coin_id
                    order by avg(price_inr) desc
                    limit 1
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        else:
                    cursor.execute("""
                    SELECT
                        feb2025.price_inr  AS price_feb_2025,
                        jan2026.price_inr  AS price_jan_2026,
                        ROUND(
                        ((jan2026.price_inr - feb2025.price_inr)
                            / feb2025.price_inr) * 100
                        , 2) AS pct_change
                    FROM
                        (SELECT price_inr FROM Crypto_prices
                         WHERE coin_id = 'bitcoin'
                        AND DATE_FORMAT(date, '%Y-%m') = '2025-02'
                         ORDER BY date ASC LIMIT 1) AS feb2025,
                
                        (SELECT price_inr FROM Crypto_prices
                         WHERE coin_id = 'bitcoin'
                           AND DATE_FORMAT(date, '%Y-%m') = '2026-01'
                         ORDER BY date ASC LIMIT 1) AS jan2026
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#-----------------------Oil_Prices------
    elif market == "SQL Querries on Oil_Prices":
        if asset =="Find the highest oil price in the last 5 years":
            cursor.execute("""
            SELECT date, price_inr
            FROM oil_prices
            WHERE date >= CURDATE() - INTERVAL 5 YEAR
            ORDER BY price_inr DESC
            LIMIT 1
            """)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            df = pd.DataFrame(rows, columns=columns)
            st.dataframe(df)
#--------------------------
        elif asset =="Get the average oil price per year":
                    cursor.execute("""
                    SELECT YEAR(date) AS year,
                    AVG(price_inr) AS average_price
                    FROM oil_prices
                    GROUP BY YEAR(date)
                    ORDER BY year
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="Show oil prices during COVID crash (March–April 2020)":
                    cursor.execute("""
                    SELECT *  FROM oil_prices 
                    WHERE date BETWEEN '2020-03-01' AND '2020-04-30'
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="Find the lowest price of oil in the last 10 years":
                    cursor.execute("""
                    SELECT min(price_inr) as lowest_price,date
                    FROM oil_prices group by date
                    order by lowest_price
                    limit 1
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="Calculate the volatility of oil prices (max-min difference per year)":
                    cursor.execute("""
                    SELECT year(date) as year , max(price_inr) - min(price_inr) as volatile_price
                    FROM oil_prices group by year(date)
                    order by year
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------stock_prices------------
    elif market == "SQL Querries on Stock_Prices":
        if asset =="Get all stock prices for a given ticker":
                    cursor.execute("""
                    SELECT * 
                    FROM stock_prices
                    WHERE ticker = '^GSPC'
                    ORDER BY date    
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="Find the highest closing price for NASDAQ (^IXIC)":
                    cursor.execute("""
                    SELECT Date, Close
                    FROM stock_prices
                    WHERE ticker = '^IXIC'
                    ORDER BY Close DESC
                    limit 1
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="List top 5 days with highest price difference (high - low) for S&P 500 (^GSPC)":
                    cursor.execute("""
                    SELECT Date,high,low,(high-low) as price_difference
                    FROM stock_prices
                    WHERE ticker = '^GSPC'
                    ORDER BY price_difference DESC
                    limit 5
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="Get monthly average closing price for each ticker":
                    cursor.execute("""
                    SELECT 
                    ticker,
                    MONTH(date) AS month,
                    AVG(Close) AS average_closing_price
                    FROM stock_prices
                    GROUP BY ticker, MONTH(date)
                    ORDER BY ticker, month
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------
        elif asset =="Get average trading volume of NSEI in 2024":
                    cursor.execute("""
                    SELECT avg(volume) as average_volume
                    FROM stock_prices 
                    where ticker='^NSEI' and year(date)= 2024
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]
                    df = pd.DataFrame(rows, columns=columns)
                    st.dataframe(df)
#--------------------------cross market-----------
    else:
        if asset == "Compare Bitcoin vs Oil average price in 2025":
            cursor.execute("""
                SELECT 
                    AVG(cp.price_inr) as avg_bitcoin_price,
                    AVG(op.price_inr) as avg_oil_price 
                FROM crypto_prices cp
                INNER JOIN oil_prices op ON cp.date = op.date
                WHERE cp.coin_id = 'bitcoin'
                AND YEAR(cp.date) = 2025
            """)
    
        elif asset == "Check if Bitcoin moves with S&P 500 (correlation idea)":
            cursor.execute("""
                SELECT c.date,c.price_inr AS bitcoin_price,
                       s.close AS sp500_price,
                       CASE 
                            WHEN (c.price_inr > LAG(c.price_inr) OVER (ORDER BY c.date) 
                              AND s.close > LAG(s.close) OVER (ORDER BY s.date)) 
                            THEN 'Both Up'
                            WHEN (c.price_inr < LAG(c.price_inr) OVER (ORDER BY c.date) 
                              AND s.close < LAG(s.close) OVER (ORDER BY s.date)) 
                            THEN 'Both Down'
                            ELSE 'Not Correlated'
                       END AS correlation
                FROM Crypto_prices c
                INNER JOIN stock_prices s ON c.date = s.date
                WHERE c.coin_id = 'bitcoin'
                AND s.ticker = '^GSPC'
                ORDER BY c.date DESC;
            """)
    
        elif asset == "Compare Ethereum and NASDAQ daily prices for 2025":
            cursor.execute("""
                SELECT c.date,c.price_inr AS ethereum_price,
                       s.close AS NASDAQ_price
                FROM Crypto_prices c
                INNER JOIN stock_prices s ON c.date = s.date
                WHERE c.coin_id = 'ethereum'
                AND s.ticker = '^IXIC'
                ORDER BY c.date DESC;
            """)
    
        elif asset == "Find days when oil price spiked and compare with Bitcoin price change":
            cursor.execute("""
                WITH DailyOilChange AS (
                    SELECT op.date,
                           op.price_inr AS oil_price,
                           (op.price_inr - LAG(op.price_inr) OVER (ORDER BY op.date)) /
                           LAG(op.price_inr) OVER (ORDER BY op.date) * 100 AS oil_pct_change
                    FROM oil_prices op
                ),
                SpikeDays AS (
                    SELECT date, oil_price, oil_pct_change
                    FROM DailyOilChange
                    WHERE oil_pct_change > 5
                )
                SELECT s.date AS spike_date,
                       s.oil_price AS oil_spike_price,
                       ROUND(s.oil_pct_change, 2) AS oil_pct_change,
                       b.price_inr AS btc_price,
                       ROUND((b.price_inr - LAG(b.price_inr) OVER (ORDER BY b.date)) /
                       LAG(b.price_inr) OVER (ORDER BY b.date) * 100, 2) AS btc_pct_change
                FROM SpikeDays s
                INNER JOIN Crypto_prices b ON s.date = b.date
                WHERE b.coin_id = 'bitcoin'
                ORDER BY s.date DESC;
            """)
    
        elif asset == "Compare top 3 coins daily price trend vs Nifty (^NSEI)":
            cursor.execute("""
                WITH TopCoins AS (
                    SELECT coin_id
                    FROM Crypto_prices
                    GROUP BY coin_id
                    ORDER BY AVG(price_inr) DESC
                    LIMIT 3
                )
                SELECT c.coin_id, c.date, c.price_inr AS coin_price,
                       s.close AS Nifty_price
                FROM Crypto_prices c
                INNER JOIN stock_prices s ON c.date = s.date
                INNER JOIN TopCoins tc ON c.coin_id = tc.coin_id
                WHERE s.ticker = '^NSEI'
                ORDER BY c.coin_id, c.date DESC;
            """)
    
        elif asset == "Compare stock prices (^GSPC) with crude oil prices on the same dates":
            cursor.execute("""
                SELECT s.date,
                       s.close AS GSPC_price,
                       op.price_inr AS crude_oil_price
                FROM stock_prices s
                INNER JOIN oil_prices op ON op.date = s.date
                WHERE s.ticker = '^GSPC'
                ORDER BY s.date;
            """)
    
        elif asset == "Correlate Bitcoin closing price with crude oil closing price (same date)":
            cursor.execute("""
                SELECT c.date,
                       c.price_inr AS bitcoin_price,
                       o.price_inr AS crude_oil_price,
                       CASE 
                            WHEN (c.price_inr > LAG(c.price_inr) OVER (ORDER BY c.date)
                              AND o.price_inr > LAG(o.price_inr) OVER (ORDER BY o.date)) 
                            THEN 'Both Up'
                            WHEN (c.price_inr < LAG(c.price_inr) OVER (ORDER BY c.date)
                              AND o.price_inr < LAG(o.price_inr) OVER (ORDER BY o.date)) 
                            THEN 'Both Down'
                            ELSE 'Not Correlated'
                       END AS correlation
                FROM Crypto_prices c
                INNER JOIN oil_prices o ON c.date = o.date
                WHERE c.coin_id = 'bitcoin'
                ORDER BY c.date DESC;
            """)
    
        elif asset == "Compare NASDAQ (^IXIC) with Ethereum price trends":
            cursor.execute("""
                SELECT s.date,
                       s.close AS Nasdaq_price,
                       c.price_inr AS ethereum_price,
                       CASE 
                            WHEN (c.price_inr > LAG(c.price_inr) OVER (ORDER BY c.date)
                              AND s.close > LAG(s.close) OVER (ORDER BY s.date)) 
                            THEN 'Both Up'
                            WHEN (c.price_inr < LAG(c.price_inr) OVER (ORDER BY c.date)
                              AND s.close < LAG(s.close) OVER (ORDER BY s.date)) 
                            THEN 'Both Down'
                            ELSE 'Not Correlated'
                       END AS correlation
                FROM stock_prices s
                INNER JOIN crypto_prices c ON c.date = s.date
                WHERE c.coin_id = 'ethereum'
                AND s.ticker = '^IXIC'
                ORDER BY c.date DESC;
            """)
    
        elif asset == "Join top 3 crypto coins with stock indices for 2025":
            cursor.execute("""
                WITH TopCoins AS (
                    SELECT coin_id
                    FROM Crypto_prices
                    GROUP BY coin_id
                    ORDER BY AVG(price_inr) DESC
                    LIMIT 3
                )
                SELECT s.ticker,
                       c.coin_id,
                       s.date,
                       s.open, s.high, s.low, s.close, s.volume,
                       c.price_inr AS coin_price
                FROM stock_prices s
                INNER JOIN crypto_prices c ON c.date = s.date
                INNER JOIN TopCoins tc ON c.coin_id = tc.coin_id
                WHERE YEAR(c.date) = 2025
                ORDER BY c.coin_id, c.date DESC;
            """)
    
        else:
            cursor.execute("""
                SELECT s.date,
                       s.ticker,
                       s.close AS stock_price,
                       o.price_inr AS oil_price,
                       c.coin_id,
                       c.price_inr AS btc_price
                FROM stock_prices s
                INNER JOIN oil_prices o ON o.date = s.date
                INNER JOIN crypto_prices c ON c.date = o.date
                WHERE c.coin_id = 'bitcoin'
                ORDER BY c.coin_id, c.date;
            """)
    
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        st.dataframe(df)
#-----------------the end---------------
