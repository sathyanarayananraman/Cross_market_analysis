#libraries used:
import pandas as pd
import pymysql
import yfinance as yf


#step 1 : data collection using API
import requests
import time

all_data = []
for page in range(1, 6):
    url = ("https://api.coingecko.com/api/v3/coins/markets"
    f"?vs_currency=inr&per_page=250&order=market_cap_desc&page={page}")
    response = requests.get(url)
    print(f"Page {page}  Status {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        all_data.extend(data)
    elif response.status_code == 429:
        print("Rate limited. Stopping loop.")
        print("Rate limit headers:", response.headers)
        print("Response:", response.text)
        break
    else:
        print("Other error:", response.status_code)
        break
    time.sleep(20)
    
records=[]
for i in all_data:
    records.append({
        'market_cap_rank': i.get('market_cap_rank'),
        'id': i.get('id'),
        'name': i.get('name'),
        'symbol': i.get('symbol')[:4].upper(),
        'current_price': i.get('current_price',5),
        'market_cap': i.get('market_cap',5),
        'total_volume': i.get('total_volume',5),
        'circulating_supply': i.get('circulating_supply',5),
        'total_supply': i.get('total_supply',5),
        'ath': i.get('ath'),
        'atl': i.get('atl'),
        'date': i.get('last_updated')})


coins=pd.DataFrame(records)
coins.fillna(0,inplace=True)
coins['symbol'] = coins['symbol'].str.strip()
coins['date']=pd.to_datetime(coins["date"]).dt.date
coins.sort_values(by="market_cap_rank")
#--------------------------------

#SQL Connection:

import pymysql

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Sathya1234',
)
cursor = connection.cursor()
print(" Connected using pymysql")

#creating DB:
cursor.execute('create  database guvi_project')

#Table Cryptocurrencies
cursor.execute('use guvi_project')
cursor.execute("""CREATE TABLE if not exists Cryptocurrencies (
market_cap_rank INT,
id varchar(50) PRIMARY KEY, 
symbol varchar(10),
name VARCHAR(100),
current_price DECIMAL(18, 6),
market_cap BIGINT,
total_volume BIGINT,
circulating_supply DECIMAL(20, 6),
total_supply DECIMAL(20, 6),
ath DECIMAL(18, 6),
atl DECIMAL(18, 6),
date DATE
)""")

#Delete Command (use if required to avoid duplicate entries while run multiple times)
cursor.execute("DELETE FROM your_table")

#Data Ingestion to DB
cursor.execute('use guvi_project')
successful = 0
failed = 0

for index, row in coins.iterrows():
    try:
        cursor.execute("""
            INSERT INTO Cryptocurrencies 
            (market_cap_rank, id, symbol, name, current_price, market_cap, 
             total_volume, circulating_supply, total_supply, ath, atl, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            row['market_cap_rank'],
            row['id'],
            row['symbol'],
            row['name'],
            row['current_price'],
            row['market_cap'],
            row['total_volume'],
            row['circulating_supply'],
            row['total_supply'],
            row['ath'],
            row['atl'],
            row['date']
        ))
        successful += 1
    except Exception as e:
        failed += 1
        print(f"Error inserting {row['id']}: {e}")

connection.commit()
print(f"\n✓ Successfully inserted: {successful}")
print(f"✗ Failed: {failed}")
#---------------------------------------

#Create Table: Crypto_prices(top 5 historical prices)

cursor.execute('use guvi_project')
cursor.execute("""CREATE TABLE if not exists Crypto_prices (
coin_id varchar(50),
date DATE,
price_inr DECIMAL(18, 6),
FOREIGN KEY(coin_id) references Cryptocurrencies(id)
)""")

#Fetching Top_5 Coins Historical data

all_history = []  
for coin in records[:5]:
    coin_id = coin['id']
    market_cap_rank = coin['market_cap_rank']

    url = f'https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart?vs_currency=inr&days=365'
    responses = requests.get(url)
    data = responses.json()
    #print(json.dumps(data)) #remove this line when execute

    if 'prices' in data:
        pricedf = pd.DataFrame(data["prices"], columns=["timestamp", "price"]) 
        pricedf["market_cap_rank"] = market_cap_rank
        pricedf["coin_id"] = coin_id
        pricedf["date"] = pd.to_datetime(pricedf["timestamp"], unit='ms').dt.date
        all_history.append(pricedf[["market_cap_rank","coin_id", "date", "price"]])
    else:
        print('Not Found')
    time.sleep(10)

final_df = pd.concat(all_history, ignore_index=True).sort_values(by=['date'], ascending=[True])

#data ingestion
cursor.execute('use guvi_project')
successful = 0
failed = 0

for index, row in final_df.iterrows():
    try:
        cursor.execute("""
            INSERT IGNORE INTO   Crypto_prices 
            (coin_id, date, price_inr)
            VALUES (%s, %s, %s)
        """, (
            row['coin_id'],
            row['date'],
            row['price'],
        
        ))
        successful += 1
    except Exception as e:
        failed += 1
        print(f"Error inserting {row['coin_id']}: {e}")

connection.commit()
print(f"\n✓ Successfully inserted: {successful}")
print(f"✗ Failed: {failed}")

#----------------------------------------------------------------

#create Table: Oil_prices

#Fetching Oil prices
import pandas as pd
oil_df = pd.read_csv("https://raw.githubusercontent.com/datasets/oil-prices/main/data/wti-daily.csv")
odf = oil_df[(oil_df['Date'] > '2019-12-31') & (oil_df['Date'] < '2026-01-31')]
odf.to_csv("oil_prices.csv",index = True)
odf.sort_values(by='Date',ascending=0)


cursor.execute('use guvi_project')
cursor.execute("""CREATE TABLE if not exists Oil_prices (
date Date PRIMARY KEY,
Price_inr DECIMAL(18, 6)
)""")

# Data ingestion
cursor.execute('USE guvi_project')
successful = 0
failed = 0

for index, row in odf.iterrows():
    try:
        cursor.execute("""
            INSERT IGNORE INTO Oil_prices 
            (date, price_inr)
            VALUES (%s, %s)
        """, (
            row['Date'],     
            row['Price']      
        ))
        successful += 1
    except Exception as e:
        failed += 1
        print(f"Error inserting {row['Date']}: {e}")

connection.commit()
print(f"\n✓ Successfully inserted: {successful}")
print(f"✗ Failed: {failed}")

#-------------------------------------------
#Stock Prices (Yahoo Finance API)

import pymysql

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Sathya1234',
    database='guvi_project'
)
cursor = connection.cursor()
print("✓ Connected using pymysql")


import yfinance as yf
import pandas as pd

# Choose indices
tickers = ["^GSPC", "^IXIC", "^NSEI"]
start_date = "2020-01-01"
end_date = "2026-01-12"

# Download historical data
stock_df = yf.download(tickers, start=start_date, end=end_date, group_by="ticker")
stock_df.fillna(0,inplace=True)

# Reshape: Stack each ticker into a flat DataFrame with a 'ticker' column
frames = []
for ticker in tickers:
    df = stock_df[ticker].copy().reset_index() 
    df['ticker'] = ticker
    frames.append(df)
flat_df = pd.concat(frames, ignore_index=True)
print(flat_df.head())


#Table: Stock_prices
cursor.execute('use guvi_project')
cursor.execute("""CREATE TABLE if not exists Stock_prices (
Date DATE,
Open DECIMAL(18, 6),
High DECIMAL(18, 6),
Low DECIMAL(18, 6),
Close DECIMAL(18, 6),
Volume Bigint,
ticker varchar(20)
)""")

# Data ingestion
cursor.execute('USE guvi_project')
successful = 0
failed = 0

for _, row in flat_df.iterrows():
    try:
        cursor.execute("""
            INSERT IGNORE INTO Stock_prices 
            (Date, Open, High, Low, Close, Volume, ticker)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            row['Date'],
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            int(row['Volume']),   # ✅ Cast to int for BIGINT safety
            row['ticker']
        ))
        successful += 1
    except Exception as e:
        failed += 1
        print(f"Error inserting {row['Date']} ({row['ticker']}): {e}")

connection.commit()
print(f"\n✓ Successfully inserted: {successful}")
print(f"✗ Failed: {failed}")
#--------------------------------Sql Querries on crytpcurrencies:-------------------


import pymysql

connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Sathya1234',
    database='guvi_project'
)
cursor = connection.cursor()
print("✓ Connected using pymysql")

#Find the top 3 cryptocurrencies by market cap:
cursor.execute('''select id,name,symbol,market_cap from Cryptocurrencies 
               order by market_cap desc
               limit 3''')
print(cursor.fetchall())


#List all coins where circulating supply exceeds 90% of total supply.
cursor.execute("""
    SELECT id,symbol,name,current_price,circulating_supply,total_supply FROM Cryptocurrencies
    WHERE circulating_supply > total_supply * 0.9
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)


#Get coins that are within 10% of their all-time-high (ATH).
cursor.execute("""
    SELECT id,symbol,name,current_price,ath FROM Cryptocurrencies
    WHERE current_price <= ath * 0.1

""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Find the average market cap rank of coins with volume above $1B.
cursor.execute("""
    SELECT 
        AVG(market_cap_rank) AS avg_market_cap_rank
    FROM Cryptocurrencies  
    WHERE total_volume >= 1000000000
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Get the most recently updated coin.
cursor.execute("""
    SELECT id,symbol,name FROM Cryptocurrencies
    order by date desc
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df

#---------------------------------------------Sql qurries on crypto prices----------------
      
#Find the highest daily price of Bitcoin in the last 365 days:
cursor.execute("""
    SELECT coin_id,date,max(price_inr) FROM Crypto_prices
    group by date,coin_id
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Calculate the average daily price of Ethereum in the past 1 year:
cursor.execute("""
    SELECT coin_id,date,avg(price_inr) FROM Crypto_prices
    where coin_id='ethereum'
    group by date,coin_id
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Show the daily price trend of Bitcoin in January 2026. (or change the month and year according you your data):
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
print(df)

#Find the coin with the highest average price over 1 year:
cursor.execute("""
    SELECT coin_id,avg(price_inr) 
    FROM Crypto_prices group by coin_id
    order by avg(price_inr) desc
     limit 1
""")

rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Get the % change in Bitcoin’s price between feb 2025 and jan 2025:
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
print(df)

#-----------------------------------Querries on Oil_price--------------

#Find the highest oil price in the last 5 years.
cursor.execute("""
SELECT date,max(price_inr)
FROM oil_prices group by date
order by max(price_inr) desc
limit 1
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Get the average oil price per year:
cursor.execute("""
SELECT YEAR(date) AS year,
AVG(price_inr) AS average_price
FROM oil_prices
GROUP BY YEAR(date)
ORDER BY year;
 
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Show oil prices during COVID crash (March–April 2020):

cursor.execute("""
    SELECT *
    FROM oil_prices 
    WHERE date BETWEEN '2020-03-01' AND '2020-04-30'
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Find the lowest price of oil in the last 10 years:
cursor.execute("""
    SELECT min(price_inr) as lowest_price,date
    FROM oil_prices group by date
    order by lowest_price
    limit 1

   """)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Calculate the volatility of oil prices (max-min difference per year):
cursor.execute("""
    SELECT year(date) as year , max(price_inr) - min(price_inr) as volatile_price
    FROM oil_prices group by year(date)
    order by year

   """)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)
#--------------------------------------querrie on stock prices:-------------
#Get all stock prices for a given ticker:
cursor.execute("""
    SELECT * 
    FROM stock_prices
    WHERE ticker = '^GSPC'
    ORDER BY date
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Find the highest closing price for NASDAQ (^IXIC)
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
print(df)

#List top5 days with highest price difference (high - low) for S&P 500 (^GSPC):
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
print(df)

#Get monthly average closing price for each ticker:
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
print(df)

#Get average trading volume of NSEI in 2024:

cursor.execute("""
    SELECT avg(volume) as average_volume
    FROM stock_prices 
    where ticker='^NSEI' and year(date)= 2024
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)
# ----------------------Join Queries (Cross-Market Analysis)--------------------------------

#Compare Bitcoin vs Oil average price in 2025:
import pandas as pd
cursor.execute(""" 
    SELECT 
        AVG(cp.price_inr) as avg_bitcoin_price,
        AVG(op.price_inr) as avg_oil_price 
    FROM crypto_prices cp
    INNER JOIN oil_prices op ON cp.date = op.date
    WHERE cp.coin_id = 'bitcoin'
        AND YEAR(cp.date) = 2025
""")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Check if Bitcoin moves with S&P 500 (correlation idea):
cursor.execute(""" 
SELECT
    c.date,c.price_inr AS bitcoin_price,
    s.close AS sp500_price,
    CASE 
        WHEN (c.price_inr > LAG(c.price_inr) OVER (ORDER BY c.date) AND 
              s.close > LAG(s.close) OVER (ORDER BY s.date)) 
        THEN 'Both Up '
        WHEN (c.price_inr < LAG(c.price_inr) OVER (ORDER BY c.date) AND 
              s.close < LAG(s.close) OVER (ORDER BY s.date)) 
        THEN 'Both Down '
        ELSE 'Not Correlated '
    END AS correlation
FROM  Crypto_prices c
INNER JOIN
    stock_prices s ON c.date = s.date
WHERE 
    c.coin_id = 'bitcoin'
    AND s.ticker = '^GSPC'
ORDER BY
    c.date DESC;
    """)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Compare Ethereum and NASDAQ(IXIC) daily prices for 2025:
cursor.execute(""" 
SELECT
    c.date,c.price_inr AS ethereum_price,
    s.close AS NASDAQ_price
   FROM  Crypto_prices c
INNER JOIN
    stock_prices s ON c.date = s.date
WHERE 
    c.coin_id = 'ethereum'
    AND s.ticker = '^IXIC'
ORDER BY
    c.date DESC;
    """)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Find days when oil price spiked and compare with Bitcoin price change:

cursor.execute('''
WITH DailyOilChange AS (
    SELECT 
        op.date,
        op.price_inr AS oil_price,
        (op.price_inr - LAG(op.price_inr) OVER (ORDER BY op.date)) / 
        LAG(op.price_inr) OVER (ORDER BY op.date) * 100 AS oil_pct_change
    FROM oil_prices op
),
SpikeDays AS (
    SELECT 
        date,
        oil_price,
        oil_pct_change
    FROM DailyOilChange
    WHERE oil_pct_change > 5 
)
SELECT 
    s.date AS spike_date,
    s.oil_price AS oil_spike_price,
    ROUND(s.oil_pct_change, 2) AS oil_pct_change,
    b.price_inr AS btc_price,
    ROUND((b.price_inr - LAG(b.price_inr) OVER (ORDER BY b.date)) / 
          LAG(b.price_inr) OVER (ORDER BY b.date) * 100, 2) AS btc_pct_change
FROM SpikeDays s
INNER JOIN Crypto_prices b ON s.date = b.date
WHERE b.coin_id = 'bitcoin'
ORDER BY s.date DESC;
''')
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Compare top 3 coins daily price trend vs Nifty (^NSEI):

cursor.execute('''
WITH TopCoins AS (
    SELECT coin_id FROM Crypto_prices
    GROUP BY coin_id
    ORDER BY AVG(price_inr) DESC
    LIMIT 3
)
SELECT
    c.coin_id, c.date, c.price_inr AS coin_price,
    s.close AS Nifty_price
FROM Crypto_prices c
INNER JOIN
    stock_prices s ON c.date = s.date
INNER JOIN
    TopCoins tc ON c.coin_id = tc.coin_id
WHERE s.ticker = '^NSEI'
ORDER BY  c.coin_id, c.date DESC
''')
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Compare stock prices (^GSPC) with crude oil prices on the same dates:
cursor.execute('''
    select s.date, s.close as GSPC_price, op.price_inr as crude_oil_price
    from stock_prices s
    inner join
      oil_prices op on op.date=s.date
    where ticker='^GSpc'
    order by s.date

''')
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Correlate Bitcoin closing price with crude oil closing price (same date):

cursor.execute(""" 
SELECT
    c.date,c.price_inr AS bitcoin_price,
    o.price_inr AS crude_oil_price,
    CASE 
        WHEN (c.price_inr > LAG(c.price_inr) OVER (ORDER BY c.date) AND 
              o.price_inr > LAG(o.price_inr) OVER (ORDER BY o.date)) 
        THEN 'Both Up '
        WHEN (c.price_inr < LAG(c.price_inr) OVER (ORDER BY c.date) AND 
              o.price_inr < LAG(o.price_inr) OVER (ORDER BY o.date)) 
        THEN 'Both Down '
        ELSE 'Not Correlated '
    END AS correlation
FROM
    Crypto_prices c
INNER JOIN
    oil_prices o ON c.date = o.date
WHERE 
    c.coin_id = 'bitcoin'
ORDER BY
    c.date DESC;
    """)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)


#Compare NASDAQ (^IXIC) with Ethereum price trends:

cursor.execute(""" 
SELECT
     s.date, s.close AS Nasdaq_price, c.price_inr AS ethereum_price,
    CASE 
        WHEN (c.price_inr > LAG(c.price_inr) OVER (ORDER BY c.date) AND 
              s.close > LAG(s.close) OVER (ORDER BY s.date)) 
        THEN 'Both Up '
        WHEN (c.price_inr < LAG(c.price_inr) OVER (ORDER BY c.date) AND 
              s.close < LAG(s.close) OVER (ORDER BY s.date)) 
        THEN 'Both Down '
        ELSE 'Not Correlated '
    END AS correlation
FROM stock_prices s
INNER JOIN
    crypto_prices c ON c.date = s.date
WHERE 
    c.coin_id = 'ethereum'
    AND s.ticker = '^IXIC'
ORDER BY
    c.date DESC;
    """)
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Join top 3 crypto coins with stock indices for 2025:

cursor.execute('''
WITH TopCoins AS (
    SELECT coin_id FROM Crypto_prices
    GROUP BY coin_id
    ORDER BY AVG(price_inr) DESC
    LIMIT 3
)
SELECT
s.ticker,c.coin_id,s.date,s.open,s.high,s.low,s.close,s.volume,    
 c.date, c.price_inr AS coin_price
FROM stock_prices s
INNER JOIN
    crypto_prices c ON c.date = s.date
INNER JOIN
    TopCoins tc ON c.coin_id = tc.coin_id
WHERE year(c.date) = 2025
ORDER BY  c.coin_id, c.date DESC
''')
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)

#Multi-join: stock prices, oil prices, and Bitcoin prices for daily comparison:
cursor.execute('''
select s.date,s.ticker,s.close as stock_price,
        o.price_inr as Oil_price,
        c.coin_id,c.price_inr as btc_price 
from stock_prices s
inner join 
        oil_prices o ON o.date = s.date
inner join 
        crypto_prices c ON c.date = o.date
where c.coin_id="bitcoin"
ORDER BY  c.coin_id, c.date 
''')
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]
df = pd.DataFrame(rows, columns=columns)
print(df)
#------------------------------The End--------------------------------------------------
      