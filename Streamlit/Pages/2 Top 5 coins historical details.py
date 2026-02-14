# import streamlit as st
# import pandas as pd


# st.set_page_config(
#     page_title="Project"
# )

# st.title('Top 5 coins  on market')
# st.set_page_config(page_title="Cross-Market Overview", layout="wide", initial_sidebar_state="expanded")

# #sql connection:
# import pymysql

# connection = pymysql.connect(
#     host='localhost',
#     user='root',
#     password='Sathya1234',
#     database='guvi_project'
# )
# cursor = connection.cursor()


# option=st.selectbox[{coin1},{coin2},{coin3},{coin4},{coin5}]
# if option == coin1:
#     cursor.execute('select * from Crypto_prices where market_cap_rank = 1')
# elif option== coin2:
#     cursor.execute('select * from Crypto_prices where market_cap_rank = 2')
# elif option== coin3:
#     cursor.execute('select * from Crypto_prices where market_cap_rank = 3') 
# elif option== coin4:
#     cursor.execute('select * from Crypto_prices where market_cap_rank = 4')
# else:
#     cursor.execute('select * from Crypto_prices where market_cap_rank = 5')
# rows = cursor.fetchall()
# columns = [desc[0] for desc in cursor.description]
# df = pd.DataFrame(rows, columns=columns)

import streamlit as st
import pandas as pd
import pymysql

st.markdown("""
    <style>
    .stApp {
        background-color: #A9A9A9 ;
        color: #FFFAFA;
        html, body, [class*="css"]  {
    font-size: 18px !important;
    }
    </style>
""", unsafe_allow_html=True)


# DB connection
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Sathya1234',
    database='guvi_project'
)
cursor = connection.cursor()

st.title(" Top 5 coins Historical data")

# Get Top 5 Coins
cursor.execute("""
    SELECT coin_id
    FROM Crypto_prices
    GROUP BY coin_id
    ORDER BY MAX(price_inr) DESC
    LIMIT 5

""")

rows = cursor.fetchall()

# Convert to list
top5_coins = [row[0] for row in rows]

# Selectbox
selected_coin = st.selectbox("Select Coin", top5_coins)

#  Show data for selected coin
cursor.execute("""
    SELECT date, price_inr
    FROM Crypto_prices
    WHERE coin_id = %s
    ORDER BY date DESC
    LIMIT 30
""", (selected_coin,))

data = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

df = pd.DataFrame(data, columns=columns)

st.dataframe(df)
st.line_chart(df.set_index("date"))
