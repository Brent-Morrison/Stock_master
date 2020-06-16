##############################################################################
#
# Grab daily price data from AlphaVantage
#
##############################################################################

password = ''

from sqlalchemy import create_engine, MetaData, Table, text
import psycopg2
import pandas as pd
import numpy as np
import time
import requests


# Symbols need to be batched into groups of 500 in order to align with API call limit
# (5 API requests per minute; 500 API requests per day)


base_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol='
symbols = ['A','AA','AAC','AAL']
#outputsize = 'compact'
outputsize = 'full'
apikey = ''
datatype = 'csv'
wait_seconds = 15

df_prices = None
for symbol in symbols:
  url = base_url+symbol+'&outputsize='+outputsize+'&apikey='+apikey+'&datatype='+datatype
  time.sleep(wait_seconds)
  df = pd.read_csv(url)
  df['symbol']=symbol
  if df_prices is None:
    df_prices = df
  else:
    df_prices = pd.concat([df_prices, df])

# Format for database
df_prices = df_prices[['timestamp','open','high','low','close','adjusted_close',
                'volume','dividend_amount','split_coefficient','symbol']]



##############################################################################
#
# Push to database
#
##############################################################################

# Connect to postgres database
engine = create_engine('postgresql://postgres:'+password+
                        '@localhost:5432/stock_master')
conn = engine.connect()
meta = MetaData(engine)
meta.reflect(schema='alpha_vantage')
company_tickers = meta.tables['alpha_vantage.shareprices_daily']

# Insert to postgres database
df_prices.to_sql(name='shareprices_daily', con=engine, schema='alpha_vantage', 
                        index=False, if_exists='append', method='multi', chunksize=50000)

# Close connection
conn.close()