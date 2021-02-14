##############################################################################
#
# Grab daily price data from AlphaVantage
# - Add logic to do nothing when there is no new data, eg ZAYO 
#
# SQL write performance enhancements
# - https://naysan.ca/2020/06/21/pandas-to-postgresql-using-psycopg2-copy_from/
# - https://hakibenita.com/fast-load-data-python-postgresql
# - https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table/47984180#47984180
#
##############################################################################

# Libraries
from sqlalchemy import create_engine, MetaData, Table, text
import psycopg2
import pandas as pd
import numpy as np
import datetime as dt
import time
import requests
import yfinance as yf

# Parameters
password = ''
apikey = ''
data = 'prices'                                    # Data to grab 'prices' or 'eps'
wait_seconds = 20                               # Wait time before pinging AV server
update_to_date = dt.datetime(2021,1,31).date()  # If the last date in the database is this date, do nothing
batch_size = 3                                # Number of tickers to process per batch

# Connect to postgres database
# ?gssencmode=disable' per https://stackoverflow.com/questions/59190010/psycopg2-operationalerror-fatal-unsupported-frontend-protocol-1234-5679-serve
engine = create_engine('postgresql://postgres:'+password+
                        '@localhost:5432/stock_master?gssencmode=disable')
conn = engine.connect()
meta = MetaData(engine)
meta.reflect(schema='alpha_vantage')
company_tickers = meta.tables['alpha_vantage.shareprices_daily']


# Define get_alphavantage function
def get_alphavantage(symbol, data, outputsize, apikey):
  
  base_url = 'https://www.alphavantage.co/query?'
  
  if data == 'prices':
    function='TIME_SERIES_DAILY_ADJUSTED'
    datatype='csv'
    url = base_url+'function='+function+'&symbol='+symbol+'&outputsize='+outputsize+'&apikey='+apikey+'&datatype='+datatype
    df = pd.read_csv(url)
    df['symbol']=symbol
    df = df[[
      'timestamp','open','high','low','close','adjusted_close',
      'volume','dividend_amount','split_coefficient','symbol'
      ]]
    df['capture_date'] = dt.datetime.today().date()
  elif data == 'eps':
    function='EARNINGS'
    url = base_url+'function='+function+'&symbol='+symbol+'&apikey='+apikey
    resp = requests.get(url)
    txt = resp.json()['quarterlyEarnings']
    df = pd.DataFrame(txt)
    df = df.rename(columns={
      'fiscalDateEnding': 'report_date'
      ,'reportedDate': 'date_stamp'
      ,'reportedEPS': 'reported_eps'
      ,'estimatedEPS': 'estimated_eps'
      ,'surprise': 'eps_surprise'
      ,'surprisePercentage': 'eps_surprise_perc'
      })
    df['capture_date'] = dt.datetime.today().date()
    df['symbol'] = symbol
    df = df[[
      'symbol','date_stamp','report_date','reported_eps','estimated_eps',
      'eps_surprise','eps_surprise_perc','capture_date'
      ]]
    cols = ['reported_eps','estimated_eps','eps_surprise','eps_surprise_perc']
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce', axis=1)
  return df



# Grab tickers from database
tickers = pd.read_sql(
  sql=text("""
    select * from alpha_vantage.tickers_to_update
    where symbol not in (select ticker from alpha_vantage.ticker_excl)
  """)
  ,con=conn
  )


# Re-format tickers array
default_date = dt.datetime(1980,12,31).date()
tickers['last_date_in_db'] = tickers['last_date_in_db'].fillna(default_date)
tickers['last_adj_close'] = tickers['last_adj_close'].fillna(0)
tickers['last_eps_date'] = tickers['last_eps_date'].fillna(default_date)


# Filter data frame for those not yet updated
if data == 'prices':
  ticker_list = tickers[tickers['last_date_in_db'] < update_to_date]
elif data == 'eps':
  ticker_list = tickers[tickers['last_eps_date'] < update_to_date]  # ROW 127 CONDITION HERE

ticker_list = ticker_list.values


# Update function 
iter_count = 0
push_count = 0
last_av_dates = []
for ticker in ticker_list:
  symbol=ticker[0]
  if data == 'prices':
    last_date_in_db=ticker[1] # last price date
  elif data == 'eps':
    last_date_in_db=ticker[3] # last eps date
  last_adj_close=ticker[2]


  # Stop if the batch size is met
  if iter_count == batch_size:
    break

  # If data is up to date exit loop 
  if (data == 'prices' and last_date_in_db >= update_to_date) or (data == 'eps' and (update_to_date - last_date_in_db).days < 40):
    iter_count += 1
    inner = [last_date_in_db,'data_up_to_date']
    last_av_dates.append(inner)
    print('loop no.', iter_count,':', symbol, 'data up to date')
    continue
  
  # If the default date has been returned (via the replacement of NaN's), there is no data, 
  # therefore run full update
  elif last_date_in_db == default_date:
    update_mode='full'
  
  # Else compact (100 days) update
  else:
    update_mode='compact'
  
  # Get price data from Alphavantage
  try:
    df_raw = get_alphavantage(
        symbol=symbol, 
        data=data,
        apikey=apikey, 
        outputsize=update_mode
        )
    time.sleep(wait_seconds)
    if data == 'prices':
      df_raw_last_date = pd.to_datetime(df_raw.iloc[0,0]).date()
    elif data == 'eps':
      df_raw_last_date = pd.to_datetime(df_raw.iloc[0,1]).date()
      df_raw = df_raw[df_raw['date_stamp'] > str(last_date_in_db)]
  except:
    iter_count += 1
    inner = [default_date,'failed_no_data']
    last_av_dates.append(inner)
    print('loop no.', iter_count,':', symbol, 'failed - no data')
    continue
  
  ##### Start block applying only to price data #####
  
  if data == 'prices':
  
    # Get the last adjusted close from Alphavantage for the date of the last price in the database
    df_prices_last_adj_close = df_raw[df_raw['timestamp'] == str(last_date_in_db)]['adjusted_close']

    # If the new adjusted close is not different to the existing adjusted close, filter for new dates only
    if (update_mode == 'compact') and (abs(np.round(df_prices_last_adj_close.values,2) - np.round(last_adj_close,2)) < 0.03):
      df_raw = df_raw[df_raw['timestamp'] > str(last_date_in_db)]

    # Else if the adjusted close is different, gather the full extract
    elif (update_mode == 'compact') and (abs(np.round(df_prices_last_adj_close.values,2) - np.round(last_adj_close,2)) >= 0.03):
      df_raw = None
      try:
        df_raw = get_alphavantage(
            symbol=symbol, 
            apikey=apikey, 
            outputsize = 'full'
            )
        time.sleep(wait_seconds)
        df_raw_last_date = pd.to_datetime(df_raw.iloc[0,0]).date()
      except:
        iter_count += 1
        inner = [default_date,'failed_no_data']
        last_av_dates.append(inner)
        print('loop no.', iter_count,':', symbol, 'failed - no data')
        continue
    
    # Exit loop if there are no records to update
    if len(df_raw) == 0:
      iter_count += 1
      inner = [pd.to_datetime(df_raw_last_date),'nil_records_no_update']
      last_av_dates.append(inner)
      print('loop no.', iter_count,':', symbol, len(df_raw), 'records - no update')
      continue
  
  ##### End block applying only to price data #####

  # Push to database
  try:
    if data == 'prices' and len(df_raw) > 0:
      df_raw.to_sql(name='shareprices_daily', con=engine, schema='alpha_vantage', 
                    index=False, if_exists='append', method='multi', chunksize=50000)
    elif data == 'eps' and len(df_raw) > 0:
      df_raw.to_sql(name='earnings', con=engine, schema='alpha_vantage', 
                    index=False, if_exists='append', method='multi', chunksize=50000)
    
    iter_count += 1
    push_count += 1
    inner = [pd.to_datetime(df_raw_last_date),'succesful_update']
    last_av_dates.append(inner)
    print('loop no.', iter_count,':', symbol, len(df_raw), 'records updated')
    print('push no.', push_count)
  except:
    iter_count += 1
    inner = [pd.to_datetime(df_raw_last_date),'failed_push_to_db']
    last_av_dates.append(inner)
    print('loop no.', iter_count,':', symbol, 'failed - unable to push to db')
    continue


# Send list of stale stocks to db
if data == 'prices':
  ticker_excl = pd.DataFrame(data=ticker_list[:len(last_av_dates)], 
    columns=['ticker','last_date_in_db','price','last_eps_date']) # Error - column "last_eps_date" of relation "ticker_excl" does not exist
  last_av_dates = np.array(last_av_dates)
  ticker_excl['last_av_date'] = last_av_dates[:,0]
  ticker_excl['status'] = last_av_dates[:,1]
  ticker_excl['last_av_date'] = pd.to_datetime(ticker_excl['last_av_date']).dt.date
  ticker_excl = ticker_excl.loc[(ticker_excl['last_date_in_db'] == ticker_excl['last_av_date']) | (ticker_excl['status'] == 'failed_no_data')]

  # Push to database
  ticker_excl.to_sql(name='ticker_excl', con=engine, schema='alpha_vantage', 
                    index=False, if_exists='append', method='multi', chunksize=50000)


# Close connection
conn.close()








##############################################################################
#
# Grab AlphaVantage active and delisted stock data
#
##############################################################################

# Parameters
apikey = ''
update_date = str(dt.datetime.today().date())

# urls
act_url = 'https://www.alphavantage.co/query?function=LISTING_STATUS&date='+update_date+'&state=active&apikey='+apikey
del_url = 'https://www.alphavantage.co/query?function=LISTING_STATUS&date='+update_date+'&state=delisted&apikey='+apikey

# Get data
act_df = pd.read_csv(act_url)
del_df = pd.read_csv(del_url)

# Concatenate, rename and append
df = pd.concat([act_df, del_df])
df = df.rename(columns={'ipoDate': 'ipo_date', 'delistingDate': 'delist_date'})
df['capture_date'] = dt.datetime.today().date()
df.loc[df['status'] == 'Active', 'delist_date'] = dt.datetime(9998,12,31).date()

# Push to database
df.to_sql(name='active_delisted', con=engine, schema='alpha_vantage', 
            index=False, if_exists='append', method='multi', chunksize=50000)






##############################################################################
#
# Grab S&P500 index data
#
##############################################################################

df_sp500 = yf.download('^GSPC')
df_sp500.reset_index(inplace=True)
df_sp500['dividend_amount'] = 0
df_sp500['split_coefficient'] = 0
df_sp500['symbol'] = 'GSPC'
df_sp500['capture_date'] = dt.datetime.today().date()
df_sp500.columns = ['timestamp','open','high','low','close','adjusted_close',
  'volume','dividend_amount','split_coefficient','symbol','capture_date']

# Max date of existing data
max_date = pd.read_sql(sql=text(
  """select max(timestamp) 
  from alpha_vantage.shareprices_daily 
  where symbol = 'GSPC'"""),
  con=conn)

# Convert max_date to list
#max_date = max_date['max'].tolist()

# Convert datetime to date
df_sp500['timestamp'] = pd.to_datetime(df_sp500['timestamp']).dt.date

# Return only data post existing date
df_sp500 = df_sp500[df_sp500['timestamp'] > max_date['max'][0]].copy()

# Insert to postgres database
df_sp500.to_sql(name='shareprices_daily', con=engine, schema='alpha_vantage', 
                index=False, if_exists='append', method='multi', chunksize=50000)

# Close connection
conn.close()



# SCRATCH
last_date_in_db=dt.datetime(2021,1,1).date()
update_to_date=dt.datetime(2021,1,31).date()
if (update_to_date - last_date_in_db).days < 40:
  print('continue')
else:
  print('grab data')