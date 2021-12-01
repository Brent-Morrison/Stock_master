from sqlalchemy import create_engine, text, types
import psycopg2
import pandas as pd
import numpy as np
import datetime as dt
import requests
import time
import os
import io as io
import sys
from zipfile import ZipFile
import yfinance as yf




# Connect to database ------------------------------------------------------------------------------------------------------


def pg_connect(password):
    conn = None
    try:
        engine = create_engine('postgresql://postgres:'+password+
                                '@localhost:5432/stock_master?gssencmode=disable')
        
        # https://docs.sqlalchemy.org/en/13/core/connections.html#working-with-raw-dbapi-connections
        conn = engine.connect()
        #conn = connection.connection
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        # https://docs.python.org/3/library/sys.html#sys.exit
        #sys.exit(1) 
    print("Connection successful")
    return conn




# get_alphavantage function ------------------------------------------------------------------------------------------------


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




# Copy to database ---------------------------------------------------------------------------------------------------------


def copy_from_stringio(conn, df, table):
    
    """Save dataframe in memory and use copy_from() to copy it to database table"""
    
    # Save dataframe to an in memory buffer
    buffer = io.StringIO()
    df.to_csv(buffer, index_label='id', header=False, index=False, na_rep='')
    # Reset the position to the start of the stream
    buffer.seek(0)
    db_connection = conn.connection
    cursor = db_connection.cursor()
    try:
        cursor.copy_from(buffer, table, sep=',', null='')
        db_connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        
        print("Error: %s" % error)
        db_connection.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()




# Grab S&P 500 data --------------------------------------------------------------------------------------------------------

def update_sp500_yf(conn):
    
    """Grab S&P 500 series using yfinance and write to database"""
    
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
    df_sp500.to_sql(name='shareprices_daily', con=conn, schema='alpha_vantage', 
        index=False, if_exists='append', method='multi', chunksize=10000)
    
    print(df_sp500.shape[0]," records inserted into alpha_vantage.shareprices_daily")





# --------------------------------------------------------------------------------------------------------------------------

def update_av_data(apikey, conn, update_to_date, data='prices', wait_seconds=15, batch_size= 350):
  
  """Grab Alpha vantage data and write to database

  Keyword arguments:
    apikey (string)                                 an Alpha vantage API key (no default)
    conn (connection object)                        database connection (no default)
    data (string)                                   the Data to grab 'prices' or 'eps' (default 'prices')
    wait_seconds (integer)                          wait time before pinging AV server (default 15)
    update_to_date (string - YYYY-MM-DD)            if the last date in the database is this date, do nothing
    batch_size (integer)                            number of tickers to process per batch

  Returns:
    Data frame containing write status of tickers selected

  """
  
  # Convert date string to date
  update_to_date=dt.datetime.strptime(update_to_date, '%Y-%m-%d').date()

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


  # Filter data frame for those tickers not yet updated
  if data == 'prices':
    ticker_list = tickers[tickers['last_date_in_db'] < update_to_date]
  elif data == 'eps':
    ticker_list = tickers[tickers['last_eps_date'] < (update_to_date - dt.timedelta(days=80))]  # ROW 127 CONDITION HERE

  ticker_list = ticker_list.values


  # Update loop 
  iter_count = 0
  push_count = 0
  last_av_dates = []
  for ticker in ticker_list:
    tic = time.perf_counter() 
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
    if (data == 'prices' and last_date_in_db >= update_to_date) or (data == 'eps' and (update_to_date - last_date_in_db).days < 70):
      iter_count += 1
      inner = [last_date_in_db,'data_up_to_date']
      last_av_dates.append(inner)
      toc = time.perf_counter()
      print('loop no.', iter_count,':', symbol, 'data up to date, ', round(toc - tic, 2), ' seconds')
      continue
    
    # If the default date has been returned (via the replacement of NaN's), there is no data, 
    # therefore run full update
    elif last_date_in_db == default_date:
      update_mode='full'
    
    # Else compact (100 days) update
    else:
      update_mode='compact'
    
    # Get price / eps data from Alphavantage
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
      toc = time.perf_counter()
      print('loop no.', iter_count,':', symbol, 'failed - no data, ', round(toc - tic, 2), ' seconds')
      continue
    
    ##### Start block applying only to price data #####
    
    if data == 'prices':
    
      # Get the adjusted close downloaded from Alphavantage as at the date of the last price in the database
      df_prices_last_adj_close = df_raw[df_raw['timestamp'] == str(last_date_in_db)]['adjusted_close']

      # This can return NONE if there is a gap in trading (see GPOR April to May 2021),
      # check if empty and assign 0 if so
      df_prices_last_adj_close_values = df_prices_last_adj_close.values
      if df_prices_last_adj_close_values.size == 0:
        df_prices_last_adj_close_values = 0

      # If the new adjusted close is not different to the existing adjusted close, filter for new dates only
      if (update_mode == 'compact') and (abs(np.round(df_prices_last_adj_close_values,2) - np.round(last_adj_close,2)) < 0.03):
        df_raw = df_raw[df_raw['timestamp'] > str(last_date_in_db)]

      # Else if the adjusted close is different, gather the full extract
      elif (update_mode == 'compact') and (abs(np.round(df_prices_last_adj_close_values,2) - np.round(last_adj_close,2)) >= 0.03):
        df_raw = None
        try:
          df_raw = get_alphavantage(
              symbol=symbol,
              data=data, 
              apikey=apikey, 
              outputsize = 'full'
              )
          time.sleep(wait_seconds)
          df_raw_last_date = pd.to_datetime(df_raw.iloc[0,0]).date()
        except:
          iter_count += 1
          inner = [default_date,'failed_no_data']
          last_av_dates.append(inner)
          toc = time.perf_counter()
          print('loop no.', iter_count,':', symbol, 'failed - no data, ', round(toc - tic, 2), ' seconds')
          continue
      
      # Exit loop if there are no records to update
      if len(df_raw) == 0:
        iter_count += 1
        inner = [pd.to_datetime(df_raw_last_date),'nil_records_no_update']
        last_av_dates.append(inner)
        toc = time.perf_counter()
        print('loop no.', iter_count,':', symbol, len(df_raw), 'records - no update, ', round(toc - tic, 2), ' seconds')
        continue
    
    ##### End block applying only to price data #####

    # Push to database
    try:
      if data == 'prices' and len(df_raw) > 0:
        df_raw.to_sql(name='shareprices_daily', con=conn, schema='alpha_vantage', 
                      index=False, if_exists='append', method='multi', chunksize=10000)
      elif data == 'eps' and len(df_raw) > 0:
        df_raw.to_sql(name='earnings', con=conn, schema='alpha_vantage', 
                      index=False, if_exists='append', method='multi', chunksize=10000)
      
      iter_count += 1
      push_count += 1
      inner = [pd.to_datetime(df_raw_last_date),'succesful_update']
      last_av_dates.append(inner)
      toc = time.perf_counter()
      print('loop no.', iter_count,':', symbol, len(df_raw), 'records updated, ', round(toc - tic, 2), ' seconds')
      print('push no.', push_count)
    except:
      iter_count += 1
      inner = [pd.to_datetime(df_raw_last_date),'failed_push_to_db']
      last_av_dates.append(inner)
      toc = time.perf_counter()
      print('loop no.', iter_count,':', symbol, 'failed - unable to push to db, ', round(toc - tic, 2), ' seconds')
      continue


  # Create data frame containing update status
  if data == 'prices':
    update_df = pd.DataFrame(data=ticker_list[:len(last_av_dates),:3], 
      columns=['ticker','last_date_in_db','price']) # Error - column "last_eps_date" of relation "update_df" does not exist
    last_av_dates = np.array(last_av_dates)
    update_df['last_av_date'] = last_av_dates[:,0]
    update_df['status'] = last_av_dates[:,1]
    update_df['last_av_date'] = pd.to_datetime(update_df['last_av_date']).dt.date
  
  # Return data frame listing update status
  return update_df






# TEST
# Get data
#csv = 'https://github.com/Brent-Morrison/Misc_scripts/raw/master/daily_price_ts_vw_20201018.csv'
#test_df = pd.read_csv(csv)

# Connect
#conn = pg_connect('')

# Push to db
#copy_from_stringio(conn=conn, df=test_df, table='test.test_table')