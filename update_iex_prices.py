"""Grab Alpha vantage or IEX data and write to database

Parameters:
apikey (string)                                 an Alpha vantage API key (no default)
conn (connection object)                        database connection (no default)
wait_seconds (integer)                          wait time before pinging server (default 15)
update_to_date (string - YYYY-MM-DD)            if the last date in the database is this date, do nothing
batch_size (integer)                            number of tickers to process per batch

Returns:
Data frame containing write status of tickers selected

"""

# Libraries
import sys
from functions import *
import json

# Load config file
with open('config.json', 'r') as f:
    config = json.load(f)


# TEST
#conn = pg_connect('')
#dummy_date0 = pd.read_sql(sql=text("""select * from test.shareprices_daily_test where symbol = 'YUM' and adjusted_close = 137.98"""),con=conn)
#dummy_date1 = dummy_date0.values
#db_date = dummy_date1[0][0]
#df1 = get_iex('YUM', '1m', 'pk_86b6d51533d847568f83db64c03a5d95')
#df2 = df1.copy()
#df2['timestamp'] = pd.to_datetime(df2['timestamp'])
#df2[df2['timestamp'].dt.month == 12]['dividend_amount'].mean()
#df2[df2['timestamp'].dt.month == 12]['split_coefficient'].mean()
#df3 = df1[(df1['timestamp'] > str('2021-12-28')) & (df1['timestamp'] <= str(update_to_date))]
#df2.to_sql(name='shareprices_daily_test', con=conn, schema='test', index=False, if_exists='append', method='multi', chunksize=10000)


# Connect to db
conn = pg_connect(config['pg_password'])

# TEST
#conn.execute("""truncate test.shareprices_daily_test""")


# IEX API token
api_token = config['iex_api_token']

update_to_date = '2021-12-31'
wait_seconds = 0.1
batch_size = sys.argv[1] #2


# Grab tickers from database, this is the population
# for which data will be updated
#tickers = pd.read_csv('C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\tickers_test.csv')
tickers = pd.read_sql(
#sql=text("""
#    select * from alpha_vantage.tickers_to_update
#    where symbol not in (select ticker from alpha_vantage.ticker_excl)
#""")
sql=text("""select * from test.tickers_to_update""")
,con=conn)


# Convert date parameter from string to date, for use as data frame filter
update_to_date=dt.datetime.strptime(update_to_date, '%Y-%m-%d').date()

# Re-format tickers array, NaN replacement
default_date = dt.datetime(1980,12,31).date()
tickers['last_date_in_db'] = tickers['last_date_in_db'].fillna(default_date)
tickers['last_adj_close'] = tickers['last_adj_close'].fillna(0)
tickers['last_eps_date'] = tickers['last_eps_date'].fillna(default_date)


# Filter data frame for those tickers not yet updated
ticker_list = tickers[tickers['last_date_in_db'] < update_to_date]

# To numpy array
ticker_list = ticker_list.values


# Update loop 
iter_count = 0
push_count = 0
last_av_dates = []
for ticker in ticker_list:
    tic = time.perf_counter() 
    symbol = ticker[0]
    last_date_in_db = ticker[1] # last price date
    last_adj_close = ticker[2]  # last close


    # Stop if the batch size is met
    if iter_count == batch_size:
        break

    # If data is up to date exit current loop 
    if last_date_in_db >= update_to_date:
        iter_count += 1
        inner = [last_date_in_db,'data_up_to_date']
        last_av_dates.append(inner)
        toc = time.perf_counter()
        print('loop no.', iter_count,':', symbol, 'data up to date, ', round(toc - tic, 2), ' seconds')
        continue

    # If the default date has been returned (via the replacement of NaN's), 
    # there is no data, therefore run full update
    #elif last_date_in_db == default_date:
    #    outputsize = 'max'

    # Else compact (3 months) update
    #else:
    #    outputsize= '3m'

    # Get price data from IEX
    try:
        df_raw = get_iex_price(symbol=symbol, outputsize='3m', api_token=api_token)
        time.sleep(wait_seconds)
        
        # Get last date of IEX data 
        df_raw_last_date = pd.to_datetime(df_raw.iloc[0,0]).date()

    except:
        iter_count += 1
        inner = [default_date,'failed_no_data']
        last_av_dates.append(inner)
        toc = time.perf_counter()
        print('loop no.', iter_count,':', symbol, 'failed - no data, ', round(toc - tic, 2), ' seconds')
        continue

    # Get the adjusted close downloaded from IEX as at the date of the last price in the database
    df_prices_last_adj_close = df_raw[df_raw['timestamp'] == str(last_date_in_db)]['adjusted_close']

    # This can return NONE if there is a gap in trading (see GPOR April to May 2021),
    # check if empty and assign 0 if so
    df_prices_last_adj_close_values = df_prices_last_adj_close.values
    if df_prices_last_adj_close_values.size == 0:
        df_prices_last_adj_close_values = 0

    # If the new adjusted close is not different to the existing adjusted close, and 
    # there has not been a dividend, and there has not been a split, filter for new dates only

    # Filter df for update records only
    df = df_raw[(df_raw['timestamp'] > str(last_date_in_db)) & (df_raw['timestamp'] <= str(update_to_date))].copy()

    # Boolean for equality of close, existence of dividend and split
    # If any of these are true, a full data refresh is required
    close_equal_ind = abs(np.round(df_prices_last_adj_close_values,2) - np.round(last_adj_close,2)) >= 0.02
    split_ind = df['split_coefficient'].mean() != 1
    dividend_ind = df['dividend_amount'].mean() != 1

    # Gather the full extract if any of the above conditions are true 
    if close_equal_ind or split_ind or dividend_ind:
        df_raw = None
        df = None
        try:
            df_raw = get_iex_price(symbol=symbol, outputsize='max', api_token=api_token)
            time.sleep(wait_seconds)
            # Get last date of IEX data 
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

        df = df_raw[df_raw['timestamp'] <= str(update_to_date)].copy()


    # Push to database
    try:
        df.to_sql(name='shareprices_daily_test', con=conn, schema='test', 
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
#update_df = pd.DataFrame(data=ticker_list[:len(last_av_dates),:3], 
#    columns=['ticker','last_date_in_db','price']) # Error - column "last_eps_date" of relation "update_df" does not exist
#last_av_dates = np.array(last_av_dates)
#update_df['last_av_date'] = last_av_dates[:,0]
#update_df['status'] = last_av_dates[:,1]
#update_df['last_av_date'] = pd.to_datetime(update_df['last_av_date']).dt.date