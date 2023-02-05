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
# In order to enable loading of a module, add location to $PYTHONPATH.  To be replaced once package set up
# https://askubuntu.com/questions/470982/how-to-add-a-python-module-to-syspath
sys.path.insert(1, 'C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres') 
from functions import *
import json

# Script parameters passed from Airflow
database = sys.argv[1]              # 'stock_master_test'
batch_size = sys.argv[2]            # 6 #
update_to_date = sys.argv[3]        # '2022-02-04' #
test_data_bool_raw = sys.argv[4]    # 'T' #
wait_seconds = 0.1


# Load config file
with open('C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\config.json', 'r') as f:
    config = json.load(f)


# Connect to db
conn = pg_connect(pg_password=config['pg_password'], database=database)


# Convert to integer
batch_size = int(batch_size)

# Convert 'test_data_bool_raw' to boolean
test_data_bool = True
if test_data_bool_raw.lower() == 't':
    test_data_bool = True
elif test_data_bool_raw.lower() == 'f':
    test_data_bool = False

# Convert date parameter from string to date, for use as data frame filter
update_to_date = dt.datetime.strptime(update_to_date, '%Y-%m-%d').date()


# IEX API token
if test_data_bool:
    api_token = config['iex_test_token']
    schema_name = 'test'
    tbl_name = 'shareprices_daily_iex'
else:
    api_token = config['iex_api_token']
    schema_name = 'iex'
    tbl_name = 'shareprices_daily'


# Grab tickers from database, this is the population
# for which data will be updated
tickers = pd.read_sql(
sql=text("""select * from access_layer.tickers_to_update_fn(valid_year_param => 2022, nonfin_cutoff => 950, fin_cutoff => 150)""")
#sql=text("""select * from test.tickers_to_update where symbol in ('B','BA','BAX','BBBY','HWM','INT','INTC') order by 1""")  ## TOGGLE FOR TEST ##
,con=conn)


# Remove trailing underscores from column names
tickers.columns = tickers.columns.str.rstrip('_')      ## TOGGLE FOR TEST ##


# Re-format tickers array, NaN replacement
default_date = dt.datetime(1980,12,31).date()
tickers['last_date_in_db'] = tickers['last_date_in_db'].fillna(default_date)
tickers['last_adj_close'] = tickers['last_adj_close'].fillna(0)
tickers['last_eps_date'] = tickers['last_eps_date'].fillna(default_date)


# Filter data frame for those tickers not yet updated
ticker_list = tickers[tickers['last_date_in_db'] <= update_to_date]


# To numpy array
ticker_list = ticker_list.values


# Initialise loop variables 
iter_count = 0
push_count = 0
logging_list = []

# Loop
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
        toc = time.perf_counter()
        log_item = [ticker[0], last_date_in_db, default_date, 0, 0, 'data_up_to_date', round(toc - tic, 2)]
        logging_list.append(log_item)
        print('loop no.', iter_count,':', symbol, 'data up to date, ', round(toc - tic, 2), 'seconds')
        continue

    if last_date_in_db == default_date:
        # Get price data from IEX (full update)
        try:
            df_raw = get_iex_price(symbol=symbol, outputsize='max', api_token=api_token, sandbox=test_data_bool)
            time.sleep(wait_seconds)
            
            # Get last date of IEX data
            df_raw_last_date = pd.to_datetime(df_raw.iloc[0,1]).date()

        except:
            iter_count += 1
            toc = time.perf_counter()
            log_item = [ticker[0], last_date_in_db, default_date, 0, 0, 'failed_no_data', round(toc - tic, 2)]
            logging_list.append(log_item)
            print('loop no.', iter_count,':', symbol, 'failed - no data, ', round(toc - tic, 2), 'seconds')
            continue


        # Filter data frame for new dates only
        df = df_raw[(df_raw['date_stamp'] <= str(update_to_date)) & (df_raw['date_stamp'] > str(last_date_in_db))].copy()

    else:
        # Get price data from IEX (3 months)
        try:
            df_raw = get_iex_price(symbol=symbol, outputsize='3m', api_token=api_token, sandbox=test_data_bool)
            time.sleep(wait_seconds)
            
            # Get last date of IEX data
            df_raw_last_date = pd.to_datetime(df_raw.iloc[0,1]).date()

        except:
            iter_count += 1
            toc = time.perf_counter()
            log_item = [ticker[0], last_date_in_db, default_date, 0, 0, 'failed_no_data', round(toc - tic, 2)]
            logging_list.append(log_item)
            print('loop no.', iter_count,':', symbol, 'failed - no data, ', round(toc - tic, 2), 'seconds')
            continue


        # Filter data frame for new dates only
        df = df_raw[(df_raw['date_stamp'] <= str(update_to_date)) & (df_raw['date_stamp'] > str(last_date_in_db))].copy()
     

    # Push to database
    try:
        df.to_sql( con=conn, schema=schema_name, name=tbl_name,  
                  index=False, if_exists='append', method='multi', chunksize=10000)        
        iter_count += 1
        push_count += 1
        toc = time.perf_counter()
        log_item = [ticker[0], last_date_in_db, df_raw_last_date, len(df_raw), len(df), 'succesful_update', round(toc - tic, 2)]
        logging_list.append(log_item)
        print('loop no.', iter_count,':', symbol, len(df), 'records updated, ', round(toc - tic, 2), 'seconds')
        print('push no.', push_count)
    except:
        iter_count += 1
        toc = time.perf_counter()
        log_item = [ticker[0], last_date_in_db, default_date, len(df_raw), len(df), 'failed_push_to_db', round(toc - tic, 2)]
        logging_list.append(log_item)
        print('loop no.', iter_count,':', symbol, len(df_raw), 'records retrieved, push to db failed, ', round(toc - tic, 2), 'seconds')
        continue


# Create data frame containing update status
logging_df = pd.DataFrame(data=logging_list, 
    columns=['ticker','last_date_in_db','last_date_retrieved'
        ,'records_retrieved','records_updated','status','loop_time']) 
logging_df['capture_date'] = dt.datetime.today().date()

# Write to csv
logging_df.to_csv('C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\airflow\\update_iex_price_log.csv')