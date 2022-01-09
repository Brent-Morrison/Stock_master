"""Get Alpha vantage eps data and write to database

Parameters:
apikey (string)                                 an Alpha Vantage API key
wait_seconds (integer)                          wait time before pinging AV server
update_to_date (string - YYYY-MM-DD)            if the last date in the database is this date, do nothing
batch_size (integer)                            number of tickers to process per batch

"""

# Libraries
from functions import *
import json

# Load config file
with open('config.json', 'r') as f:
    config = json.load(f)

# Connect to db
conn = pg_connect(config['pg_password'])

apikey=config['av_apikey']
update_to_date=sys.argv[2]
batch_size=sys.argv[3]
wait_seconds=15


# Grab tickers from database, this is the population
# for which data will be pulled
tickers = pd.read_sql(
sql=text("""
    select * from alpha_vantage.tickers_to_update
    where symbol not in (select ticker from alpha_vantage.ticker_excl) and symbol in ('AAPL', 'AAN')
""")
,con=conn
)

# Convert date parameter from string to date
# To be used as data frame filter
update_to_date=dt.datetime.strptime(update_to_date, '%Y-%m-%d').date()

# Re-format tickers array, NaN replacement
default_date = dt.datetime(1980,12,31).date()
tickers['last_date_in_db'] = tickers['last_date_in_db'].fillna(default_date)
tickers['last_adj_close'] = tickers['last_adj_close'].fillna(0)
tickers['last_eps_date'] = tickers['last_eps_date'].fillna(default_date)


# Filter data frame for those tickers not yet updated
ticker_list = tickers[tickers['last_eps_date'] < (update_to_date - dt.timedelta(days=80))]  # ROW 127 CONDITION HERE

ticker_list = ticker_list.values


# Update loop 
iter_count = 0
push_count = 0
last_av_dates = []
for ticker in ticker_list:
    tic = time.perf_counter() 
    symbol=ticker[0]
    last_date_in_db=ticker[3] # last eps date
    last_adj_close=ticker[2]


    # Stop if the batch size is met
    if iter_count == batch_size:
        break

    # If data is up to date exit loop.  In the context of quarterly reported eps. If there
    # are less than 70 days since the last eps reporting date do not get data.
    if (update_to_date - last_date_in_db).days < 70:
        iter_count += 1
        inner = [last_date_in_db,'data_up_to_date']
        last_av_dates.append(inner)
        toc = time.perf_counter()
        print('loop no.', iter_count,':', symbol, 'data up to date, ', round(toc - tic, 2), ' seconds')
        continue

    # Get eps data from Alphavantage
    try:
        df_raw = get_av_eps(
            symbol=symbol, 
            apikey=apikey
            )
        time.sleep(wait_seconds)

        # Capture last date of data gathered
        df_raw_last_date = pd.to_datetime(df_raw.iloc[0,1]).date()
        
        # Filter data for that greater than last date in database
        df_raw = df_raw[df_raw['date_stamp'] > str(last_date_in_db)]

    except:
        iter_count += 1
        inner = [default_date,'failed_no_data']
        last_av_dates.append(inner)
        toc = time.perf_counter()
        print('loop no.', iter_count,':', symbol, 'failed - no data, ', round(toc - tic, 2), ' seconds')
        continue

    # Push to database
    try:
        if len(df_raw) > 0:
            df_raw.to_sql(name='earnings', con=conn, schema='alpha_vantage', index=False, if_exists='append', method='multi', chunksize=10000)
        
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