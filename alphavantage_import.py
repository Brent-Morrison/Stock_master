###################################################################################################
#
# GRAB DAILY PRICE DATA FROM ALPHA VANTAGE / S&P 500 DATA
# 
# Notes 
# - Check the stocks that are excluded from update via the "ticker_excl" table
#   "select * from alpha_vantage.ticker_excl"
# - Delete from this table if erronous entries have been made.
# - Alpha Vantage download restrictions seem to be stricter.  Limit daily downloads to 250.
# 
# To do
# - Add logic to do nothing when there is no new data, eg ZAYO 
#
# SQL write performance enhancements
# - https://naysan.ca/2020/06/21/pandas-to-postgresql-using-psycopg2-copy_from/
# - https://hakibenita.com/fast-load-data-python-postgresql
# - https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table/47984180#47984180
#
###################################################################################################

# Libraries
from functions import *


# Connect to db
conn = pg_connect('')


# Update function
# - The function will write valid data to the database on each iteration
# - The object assigned to "update_df" below is a data frame containing the 
#   status of the stocks looped over 
update_df = update_av_data(
  apikey='', 
  conn=conn, 
  update_to_date='2021-11-30', 
  data='prices', 
  wait_seconds=12.5, 
  batch_size=325
  )


# Filter resultant data frame for errors
ticker_excl = update_df.loc[
  (update_df['last_date_in_db'] == update_df['last_av_date']) | 
  (update_df['status'] == 'failed_no_data') | 
  (update_df['status'] == 'nil_records_no_update')
  ]

# Push to database
ticker_excl.to_sql(name='ticker_excl', con=conn, schema='alpha_vantage', 
  index=False, if_exists='append', method='multi', chunksize=50000)


# Close connection
conn.close()




# --------------------------------------------------------------------------------------------------

# Libraries
from functions import *


# Connect to db
conn = pg_connect('')


# Grab and insert S&P500 data
update_sp500_yf(conn)


# Close connection
conn.close()








# --------------------------------------------------------------------------------------------------

# Libraries
from functions import *


# Connect to db
conn = pg_connect('')


# Grab and insert S&P500 data
update_active_delisted(conn, '')


# Close connection
conn.close()







###################################################################################################
#
# Grab AlphaVantage active and delisted stock data
#
###################################################################################################


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
df = df.loc[df['assetType'] == 'Stock']
df = df.drop('assetType', axis=1)
existing = pd.read_sql(sql=text("""select * from alpha_vantage.active_delisted"""), con=conn)
update_df = pd.concat([df, existing]).drop_duplicates(['symbol','exchange','status'], keep=False)

# Push to database
df.to_sql(name='active_delisted', con=conn, schema='alpha_vantage', 
  index=False, if_exists='append', method='multi', chunksize=50000)













###################################################################################################
#
# Various tests
#
###################################################################################################



# Insert timing
tic = time.perf_counter()
df_test = get_alphavantage(
    symbol='MAA',
    data='prices', 
    apikey='J2MWHUOABDSEVS6P', 
    outputsize = 'full'
    )
toc = time.perf_counter()
print('Download time was', round(toc - tic, 2), 'seconds')


# Push to db
tic = time.perf_counter()
copy_from_stringio(conn=conn, df=df_test, table='test.shareprices_daily_test')
toc = time.perf_counter()
print('Upload time was', round(toc - tic, 2), 'seconds for stringio')

tic = time.perf_counter()
copy_from_stringio(conn=conn, df=df_test, table='test.shareprices_daily_test_idx')
toc = time.perf_counter()
print('Upload time was', round(toc - tic, 2), 'seconds for stringio with index')

# to_sql dtypes parameters
from sqlalchemy import types
sql_types={
  'timestamp': types.Date, 
  'open': types.Numeric,
  'high': types.Numeric,
  'low': types.Numeric,
  'close': types.Numeric,
  'adjusted_close': types.Numeric,
  'volume': types.Numeric,
  'dividend_amount': types.Numeric,
  'split_coefficient': types.Numeric,
  'symbol': types.Text,
  'capture_date': types.Date
  }

tic = time.perf_counter()
df_test.to_sql(
  con=conn,
  schema='test',
  name='shareprices_daily',   
  index=False, 
  if_exists='append', 
  method='multi', 
  chunksize=10000)
toc = time.perf_counter()
print('Upload time was', round(toc - tic, 2), 'seconds for pd.to_sql')


tic = time.perf_counter()
df_test.to_sql(
  con=conn,
  schema='test',
  name='shareprices_daily_test_idx',   
  index=False, 
  if_exists='append', 
  method='multi', 
  chunksize=10000,
  dtype=sql_types)
toc = time.perf_counter()
print('Upload time was', round(toc - tic, 2), 'seconds for pd.to_sql')


# Simfin industry data -----------------------------------------------------------------------------------------------------

conn = pg_connect('Bremor*74')
# Read data
df = pd.read_csv("C:/Users/brent/Downloads/us-companies.csv", sep = ";")

# Check columns returns as expected
if df.columns.tolist() != ['Ticker', 'SimFinId', 'Company Name', 'IndustryId']:
  raise ValueError("Retrieved column names have changes from, 'Ticker', 'SimFinId', 'Company Name', 'IndustryId'")

# Append capture date
current_date = dt.datetime.today().date()
df['capture_date'] = current_date

# Rename columns
df.rename(columns={'Ticker': 'ticker', 'SimFinId': 'simfin_id', 'Company Name': 'company_name', 'IndustryId': 'industry_id'}, inplace = True)

# Grab existing data and selet only new records
existing = pd.read_sql(sql=text("""select * from simfin.us_companies"""), con=conn)
update_df = pd.concat([df, existing]).sort_values(by=['ticker','capture_date']).drop_duplicates(subset=['ticker','simfin_id','industry_id'], keep=False)
update_df = update_df[update_df['capture_date'] == current_date]

update_df.to_sql(name='us_companies', con=conn, schema='simfin', 
                  index=False, if_exists='append', method='multi', chunksize=10000)
print(len(update_df), "records inserted")



