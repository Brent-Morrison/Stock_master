"""Grab S&P 500 series using yfinance and write to database

Keyword arguments:
schema (string)         schema of the insert table
table (string)          the insert table
update_to_date (date)   the update date

Returns:
Print conformation of records inserted

"""

# Libraries
import sys
sys.path.insert(1, 'C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres')  # to be replaced once package set up
from functions import *
import json


# Script parameters
database = sys.argv[1]              # 'stock_master_test'
update_to_date = sys.argv[2]        # '2022-02-04' #


# Load config file
with open('C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\config.json', 'r') as f:
    config = json.load(f)


# Connect to db
conn = pg_connect(pg_password=config['pg_password'], database=database)


# Update to date
update_to_date = dt.datetime.strptime(update_to_date, '%Y-%m-%d').date()

# Max date of existing data 
# TO DO - PARAMETERISE WITH SCHEMA.TABLE
last_date_in_db = pd.read_sql(sql=text(
"""select max(date_stamp) 
from access_layer.shareprices_daily 
where symbol = 'GSPC'"""),
con=conn)['max'][0]

# Check that the data is not already up tp date
if last_date_in_db > update_to_date:
    print('Nil records inserted - data is up to date')

else:
    df_sp500 = yf.download('^GSPC')
    df_sp500.reset_index(inplace=True)
    
    # Check the structure of the data frame returned
    if np.all(df_sp500.columns != ['Date','Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']):
        print('The data retrieved from yfinance does not conform to the expected column format')
    
    else:
        #print('OK')
        df_sp500['dividend_amount'] = 0
        df_sp500['split_coefficient'] = 0
        df_sp500['symbol'] = 'GSPC'
        df_sp500['capture_date'] = dt.datetime.today().date()
        df_sp500['data_source'] = 'yfnc'
        df_sp500.columns = ['date_stamp','open','high','low','close','adjusted_close',
        'volume','dividend_amount','split_coefficient','symbol','capture_date','data_source']

        # Re-arrange
        df_sp500 = df_sp500[['symbol','date_stamp','open','high','low','close','adjusted_close',
        'volume','dividend_amount','split_coefficient','capture_date','data_source']]

        # Convert datetime to date
        df_sp500['date_stamp'] = pd.to_datetime(df_sp500['date_stamp']).dt.date

        # Return only data post existing date
        df_sp500 = df_sp500[(df_sp500['date_stamp'] > last_date_in_db) & (df_sp500['date_stamp'] <= update_to_date)].copy()

        # Insert to postgres database
        df_sp500.to_sql(name='shareprices_daily', con=conn, schema='access_layer', 
            index=False, if_exists='append', method='multi', chunksize=10000)
        
        # TO DO - PARAMETERISE WITH SCHEMA.TABLE
        print(df_sp500.shape[0]," records inserted into access_layer.shareprices_daily")