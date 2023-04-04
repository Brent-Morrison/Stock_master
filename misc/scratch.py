# Libraries
import sys
# In order to enable loading of a module, add location to $PYTHONPATH.  To be replaced once package set up
# https://askubuntu.com/questions/470982/how-to-add-a-python-module-to-syspath
sys.path.insert(1, 'C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres') 
from functions import *
import json

# Load config file
with open('C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\config.json', 'r') as f:
    config = json.load(f)


# Connect to db
conn = pg_connect(pg_password=config['pg_password'], database='stock_master')


# Grab data from SEC website
hdrs = {'User-Agent':'brentjohnmorrison@hotmail.com'}#, 'Accept-Encoding':'json', 'Host':'https://data.sec.gov/'}
cik='0001326801'
url = 'https://data.sec.gov/submissions/CIK{}.json'.format(cik)
res = requests.get(url, headers=hdrs)
res_data = json.loads(res.text)
res_data['formerNames']
filings = res_data['filings']['recent']
filings_df_meta = pd.DataFrame.from_dict(filings)

# Opening JSON file
f = open('C:\\Users\\brent\\Documents\\TRADING_Current\\CIK0001420800.json')
data = json.load(f)




# Update SP500 constituents
import pandas as pd
import datetime as dt
sp500_wik_list = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")
sp500_delta = sp500_wik_list[1]

# Check structure of df retrieved (better with try / except)
sp500_delta_cols = [
    ('Date'   ,'Date'    ),
    ('Added'  ,'Ticker'  ),
    ('Added'  ,'Security'),
    ('Removed','Ticker'  ),
    ('Removed','Security'),
    ('Reason' ,'Reason'  )]

try:
    list(sp500_delta.columns) == sp500_delta_cols
except:
    print('Structure of table retrieved has changed\nExpected:', sp500_delta_cols, '\nReturned:',list(sp500_delta.columns))

# New df with desired format
df = pd.DataFrame(
    {'date_stamp'   : sp500_delta['Date','Date'], 
     'added'        : sp500_delta['Added','Ticker'],
     'added_name'   : sp500_delta['Added','Security'],
     'removed'      : sp500_delta['Removed','Ticker'],
     'removed_name' : sp500_delta['Removed','Security']}
)
df['date_stamp'] = df['date_stamp'].apply(lambda x: dt.datetime.strptime(x, "%B %d, %Y"))
df['capture_date'] = dt.datetime.today().date()

# Insert to postgres database
df.to_sql(
    name='sp500_cons_temp', con=conn, schema='reference', 
    index=False, if_exists='append', method='multi', chunksize=10000
    )

