# Libraries
import sys
# In order to enable loading of a module, add location to $PYTHONPATH.  To be replaced once package set up
# https://askubuntu.com/questions/470982/how-to-add-a-python-module-to-syspath
sys.path.insert(1, 'C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\python') 
from functions import *
import json

# Load config file
with open('C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres\\config.json', 'r') as f:
    config = json.load(f)


# Connect to db
conn = pg_connect(pg_password=config['pg_password'], database='stock_master')


# 1. Update the table 'edgar.company_tickers' with CIK and ticker data using the python function 'update_sec_company_tickers'
update_sec_company_tickers(conn)


# 2. Update the table 'alpha_vantage.active_delisted' with IPO and delist date data using the python function 'update_active_delisted'
update_active_delisted(conn, apikey=config['av_apikey'])






# --------------------------------------------------------------------------------------------------------------------------

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


# urls
skew_url = 'https://cdn.cboe.com/api/global/us_indices/daily_prices/SKEW_History.csv'

# Get data
skew = pd.read_csv(skew_url)
