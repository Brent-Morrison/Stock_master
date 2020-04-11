from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime as dt

# View columns of file to be read in
cols = list(pd.read_csv('C:/Users/brent/Downloads/num.txt', delimiter = '\t', nrows = 1))
print(cols)

# Read file
# If required, to exlude columns (usecols =[i for i in cols if i != 'footnote'])
df = pd.read_csv('C:/Users/brent/Downloads/num.txt', delimiter = '\t') 

~ Subset for test
df_test = df.iloc[:9]

df_test.info()

# Convert date
df_test['ddate_new'] = pd.to_datetime(df_test['ddate'], format='%Y%m%d').dt.strftime("%Y-%m-%d")

# Convert date
engine = create_engine('postgresql://postgres:*******@localhost:5432/stock_master')
df.to_sql('edgar.num_stage', engine)
