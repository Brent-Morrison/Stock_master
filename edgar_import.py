from sqlalchemy import create_engine
import pandas as pd

engine = create_engine('postgresql://postgres:pass@localhostlocalhost:5432/stock_master')
cols = list(pd.read_csv('C:/Users/brent/Downloads/num.txt', delimiter = '\t', nrows = 1))
df = pd.read_csv('C:/Users/brent/Downloads/num.txt', delimiter = '\t') #usecols =[i for i in cols if i != 'footnote'])
df.to_sql('pandas_db', engine)

print(cols)
