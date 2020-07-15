##############################################################################
#
# Script to extract data from postgres database and enrich stock return
# data with various attributes
#
##############################################################################


password = ''

# Libraries
from sqlalchemy import create_engine, MetaData, Table, text
import psycopg2
import pandas as pd
import numpy as np
import datetime as dt
import math as m
import os


# Connect to postgres database
engine = create_engine('postgresql://postgres:'+password+
                        '@localhost:5432/stock_master')
conn = engine.connect()
meta = MetaData(engine)

# Read data
df_raw = pd.read_sql(
    sql = """select * from alpha_vantage.returns_view""",
    con=conn,
    index_col='timestamp',
    parse_dates={'timestamp': {'format': '%Y-%m-%d'}}
    )


df = df_raw.copy()

df['month'] = df.index.month
df['year'] = df.index.year
df['rtn_ari_1d'] = df.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=1)/x.shift(periods=1))
df['rtn_log_1d'] = df.groupby('symbol').adjusted_close.apply(lambda x: np.log(x).diff(periods=1))
df['vol_ari_20d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(20).std()*m.sqrt(252))
df['vol_ari_60d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(60).std()*m.sqrt(252))
df['vol_ari_120d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(120).std()*m.sqrt(252))
df['amihud'] = abs(df.rtn_ari_1d) / (df.volume * df.adjusted_close / 10e7)
df['amihud_3m'] = df.groupby('symbol').amihud.apply(lambda x: x.rolling(60).mean())
df['amihud_vol_3m'] = df.groupby('symbol').amihud.apply(lambda x: x.rolling(60).std()*m.sqrt(252))
# df['smax_20d'] = df.groupby('symbol').resample('M').rtn_log_1d.transform(lambda x: x.nlargest(5).sum()).reset_index(drop=True)
# Cannot assign https://stackoverflow.com/questions/20737811/attach-a-calculated-column-to-an-existing-dataframe
smax_20d_s = df.groupby(['symbol','year','month']).rtn_ari_1d.transform(lambda x: x.nlargest(5).mean()) 
df['smax_20d'] = smax_20d_s


dfm = df.groupby(['symbol','year','month']).agg(
    {
        'close'         : 'last',
        'adjusted_close': 'last',
        'volume'        : 'mean',
        'rtn_log_1d'    : 'sum',
        'amihud'        : 'mean',
        'amihud_3m'     : 'last',
        'amihud_vol_3m' : 'last',
        'vol_ari_20d'   : 'last',
        'vol_ari_60d'   : 'last',
        'vol_ari_120d'  : 'last',
        'smax_20d'      : 'last'
    }
    ).reset_index().copy()
dfm['day'] = 1
dfm['date'] = pd.to_datetime(dfm[['year','month','day']]).dt.to_period('M').dt.to_timestamp('M')
dfm.set_index('date', inplace=True)
dfm.drop(['day'], axis=1)
dfm['smax_20d'] = dfm.smax_20d / dfm.vol_ari_20d
dfm['smax_20d_dcl'] = dfm.groupby('month').smax_20d.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_1m'] = dfm.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=1)/x.shift(periods=1))
dfm['rtn_ari_1m_dcl'] = dfm.groupby('month').rtn_ari_1m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_6m'] = dfm.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=6)/x.shift(periods=6))
dfm['rtn_ari_6m_dcl'] = dfm.groupby('month').rtn_ari_6m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_12m'] = dfm.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=11)/x.shift(periods=11))
dfm['rtn_ari_12m'] = dfm['rtn_ari_12m'].shift(periods=1)


#Write to csv for testing
dfm.to_csv('dfm.csv')

dfm.head()

dfm[dfm['symbol'] == 'JPM']

# Close connection
conn.close()
