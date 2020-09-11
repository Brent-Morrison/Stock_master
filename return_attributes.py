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

# Daily stock data
df = df_raw.copy()
df['month'] = df.index.month
df['year'] = df.index.year
df['rtn_ari_1d'] = df.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=1)/x.shift(periods=1))
df['rtn_ari_1d_mkt'] = df.groupby('symbol').sp500.apply(lambda x: x.diff(periods=1)/x.shift(periods=1))
df['rtn_log_1d'] = df.groupby('symbol').adjusted_close.apply(lambda x: np.log(x).diff(periods=1))
df['vol_ari_20d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(20).std()*m.sqrt(252))
df['vol_ari_60d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(60).std()*m.sqrt(252))
df['vol_ari_120d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(120).std()*m.sqrt(252))
df['skew_ari_120d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(120).skew())
df['kurt_ari_120d'] = df.groupby('symbol').rtn_ari_1d.apply(lambda x: x.rolling(120).kurt())
df['amihud'] = abs(df.rtn_ari_1d) / (df.volume * df.adjusted_close / 10e7)
df['amihud_3m'] = df.groupby('symbol').amihud.apply(lambda x: x.rolling(60).mean())
df['amihud_vol_3m'] = df.groupby('symbol').amihud.apply(lambda x: x.rolling(60).std()*m.sqrt(252))
# df['smax_20d'] = df.groupby('symbol').resample('M').rtn_log_1d.transform(lambda x: x.nlargest(5).sum()).reset_index(drop=True)
# Cannot assign https://stackoverflow.com/questions/20737811/attach-a-calculated-column-to-an-existing-dataframe
# On a rolling basis https://stackoverflow.com/questions/56555253/pandas-rolling-2nd-largest-value
# and also https://stackoverflow.com/questions/51445439/speed-up-finding-the-average-of-top-5-numbers-from-a-rolling-window-in-python
smax_20d_s = df.groupby(['symbol','year','month']).rtn_ari_1d.transform(lambda x: x.nlargest(5).mean()) 
df['smax_20d'] = smax_20d_s

# Rolling correlations function
def roll_corr_120(x):
    return pd.DataFrame(x['rtn_ari_1d'].rolling(window=120).corr(x['rtn_ari_1d_mkt']))

df['cor_rtn_1d_mkt_120d'] = df.groupby('symbol')[['rtn_ari_1d','rtn_ari_1d_mkt']].apply(roll_corr_120)

# Rolling beta function
def roll_beta_120(x):
    #return pd.DataFrame(x['rtn_ari_1d'].rolling(window=120).cov(x['rtn_ari_1d_mkt'])) / pd.DataFrame(x['rtn_ari_1d'].rolling(window=120).var())
    cov = pd.DataFrame(x['rtn_ari_1d'].rolling(window=120).cov(x['rtn_ari_1d_mkt']))
    var = pd.DataFrame(x['rtn_ari_1d_mkt'].rolling(window=120).var())
    beta = pd.concat([cov,var], axis=1)
    beta['beta'] = beta.iloc[:,0] / beta.iloc[:,1]
    beta.drop(beta.columns[[0,1]], axis=1, inplace=True)
    return beta

df['beta_rtn_1d_mkt_120d'] = df.groupby('symbol')[['rtn_ari_1d','rtn_ari_1d_mkt']].apply(roll_beta_120)

# Monthly stock data
dfm = df.groupby(['symbol','gics_sector','year','month']).agg(
    {
        'close'                 : 'last',
        'adjusted_close'        : 'last',
        'volume'                : 'mean',
        'rtn_log_1d'            : 'sum',
        'amihud'                : 'mean',
        'amihud_3m'             : 'last',
        'amihud_vol_3m'         : 'last',
        'vol_ari_20d'           : 'last',
        'vol_ari_60d'           : 'last',
        'vol_ari_120d'          : 'last',
        'skew_ari_120d'         : 'last',
        'kurt_ari_120d'         : 'last',
        'smax_20d'              : 'last',
        'cor_rtn_1d_mkt_120d'   : 'last',
        'beta_rtn_1d_mkt_120d'  : 'last'
    }
    ).reset_index()
dfm['day'] = 1
dfm['date'] = pd.to_datetime(dfm[['year','month','day']]).dt.to_period('M').dt.to_timestamp('M')
dfm.set_index('date', inplace=True)
dfm.drop(['day'], axis=1, inplace=True)
# smax_20d needs to on a rolling a basis
dfm['smax_20d'] = dfm.smax_20d / dfm.vol_ari_20d
dfm['smax_20d_dcl'] = dfm.groupby('month').smax_20d.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['cor_rtn_1d_mkt_120d_dcl'] = dfm.groupby('month').cor_rtn_1d_mkt_120d.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['beta_rtn_1d_mkt_120d_dcl'] = dfm.groupby('month').beta_rtn_1d_mkt_120d.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_1m'] = dfm.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=1)/x.shift(periods=1))
dfm['rtn_ari_1m_dcl'] = dfm.groupby('month').rtn_ari_1m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_1m_ind_dcl'] = dfm.groupby(['month','gics_sector']).rtn_ari_1m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_3m'] = dfm.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=3)/x.shift(periods=3))
dfm['rtn_ari_3m_dcl'] = dfm.groupby('month').rtn_ari_3m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_3m_ind_dcl'] = dfm.groupby(['month','gics_sector']).rtn_ari_3m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_6m'] = dfm.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=6)/x.shift(periods=6))
dfm['rtn_ari_6m_dcl'] = dfm.groupby('month').rtn_ari_6m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_6m_ind_dcl'] = dfm.groupby(['month','gics_sector']).rtn_ari_6m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_12m'] = dfm.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=11)/x.shift(periods=11))
dfm['rtn_ari_12m'] = dfm['rtn_ari_12m'].shift(periods=1)
dfm['rtn_ari_12m_dcl'] = dfm.groupby('month').rtn_ari_12m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['rtn_ari_12m_ind_dcl'] = dfm.groupby(['month','gics_sector']).rtn_ari_12m.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
#dfm['cor_rtn_1d_mkt_120d_dcl'] = dfm.groupby('month').cor_rtn_1d_mkt_120d.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
#dfm['beta_rtn_1d_mkt_120d_dcl'] = dfm.groupby('month').beta_rtn_1d_mkt_120d.transform(lambda x: pd.qcut(x, 10, labels=range(10,0,-1)))
dfm['fwd_rtn_ari_1m'] = dfm['rtn_ari_1m'].shift(periods=-1)
dfm['fwd_rtn_ari_3m'] = dfm['rtn_ari_3m'].shift(periods=-3)

# Industry groups
dfm_ind = dfm.groupby(['gics_sector','year','month']).agg(
    {
        'rtn_ari_1m'            : 'mean',
        'rtn_ari_3m'            : 'mean',
        'rtn_ari_6m'            : 'mean',
        'rtn_ari_12m'           : 'mean'
    }
    ).reset_index()
dfm['day'] = 1
dfm['date'] = pd.to_datetime(dfm[['year','month','day']]).dt.to_period('M').dt.to_timestamp('M')
dfm.set_index('date', inplace=True)
dfm.drop(['day'], axis=1, inplace=True)

# Close connection
conn.close()




##############################################################################
#
# TESTING
# 
#
##############################################################################

dfm.loc['2020-06-30']['gics_sector'].value_counts()
test = dfm.query('date == "2020-06-30" & gics_sector == "Energy"')

#Write to csv for testing
dfm.to_csv('beta.csv')

dfm.head()

AAPL = dfm[dfm['symbol'] == 'JPM'] #.to_csv('jpm_test.csv')

# Rolling beta by group
# https://stackoverflow.com/questions/39501277/efficient-python-pandas-stock-beta-calculation-on-many-dataframes
# https://stackoverflow.com/questions/34802972/python-pandas-calculate-rolling-stock-beta-using-rolling-apply-to-groupby-object
# https://blog.quantinsti.com/asset-beta-market-beta-python/

# Testing rolling correlations
beta_test = df[df['symbol'].isin(['AAPL','JPM'])][['symbol','adjusted_close','sp500']].copy()
beta_test['rtn_ari_1d'] = beta_test.groupby('symbol').adjusted_close.apply(lambda x: x.diff(periods=1)/x.shift(periods=1))
beta_test['rtn_ari_1d_mkt'] = beta_test.groupby('symbol').sp500.apply(lambda x: x.diff(periods=1)/x.shift(periods=1))


# Function methodology
def roll_beta_120x(x):
    #return pd.DataFrame(x['rtn_ari_1d'].rolling(window=120).cov(x['rtn_ari_1d_mkt'])) / pd.DataFrame(x['rtn_ari_1d'].rolling(window=120).var())
    cov = pd.DataFrame(x['rtn_ari_1d'].rolling(window=120).cov(x['rtn_ari_1d_mkt']))
    var = pd.DataFrame(x['rtn_ari_1d_mkt'].rolling(window=120).var())
    beta = pd.concat([cov,var], axis=1)
    beta['beta'] = beta.iloc[:,0] / beta.iloc[:,1]
    beta.drop(beta.columns[[0,1]], axis=1, inplace=True)
    return beta
beta_test['beta_rtn_1d_mkt_120d'] = beta_test.groupby('symbol')[['rtn_ari_1d','rtn_ari_1d_mkt']].apply(roll_beta_120)

beta_test.to_csv('beta.csv')

cov = pd.DataFrame(beta_test['rtn_ari_1d'].rolling(window=120).cov(beta_test['rtn_ari_1d_mkt']))
var = pd.DataFrame(beta_test['rtn_ari_1d'].rolling(window=120).var())
beta = pd.concat([cov,var], axis=1)
beta['beta'] = beta.iloc[:,0] / beta.iloc[:,1]
beta.drop(beta.columns[[0,1]], axis=1, inplace=True)