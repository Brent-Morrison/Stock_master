# Libraries
from functions import *
import json

# Load config file
with open('config.json', 'r') as f:
    config = json.load(f)


# Connect to db
conn = pg_connect(config['pg_password'])


# Unadjusted data
df0 = pd.read_sql(
sql=text("""
    select 
    symbol
    ,date_stamp
    ,close
    ,volume 
    ,dividend_amount 
    ,split_coefficient 
    ,capture_date 
    from access_layer.shareprices_daily_raw
    where symbol in ('AAPL','DLTR','XOM')
    order by 1,2 desc
""")
,con=conn
)

df1 = df0.copy()

df1['div_adj_fctr'] = np.where( \
    # condition
    df1['dividend_amount'].shift(1) != 0 \
    # true
    ,(df1['close'] - df1['dividend_amount'].shift(1)) / df1['close'] \
    # false
    ,0 #df1['div_adj_fctr'].shift(1)
    )

df1['max_date'] = df1.groupby('symbol')['date_stamp'].transform(max)

df1['div_adj_fctr0'] = np.select( \
    [
        df1['max_date'] == df1['date_stamp'],                               # cond 1
        df1['dividend_amount'].shift(1) != 0                                # cond 2
    ]  
    ,
    [
        1,                                                                  # choice 1
        (df1['close'] - df1['dividend_amount'].shift(1)) / df1['close']     # choice 2
    ]
    ,np.nan                                                                 # default
    )

df1['div_adj_fctr1'] = df1.groupby('symbol')['div_adj_fctr0'].fillna(method="ffill")

df1['rtn_ari_1d'] = (df1['close']-(df1['close'].shift(-1)*((df1['close'].shift(-1)-df1['dividend_amount'])/df1['close'].shift(-1)))) / \
    (df1['close'].shift(-1)*(df1['close'].shift(-1)-df1['dividend_amount'])/df1['close'].shift(-1))

df1['rtn_log_1d'] = np.log(1+df1['rtn_ari_1d'])


df1['adjusted_close'] = np.where( \
    # condition
    df1['max_date'] == df1['date_stamp'] \
    # true
    ,df1['close'] \
    # false
    ,df1['adjusted_close'].shift(1) / (1 + df1['rtn_ari_1d'].shift(1))
    #,df1['close'].shift(1)
    )