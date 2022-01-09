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
    ,"timestamp"
    ,close
    ,volume 
    ,dividend_amount 
    ,split_coefficient 
    ,capture_date 
    from 
        (	-- Capture most recent version of price data (i.e., split & dividend adjusted)
            select 
            sd.* 
            ,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
            from alpha_vantage.shareprices_daily sd 
        ) t1
    where row_num = 1
    and symbol in ('AAPL','DLTR','XOM')
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

df1['max_date'] = df1.groupby('symbol')['timestamp'].transform(max)

df1['div_adj_fctr0'] = np.select( \
    [df1['max_date'] == df1['timestamp'], df1['dividend_amount'].shift(1) != 0]  #.groupby('symbol')
    ,[1, (df1['close'] - df1['dividend_amount'].shift(1)) / df1['close']]
    ,np.nan
    )

df1['div_adj_fctr1'] = df1.groupby('symbol')['div_adj_fctr0'].fillna(method="ffill")

df1['rtn_ari_1d'] = (df1['close']-(df1['close'].shift(-1)*((df1['close'].shift(-1)-df1['dividend_amount'])/df1['close'].shift(-1)))) / \
    (df1['close'].shift(-1)*(df1['close'].shift(-1)-df1['dividend_amount'])/df1['close'].shift(-1))

df1['rtn_log_1d'] = np.log(1+df1['rtn_ari_1d'])
