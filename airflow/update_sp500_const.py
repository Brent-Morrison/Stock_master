"""
Grab SP500 constituent data from Wikipedia and insert into postgres tables

"""

# Libraries
import sys

# In order to enable loading of a module, add location to $PYTHONPATH.  To be replaced once package set up
# https://askubuntu.com/questions/470982/how-to-add-a-python-module-to-syspath
project_path = 'C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres'
sys.path.insert(1, project_path)  

from functions import *
import json


# Script parameters passed from Airflow
database = sys.argv[1]


# Load config file
with open(project_path+'\\config.json', 'r') as f:
    config = json.load(f)


# Connect to db
conn = pg_connect(pg_password=config['pg_password'], database=database)


# Update SP500 constituents
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
    {'date_stamp'  : sp500_delta['Date','Date'], 
    'added'        : sp500_delta['Added','Ticker'],
    'added_name'   : sp500_delta['Added','Security'],
    'removed'      : sp500_delta['Removed','Ticker'],
    'removed_name' : sp500_delta['Removed','Security']}
)
df['date_stamp'] = df['date_stamp'].apply(lambda x: dt.datetime.strptime(x, "%B %d, %Y"))
df['capture_date'] = dt.datetime.today().date()

# Clear postgres temp table
conn.execution_options(autocommit=True).execute(text("delete from reference.sp500_cons_temp where date_stamp is not null"))

# Insert wikipedia data into postgres database
df.to_sql(
    name='sp500_cons_temp', con=conn, schema='reference', 
    index=False, if_exists='append', method='multi', chunksize=10000
    )

# Find the most recent SP500 change date
conn.execution_options(autocommit=True).execute(text("""
    with max_date_tbl as (
    select max(max_date) as max_date 
    from (
        select 
        greatest(
            min_date, 
            case when max_date = '9998-12-31'::date then '1980-12-31'::date else max_date end
            ) as max_date 
        from reference.sp500_cons
        ) t1
    )
    update reference.sp500_cons_mdate
    set max_date = max_date_tbl.max_date from max_date_tbl
    """))

# Insert re additions
conn.execution_options(autocommit=True).execute(text("""
    with insert_tbl as (
        select
        added 				as ticker
        ,date_stamp 		as min_date
        ,'9998-12-31'::date as max_date
        ,capture_date
        from reference.sp500_cons_temp 
        where date_stamp > (select max_date from reference.sp500_cons_mdate)
        and added is not null
        )
    insert into reference.sp500_cons select * from insert_tbl
    """))

# Update re removals
conn.execution_options(autocommit=True).execute(text("""
    with update_tbl as (
        select
        t.removed as ticker
        ,c.min_date
        ,t.date_stamp as max_date
        ,t.capture_date
        from reference.sp500_cons_temp t
        left join reference.sp500_cons c
        on t.removed = c.ticker 
        where t.date_stamp > (select max_date from reference.sp500_cons_mdate)
        and t.removed is not null
        and c.max_date = '9998-12-31'::date
        )
    update reference.sp500_cons c
    set max_date = u.max_date,
        capture_date = u.capture_date
    from update_tbl u
    where c.ticker = u.ticker
    """))


# Close connection
conn.close()


print('S&P500 consituent list updated')