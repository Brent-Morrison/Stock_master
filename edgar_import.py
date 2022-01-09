##############################################################################
#
# Script to extract data from SEC website
# at https://www.sec.gov/dera/data/financial-statement-data-sets.html
#
##############################################################################

# Libraries
from functions import *

# Connect to postgres database
conn = pg_connect('')

# Libraries
from sqlalchemy import create_engine, MetaData, Table, text
import psycopg2
import pandas as pd
import numpy as np
import datetime as dt
import requests
import os
import io as io
from zipfile import ZipFile


# Create list of URL's for dates required
# Note that 2020 Q1 is under a different url
# https://www.sec.gov/files/node/add/data_distribution/2020q1.zip

# url_list = ['https://www.sec.gov/files/node/add/data_distribution/2020q2.zip']

# Prior quarters
start_year = 2021
end_year = 2021
start_qtr = 2
end_qtr = 3

# Database write method (string: pandas OR stringio)
db_write_method = 'pandas'

base_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/'
url_list = [base_url+str(y)+'q'+str(q)+'.zip' 
            for y in range(start_year, end_year+1) 
            for q in range(start_qtr,end_qtr+1)]

# ADD ERROR CHECK FOR URL VALIDITY PER
# https://stackoverflow.com/questions/16778435/python-check-if-website-exists

# Connect to postgres database 2
# ?gssencmode=disable' per https://stackoverflow.com/questions/59190010/psycopg2-operationalerror-fatal-unsupported-frontend-protocol-1234-5679-serve
#engine = create_engine('postgresql://postgres:'+password+
#                        '@localhost:5432/stock_master?gssencmode=disable')
#conn = engine.connect()

# Tables
meta = MetaData(conn)
meta.reflect(schema='edgar')
sub_stage = meta.tables['edgar.sub_stage']
tag_stage = meta.tables['edgar.tag_stage']
num_stage = meta.tables['edgar.num_stage']

# Dictionary for logging count of lines per file
zf_info_dict = {}

# Looped implementation
for url in url_list:
    resp = requests.get(url)
    zf = ZipFile(io.BytesIO(resp.content)) # Open the Zipfile
    zf_files = zf.infolist() # List containing a ZipInfo object for each member of the archive
    
    # Extract the quarter from the url string
    # Set this manually for current url which has differing length
    qtr = url[66:72]

    # Loop over text files in the downloaded zip file and read to individual 
    # dataframes.  Exclude the readme & pre files.
    zf_files_dict = {}
    for zfile in zf_files:
        if zfile.filename == 'readme.htm':
            continue
        if zfile.filename == 'pre.txt':
            continue     
        
        # For the sub and num files
        if zfile.filename != 'tag.txt':
            zf_info_dict[zfile.filename+'_'+qtr] = len(zf.open(zfile.filename).readlines())-1
            try:
                zf_files_dict[zfile.filename] = pd.read_csv(zf.open(zfile.filename),
                    delimiter='\t', encoding='utf-8')
            except UnicodeDecodeError:
                print('{f}{q} is not a utf-8 file'.format(f=zfile.filename, q=qtr))
                try:
                    zf_files_dict[zfile.filename] = pd.read_csv(zf.open(zfile.filename),
                        delimiter='\t', encoding='ISO-8859-1')
                except UnicodeDecodeError:
                    print('{f}{q} is not a ISO-8859-1 file'.format(f=zfile.filename, q=qtr))
                finally:
                    pass
            finally:
                pass
        
        # Tag does not load properly, save locally using 'extractall()' in order to use (delimiter='\t|\n')
        else:
            zf_info_dict[zfile.filename+'_'+qtr] = len(zf.open(zfile.filename).readlines())-1
            # Extract all members from the archive to the current working directory
            zf.extractall(members = ['tag.txt'])
            try:
                tag = pd.read_csv('tag.txt', delimiter='\t|\n', encoding='utf-8')         
            except UnicodeDecodeError:
                print('{f}_{q} is not utf-8 encoding'.format(f=zfile.filename, q=qtr))
                try:
                    tag = pd.read_csv('tag.txt', delimiter='\t|\n', encoding='ISO-8859-1')
                except UnicodeDecodeError:
                    print('{f}_{q} is not ISO-8859-5 encoding'.format(f=zfile.filename, q=qtr))
                else:
                    print('{f}_{q} opened with ISO-8859-1 encoding'.format(f=zfile.filename, q=qtr))
            else:
                print('{f}_{q} opened with utf-8 encoding'.format(f=zfile.filename, q=qtr))

            finally:
                os.remove('tag.txt')

    # Extract to individual dataframes and unsure columns align to database
    # table structure.  Add column (sec_qtr) indicating the zip file data originates from.
    # We are only loading specific columns from the sub file.
    sub = zf_files_dict['sub.txt']
    sub_cols_to_drop = ['bas1','bas2','baph','countryma','stprma','cityma', 
        'zipma', 'mas1','mas2','countryinc','stprinc','ein',
        'accepted']
    sub = sub.drop(sub_cols_to_drop, axis=1)
    sub = sub[['adsh','cik','name','sic','countryba','stprba','cityba',
        'zipba','former','changed','afs','wksi','fye','form','period','fy',
        'fp','filed','prevrpt','detail','instance','nciks','aciks']].copy()
    sub['sec_qtr']=qtr
    
    tag = tag[['tag','version','custom','abstract','datatype','iord','crdr',
        'tlabel','doc']].copy()
    tag['sec_qtr']=qtr
    
    num = zf_files_dict['num.txt']
    num = num[['adsh','tag','version','ddate','qtrs','uom',
        'coreg','value','footnote']].copy()
    num['sec_qtr']=qtr

    # Clear table contents (this is redundent if 'to_sql' specifies replace)
    conn.execute(sub_stage.delete())
    conn.execute(tag_stage.delete())
    conn.execute(num_stage.delete())

    # Insert to postgres database
    if db_write_method == 'pandas':
        sub.to_sql(name='sub_stage', con=conn, schema='edgar', 
                    index=False, if_exists='append', method='multi', chunksize=50000)
        tag.to_sql(name='tag_stage', con=conn, schema='edgar', 
                    index=False, if_exists='append', method='multi', chunksize=50000)
        num.to_sql(name='num_stage', con=conn, schema='edgar', 
                    index=False, if_exists='append', method='multi', chunksize=50000)
        print('{} pushed to DB'.format(qtr))
    else:
        copy_from_stringio(conn=conn, df=sub, table='edgar.sub_stage')
        copy_from_stringio(conn=conn, df=tag, table='edgar.tag_stage')
        copy_from_stringio(conn=conn, df=num, table='edgar.num_stage')

    # Push to bad data and "final" tables
    sql_file = open("edgar_push_stage_final.sql")
    text_sql = text(sql_file.read())
    conn.execute(text_sql)
    print('{} pushed to final tables'.format(qtr))

    # Clean up
    conn.execute(sub_stage.delete())
    conn.execute(tag_stage.delete())
    conn.execute(num_stage.delete())

    # Close zip
    zf.close()


# Save log file
log = pd.DataFrame.from_dict(zf_info_dict, orient='index', columns=['line_count'])
log.to_csv('log.csv')


# Close connection
conn.close()











##############################################################################
#
# LOAD VARIOUS TICKER LISTS
# The script below does not allow for updating of these tables
# Use df.drop_duplicates after unioning old (from DB) and new (from web)
# in order to select only new records
#
##############################################################################

# Connect to postgres database
conn = pg_connect('')

meta = MetaData(conn)
meta.reflect(schema='alpha_vantage')
sp_1000     = meta.tables['alpha_vantage.sp_1000']
rs_1000     = meta.tables['alpha_vantage.rs_1000']
sp_500      = meta.tables['alpha_vantage.sp_500']
sp_500_dlta = meta.tables['alpha_vantage.sp_500_dlta']


# S&P1000 grab data
sp_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_1000_companies'
sp_1000_url = pd.read_html(sp_url)
sp_1000_wik = sp_1000_url[5]
sp_1000_wik['capture_date'] = dt.datetime.today().date()
sp_1000_wik.drop('SEC filings', axis=1, inplace=True)
sp_1000_wik.rename(columns={'Company': 'company',
                            'Ticker symbol': 'ticker',
                            'GICS economic sector': 'gics_sector',
                            'GICS sub-industry': 'gics_industry',
                            'CIK': 'cik'},
                            inplace=True)

# S&P1000 insert to postgres database
sp_1000_wik.to_sql(name='sp_1000', con=conn, schema='alpha_vantage', 
                index=False, if_exists='append', method='multi', chunksize=50000)


# Russell 1000 grab data
rs_url = 'https://en.wikipedia.org/wiki/Russell_1000_Index'
russ_1000_url = pd.read_html(rs_url)
rs_1000_wik = russ_1000_url[2]
rs_1000_wik['capture_date'] = dt.datetime.today().date()
rs_1000_wik.rename(columns={'Company': 'company',
                            'Ticker': 'ticker'},
                            inplace=True)

# Russell 1000 insert to postgres database
rs_1000_wik.to_sql(name='rs_1000', con=conn, schema='alpha_vantage', 
                index=False, if_exists='append', method='multi', chunksize=50000)


# S&P500 grab data
sp5_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
sp_500_url = pd.read_html(sp5_url)
sp_500_wik = sp_500_url[0]
sp_500_wik.drop(['SEC filings', 'Headquarters Location','Founded'], axis=1, inplace=True)
sp_500_wik.columns = ['symbol','name','gics_sector','gics_industry','date_added','cik']
sp_500_wik['date_added'] = sp_500_wik['date_added'].str.slice(0,10)
sp_500_wik['capture_date'] = dt.datetime.today().date()

# SP500 insert to postgres database
sp_500_wik.to_sql(name='sp_500', con=conn, schema='alpha_vantage', 
                index=False, if_exists='append', method='multi', chunksize=50000)


# S&P500 delt grab data
sp_500_dlta_wik = sp_500_url[1]
sp_500_dlta_wik.columns = sp_500_dlta_wik.columns.droplevel(0)
sp_500_dlta_wik.columns = ['date','ticker_added','name_added',
                            'ticker_removed','name_removed',
                            'reason']
sp_500_dlta_wik['capture_date'] = dt.datetime.today().date()
sp_500_dlta_wik['date'] = pd.to_datetime(sp_500_dlta_wik['date'], infer_datetime_format=True)

# SP500 insert to postgres database
sp_500_dlta_wik.to_sql(name='sp_500_dlta', con=conn, schema='alpha_vantage', 
                index=False, if_exists='append', method='multi', chunksize=50000)


# Close connection
conn.close()


