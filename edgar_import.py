##############################################################################
# Script to extract data from SEC website
# at https://www.sec.gov/dera/data/financial-statement-data-sets.html
#
##############################################################################


password = ''

# Libraries
from sqlalchemy import create_engine, MetaData, Table
import psycopg2
import pandas as pd
import numpy as np
import datetime as dt
import requests
import io as io
from zipfile import ZipFile


# Create list of URL's for dates required
# Note that the current month is a different url
# https://www.sec.gov/files/node/add/data_distribution/2020q1.zip

start_year = 2009
end_year = 2009
start_qtr = 3
end_qtr = 4

base_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/'
url_list = [base_url+str(y)+'q'+str(q)+'.zip' 
            for y in range(start_year, end_year+1) 
            for q in range(start_qtr,end_qtr+1)]
print(url_list)


# Connect to postgres database
engine = create_engine('postgresql://postgres:'+password+
                        '@localhost:5432/stock_master')
conn = engine.connect()
meta = MetaData(engine)
meta.reflect(schema='edgar')
sub_stage = meta.tables['edgar.sub_stage']
tag_stage = meta.tables['edgar.tag_stage']
num_stage = meta.tables['edgar.num_stage']


# Looped implementation
for url in url_list:
    resp = requests.get(url)
    zf = ZipFile(io.BytesIO(resp.content))
    zf_files = zf.infolist()
    zf_file_names = zf.namelist() 

    # Loop over text files in the downloaded zip file and read to individual 
    # dataframes.  Exclude the readme file
    zf_files_dict = {}
    for zfile in zf_files:
        if zfile.filename == 'readme.htm':
            continue
        if zfile.filename == 'pre.txt':
            continue  
        #print(zfile.filename) 
        zf_files_dict[zfile.filename] = pd.read_csv(zf.open(zfile.filename), 
                                        delimiter = '\t', encoding='utf-8')

    # Extract to individual dataframes and unsure columns align to database
    # table structure.  We are only loading specific columns from sub
    sub = zf_files_dict['sub.txt']
    sub_cols_to_drop = ['bas1','bas2','baph','countryma','stprma','cityma', 
        'zipma', 'mas1','mas2','countryinc','stprinc','ein',
        'accepted']
    sub = sub.drop(sub_cols_to_drop, axis=1)
    # THIS NEEDS DE-BUGGING
    #sub = sub[['adsh','cik','name','sic','countryba','stprba','cityba',
    #    'zipba','former','changed','afs','wksi','fye','form' 'period','fy',
    #    'fp','filed','prevrpt','detail','instance','nciks','aciks']]
    tag = zf_files_dict['tag.txt']
    tag = tag[['tag','version','custom','abstract','datatype','iord','crdr',
                'tlabel','doc']]
    num = zf_files_dict['num.txt']
    num = num[['adsh','tag','version','ddate','qtrs','uom',
                'coreg','value','footnote']]

    # Clear table contents (this is redundent if 'to_sql' specifies replace)
    #conn.execute(sub_stage.delete())
    #conn.execute(tag_stage.delete())
    #conn.execute(pre_stage.delete())
    #conn.execute(num_stage.delete())

    # Insert to postgres database
    sub.to_sql(name='sub_stage', con=engine, schema='edgar', 
                index=False, if_exists='append')
    tag.to_sql(name='tag_stage', con=engine, schema='edgar', 
                index=False, if_exists='append')
    num.to_sql(name='num_stage', con=engine, schema='edgar', 
                index=False, if_exists='append')

# Close connection
conn.close()


# References
# https://stackoverflow.com/questions/26942476/reading-csv-zipped-files-in-python
# https://stackoverflow.com/questions/23419322/download-a-zip-file-and-extract-it-in-memory-using-python3
# https://github.com/pandas-dev/pandas/issues/14553
