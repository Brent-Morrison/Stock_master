##############################################################################
#
# Script to extract data from SEC website
# at https://www.sec.gov/dera/data/financial-statement-data-sets.html
#
##############################################################################


password = ''

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
# Note that the current quarter is a different url
# https://www.sec.gov/files/node/add/data_distribution/2020q1.zip

# url_list = ['https://www.sec.gov/files/node/add/data_distribution/2020q1.zip']

# Prior quarters
start_year = 2017
end_year = 2017
start_qtr = 1
end_qtr = 4

base_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/'
url_list = [base_url+str(y)+'q'+str(q)+'.zip' 
            for y in range(start_year, end_year+1) 
            for q in range(start_qtr,end_qtr+1)]


# Connect to postgres database
engine = create_engine('postgresql://postgres:'+password+
                        '@localhost:5432/stock_master')
conn = engine.connect()
meta = MetaData(engine)
meta.reflect(schema='edgar')
sub_stage = meta.tables['edgar.sub_stage']
tag_stage = meta.tables['edgar.tag_stage']
num_stage = meta.tables['edgar.num_stage']

# Dictionary for logging count of lines per file
zf_info_dict = {}

# Looped implementation
for url in url_list:
    resp = requests.get(url)
    zf = ZipFile(io.BytesIO(resp.content))
    zf_files = zf.infolist()
    
    # Set this string manually for current url which has differing length
    qtr = url[66:72]

    # Loop over text files in the downloaded zip file and read to individual 
    # dataframes.  Exclude the readme & pre files.
    zf_files_dict = {}
    for zfile in zf_files:
        if zfile.filename == 'readme.htm':
            continue
        if zfile.filename == 'pre.txt':
            continue
        # Tag does not load properly, workaround in order to use (delimiter='\t|\n')
        if zfile.filename == 'tag.txt':
            zf.extractall(members = ['tag.txt'])
            tag = pd.read_csv('tag.txt', 
                        delimiter='\t|\n', encoding='utf-8')
            os.remove('tag.txt')
        zf_info_dict[zfile.filename+'_'+qtr] = len(zf.open(zfile.filename).readlines())-1
        zf_files_dict[zfile.filename] = pd.read_csv(zf.open(zfile.filename), 
                                        delimiter = '\t', encoding = 'utf-8')

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
        'fp','filed','prevrpt','detail','instance','nciks','aciks']]
    sub['sec_qtr']=qtr
    tag = tag[['tag','version','custom','abstract','datatype','iord','crdr',
                'tlabel','doc']]
    tag['sec_qtr']=qtr
    num = zf_files_dict['num.txt']
    num = num[['adsh','tag','version','ddate','qtrs','uom',
                'coreg','value','footnote']]
    num['sec_qtr']=qtr

    # Clear table contents (this is redundent if 'to_sql' specifies replace)
    conn.execute(sub_stage.delete())
    conn.execute(tag_stage.delete())
    conn.execute(num_stage.delete())

    # Insert to postgres database
    sub.to_sql(name='sub_stage', con=engine, schema='edgar', 
                index=False, if_exists='append', method='multi', chunksize=10000)
    tag.to_sql(name='tag_stage', con=engine, schema='edgar', 
                index=False, if_exists='append', method='multi', chunksize=10000)
    num.to_sql(name='num_stage', con=engine, schema='edgar', 
                index=False, if_exists='append', method='multi', chunksize=10000)
    
    # Push to bad data and "final" tables
    sql_file = open("edgar_push_stage_final.sql")
    escaped_sql = text(sql_file.read())
    conn.execute(escaped_sql)

    # Close zip
    zf.close()

# Close connection
conn.close()

# Save log file
log = pd.DataFrame.from_dict(zf_info_dict, orient='index', columns=['line_count'])
log.to_csv('log.csv')








##############################################################################
#
# SCRATCH
#
##############################################################################

# This delimiter loads the full table to a pandas data frame, it does not
# work with data downloaded ina zip however
tag1 = pd.read_csv('C:/Users/brent/Downloads/tag2009q3.txt', 
        delimiter='\t|\n', encoding='utf-8')

# Pandas read_csv issue
# https://stackoverflow.com/questions/5627954/how-do-you-read-a-file-inside-a-zip-file-as-text-not-bytes/5639960#5639960

import pandas as pd
import numpy as np
import datetime as dt
import requests
import os
import io as io
from zipfile import ZipFile

start_year = 2010
end_year = 2010
start_qtr = 1
end_qtr = 2

base_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/'
url_list = [base_url+str(y)+'q'+str(q)+'.zip' 
            for y in range(start_year, end_year+1) 
            for q in range(start_qtr,end_qtr+1)]

zf_info_dict = {}
# Looped implementation
for url in url_list:
    resp = requests.get(url)
    zf = ZipFile(io.BytesIO(resp.content))
    zf_files = zf.infolist()
    
    # Set this string manually for current url which has differing length
    qtr = url[66:72]

    # Loop over text files in the downloaded zip file and read to individual 
    # dataframes.  Exclude the readme & pre files.
    zf_files_dict = {}
    for zfile in zf_files:
        # Ignore these files
        if zfile.filename == 'readme.htm':
            continue
        if zfile.filename == 'pre.txt':
            continue
        
        # For the sub and num files
        if zfile.filename != 'tag.txt':
            zf_info_dict[zfile.filename+'_'+qtr] = len(zf.open(zfile.filename).readlines())-1
            try:
                zf_files_dict[zfile.filename] = pd.read_csv(zf.open(zfile.filename), delimiter = '\t', encoding = 'utf-8')
            except UnicodeDecodeError:
                print('{f}{q} is not a utf-8 file'.format(f=zfile.filename, q=qtr))
                try:
                    zf_files_dict[zfile.filename] = pd.read_csv(zf.open(zfile.filename), delimiter='\t', encoding='ISO-8859-1')
                except UnicodeDecodeError:
                    print('{f}{q} is not a ISO-8859-1 file'.format(f=zfile.filename, q=qtr))
                finally:
                    pass
            finally:
                pass

        # Tag does not load properly, workaround in order to use (delimiter='\t|\n')
        #if zfile.filename == 'tag.txt':
        else:
            zf_info_dict[zfile.filename+'_'+qtr] = len(zf.open(zfile.filename).readlines())-1
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

        
        #with zf.open(zfile.filename) as txt_file:
        #with io.StringIO(zf.open(zfile.filename)) as txt_file:
        #with io.TextIOWrapper(zf.open(zfile.filename), encoding='utf-8', newline=None) as txt_file:
        #    zf_info_dict[zfile.filename+'_'+qtr] = len(txt_file.readlines())-1
        #    #zf_info_dict[zfile.filename+'_'+qtr] = len(zf.open(zfile.filename).readlines())-1
        #    zf_files_dict[zfile.filename] = pd.read_csv(txt_file, delimiter = '\t|\n', encoding = 'utf-8')
        #                                    , lineterminator='\n', engine = 'c')

    #tag = zf_files_dict['tag.txt']
    sub = zf_files_dict['sub.txt']
    num = zf_files_dict['num.txt']
    zf.close()

log = pd.DataFrame.from_dict(zf_info_dict, orient='index', columns=['line_count'])
log.to_csv('log.csv')

tag1 = pd.read_csv('C:/Users/brent/Documents/VS_Code/postgres/postgres/tag.txt', 
                        delimiter='\t|\n', encoding='ISO-8859-1')
