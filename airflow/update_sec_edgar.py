"""Grab SEC Edgar data from the SEC DERA website and insert into postgres tables

Parameters:
database (string)                               The database to connect to
db_write_method (string)                        Database write method ('pandas', 'stringio', 'pg_copy')

Executes:
Insert to database tables

Returns:
Log file specifying number of rows written to the data base for each table

"""

# Libraries
import sys

# In order to enable loading of a module, add location to $PYTHONPATH.  To be replaced once package set up
# https://askubuntu.com/questions/470982/how-to-add-a-python-module-to-syspath
project_path = 'C:\\Users\\brent\\Documents\\VS_Code\\postgres\\postgres'
sys.path.insert(1, project_path)  

from functions import *
import json
import shutil

tik = dt.datetime.now()

# Script parameters passed from Airflow
#database = 'stock_master'
db_write_method = 'pg_copy'
database = sys.argv[1]
#db_write_method = sys.argv[2]


# Load config file
with open(project_path+'\\config.json', 'r') as f:
    config = json.load(f)


# Connect to db
conn = pg_connect(pg_password=config['pg_password'], database=database)


# Get the quarter of the last loaded data in the database
edgar_sub_tbl = pd.read_sql(sql=text("""
    select 'sub' as table, sec_qtr, count(*) as n from edgar.sub group by 1,2 order by 2 desc, 1 asc
    """),con=conn)

last_qtr_in_db = edgar_sub_tbl.iloc[0,1]


# Last qtr in db string to date (plus one day for correct range)
start_date = dt.datetime(
    year=int(last_qtr_in_db[0:4])+1 if last_qtr_in_db[5:6] == '4' else int(last_qtr_in_db[0:4]),
    month=int(1) if last_qtr_in_db[5:6] == '4' else int(last_qtr_in_db[5:6])*3+1,
    day=1)


# Current date
current_date = dt.datetime.today() # .strftime('%Y-%m-%d')


# Range of qtrs required
qtrs_req = pd.date_range(start_date, current_date, freq='Q').tolist()

# If the list above (qtrs_req) is empty then exit the script - data is up to date.
if len(qtrs_req) > 0:

    # Convert range of qtrs required to list of urls required
    base_url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/'
    def date_to_url(i):
        return base_url+str(i.year)+'q'+str(int(i.month/3))+'.zip'

    url_req = list(map(date_to_url, qtrs_req))


    # Get status code for url's for each quarter (check if the data is available on the SEC site for download)
    url_req_status = []
    for qtr in url_req:
        r = requests.head(url_req[0])
        status = r.status_code
        url_req_status.append(status)
        time.sleep(2)


    # Two lists to list of lists
    url_status = list(map(list, zip(url_req, url_req_status)))


    # Insert valid url's (those with data available) into a separate list
    url_list = []
    for i in url_status:
        if i[1] == 200:
            url_list.append(i[0])
        else:
            print('URL (', i[0], ') is unavailable.')


    # Register postgres tables with SQL Alchemy
    meta = MetaData(conn)
    meta.reflect(schema='edgar')
    sub_stage = meta.tables['edgar.sub_stage']
    tag_stage = meta.tables['edgar.tag_stage']
    num_stage = meta.tables['edgar.num_stage']


    # Dictionary for logging count of lines per file
    zf_info_dict = {}


    # Loop over urls's and download zip files, extract content, load to database

    print('Start loop - elapsed time:', int((dt.datetime.now() - tik).total_seconds()), 'seconds')

    for url in url_list:
        resp = requests.get(url)
        zf = ZipFile(io.BytesIO(resp.content))     # Open the Zipfile
        zf_files = zf.infolist()                   # List containing a ZipInfo object for each member of the archive
        
        # Extract the quarter from the url string
        # Set this manually for current url which has differing length
        qtr = url[66:72]
        print('Processing', qtr, '- elapsed time:', int((dt.datetime.now() - tik).total_seconds()), 'seconds')

        # Loop over text files in the downloaded zip file and insert into a dictionary.
        # Read from dictionary to individual dataframes. 
        # Exclude the readme & pre files.
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
                    else:
                        print('{f} for {q} read to df with ISO-8859-1 encoding - elapsed time: {t} seconds'.format(f=zfile.filename, q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))
                    finally:
                        pass
                else:
                    print('{f} for {q} read to df with utf-8 encoding - elapsed time: {t} seconds'.format(f=zfile.filename, q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))
                finally:
                    pass
            
            # The code block below applies only to the "tag" file (it is the only file remaining after 
            # the conditions above are satisified.
            # Tag does not load properly, therefore save locally using 'extractall()', this will allow the 
            # use of (delimiter='\t|\n') for correct extraction of data.
            else:
                zf_info_dict[zfile.filename+'_'+qtr] = len(zf.open(zfile.filename).readlines())-1
                
                # Extract all members from the archive to the current working directory
                print('Start tag.txt extract - elapsed time:', int((dt.datetime.now() - tik).total_seconds()), 'seconds')
                zf.extractall(path=project_path, members=['tag.txt'])
                try:
                    tag = pd.read_csv(project_path+'\\tag.txt', delimiter='\t|\n', encoding='utf-8')
                except UnicodeDecodeError:
                    print('{f}_{q} is not utf-8 encoding'.format(f=zfile.filename, q=qtr))
                    try:
                        tag = pd.read_csv(project_path+'\\tag.txt', delimiter='\t|\n', encoding='ISO-8859-1')
                    except UnicodeDecodeError:
                        print('{f}_{q} is not ISO-8859-5 encoding'.format(f=zfile.filename, q=qtr))
                    else:
                        print('{f} for {q} read to df with ISO-8859-1 encoding - elapsed time: {t} seconds'.format(f=zfile.filename, q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))
                else:
                    print('{f} for {q} read to df with utf-8 encoding - elapsed time: {t} seconds'.format(f=zfile.filename, q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))

                finally:
                    # Delete the temporary file
                    os.remove(project_path+'\\tag.txt')

        # Extract to individual dataframes and unsure columns align to database
        # table structure.  Add column (sec_qtr) indicating the zip file the data originates from.
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

        # TO DO - ADD STEP HERE TSTING FOR UNIQENESS OF RECRODS OVER PRIMARY KEY

        # Clear table contents (this is redundent if 'to_sql' specifies replace)
        conn.execute(sub_stage.delete())
        conn.execute(tag_stage.delete())
        conn.execute(num_stage.delete())

        # Insert into postgres database
        print('Start staging table insert for {q} - elapsed time: {t} seconds'.format(q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))

        if db_write_method == 'pandas':
            sub.to_sql(name='sub_stage', con=conn, schema='edgar', 
                        index=False, if_exists='append', method='multi', chunksize=50000)
            tag.to_sql(name='tag_stage', con=conn, schema='edgar', 
                        index=False, if_exists='append', method='multi', chunksize=50000)
            num.to_sql(name='num_stage', con=conn, schema='edgar', 
                        index=False, if_exists='append', method='multi', chunksize=50000)
        
        elif db_write_method == 'stringio':
            copy_from_stringio(conn=conn, df=sub, table='edgar.sub_stage')
            copy_from_stringio(conn=conn, df=tag, table='edgar.tag_stage')
            copy_from_stringio(conn=conn, df=num, table='edgar.num_stage')
        
        elif db_write_method == 'pg_copy':
            # https://realpython.com/prevent-python-sql-injection/
            # https://www.psycopg.org/docs/sql.html
            os.makedirs(project_path+'\\temp', exist_ok=True)
            
            # Replace NaN in order to convert to integer
            sub.fillna({
                'cik':9999, 'sic':9999, 'changed':19800101, 'wksi':0, 'fye':9999,
                'period':19800101, 'fy':9999, 'filed':19800101, 'prevrpt':0,
                'detail':0, 'nciks':9999
                }, inplace=True) 
            
            tag.fillna({'custom':'int', 'abstract':'int'}, inplace=True) 

            num.fillna({'ddate':'int', 'qtrs':'int'}, inplace=True)        

            # Convert types to ensure csv can be loaded to db
            sub = sub.astype({
                'cik':'int', 'sic':'int', 'changed':'int', 'wksi':'int', 'fye':'int',
                'period':'int', 'fy':'int', 'filed':'int', 'prevrpt':'int',
                'detail':'int', 'nciks':'int'
                }) 
            
            tag = tag.astype({'custom':'int', 'abstract':'int'}) 

            num = num.astype({'ddate':'int', 'qtrs':'int'}) 

            # Save to csv in temp folder
            sub.to_csv(path_or_buf=project_path+'\\temp\\sub.csv', index=False)
            tag.to_csv(path_or_buf=project_path+'\\temp\\tag.csv', index=False)
            num.to_csv(path_or_buf=project_path+'\\temp\\num.csv', index=False)

            # For sub.csv to load, the following columns need to be changed from integer to decimal (and then cast to integer in the final table)
            # - sic, changed, fye, period (sic, zipba, filed)
            conn.execution_options(autocommit=True).execute(text("copy edgar.sub_stage from '"+project_path+"\\temp\\sub.csv' delimiter ',' csv header"))
            conn.execution_options(autocommit=True).execute(text("copy edgar.tag_stage from '"+project_path+"\\temp\\tag.csv' delimiter ',' csv header"))
            conn.execution_options(autocommit=True).execute(text("copy edgar.num_stage from '"+project_path+"\\temp\\num.csv' delimiter ',' csv header"))
            
            shutil.rmtree(project_path+'\\temp')

        print('Complete staging table insert for {q} - elapsed time: {t} seconds'.format(q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))


        # Push to bad data and "final" tables.  TO DO - THIS IS NOT REQUIRED IF UNIQUENESS IS CONFIRMED, WRITE DIRECTLY TO THESE TABLES
        print('Start final table insert for {q} - elapsed time: {t} seconds'.format(q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))
        sql_file = open(project_path+"\\edgar_push_stage_final.sql")
        text_sql = text(sql_file.read())
        conn.execute(text_sql)
        print('Complete final table insert for {q} - elapsed time: {t} seconds'.format(q=qtr, t=int((dt.datetime.now() - tik).total_seconds())))

        # Clean up
        conn.execution_options(autocommit=True).execute(text("truncate edgar.sub_stage"))
        conn.execution_options(autocommit=True).execute(text("truncate edgar.tag_stage"))
        conn.execution_options(autocommit=True).execute(text("truncate edgar.num_stage"))


    # TO DO - PUT THIS INTO SEPARATE WORKFLOW ITEM?
    # Populate 'edgar.edgar_fndmntl_all_tb'
    conn.execution_options(autocommit=True).execute(text("""
        insert into edgar.edgar_fndmntl_all_tb 
        with t1 as (
            select distinct sec_qtr from edgar.sub except 
            select distinct sec_qtr from edgar.edgar_fndmntl_all_tb
            )
        select * from edgar.edgar_fndmntl_all_vw 
        where sec_qtr in (select sec_qtr from t1)
        """))


    # Close zip
    zf.close()


    # Save log file
    log = pd.DataFrame.from_dict(zf_info_dict, orient='index', columns=['line_count'])
    log.to_csv(project_path+'\\log.csv')


    # Close connection
    conn.close()


print('Total execution time:', int((dt.datetime.now() - tik).total_seconds()), 'seconds')