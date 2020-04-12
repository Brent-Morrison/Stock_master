# Libraries
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime as dt
import requests
import io as io
from zipfile import ZipFile

# Get zip from SEC website

# 2020 - this format (https://www.sec.gov/files/node/add/data_distribution/2020q1.zip)
# Pre 2020 - this format (https://www.sec.gov/files/dera/data/financial-statement-data-sets/2009q3.zip)
url = 'https://www.sec.gov/files/dera/data/financial-statement-data-sets/2009q3.zip'
resp = requests.get(url)
zf = ZipFile(io.BytesIO(resp.content))
zf_files = zf.infolist()
zf_file_names = zf.namelist()

# Loop over text files in the downloaded zip file and read to individual dataframes
# Exclude the readme file
zf_files_dict = {}
for zfile in zf_files:
    if zfile.filename == 'readme.htm':
        continue     
    print(zfile.filename) 
    zf_files_dict[zfile.filename] = pd.read_csv(zf.open(zfile.filename), delimiter = '\t')

# Extract dataframes
sub = zf_files_dict['sub.txt']  # TO DO - convert 'filed' to date
tag = zf_files_dict['tag.txt']
pre = zf_files_dict['pre.txt']
num = zf_files_dict['num.txt']

# Convert date to date format
sub['period_new'] = pd.to_datetime(sub['period'], format='%Y%m%d').dt.strftime("%Y-%m-%d")

# Insert to postgres database
engine = create_engine('postgresql://postgres:*******@localhost:5432/stock_master')
df.to_sql('edgar.num_stage', engine)


# References
# https://stackoverflow.com/questions/26942476/reading-csv-zipped-files-in-python
# https://stackoverflow.com/questions/23419322/download-a-zip-file-and-extract-it-in-memory-using-python3
# https://github.com/pandas-dev/pandas/issues/14553
