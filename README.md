# Stock Master

The "Stock Master" database collates fundamental and price data for US stocks.  

Data is collected from:
* the Securities and Exchange Commission ("SEC"), via the Financial Statement Data Sets made available by the [Economic and Risk Analysis Office](https://www.sec.gov/dera/data/financial-statement-data-sets.html),
* the [Alpha Vantage](https://www.alphavantage.co/) API, and
* the [IEX cLOD](https://iexcloud.io/) API

This repo contains Python, R and SQL scripts for interacting with the PostgreSQL database housing this data.

## Update procedures

### Monthly

1. Import S&P 500 index data with ```update_sp500_prices.py```
2. Import individual stock price data with ```update_iex_prices.py```
3. Refresh split and dividend adjusted historical prices with ```adj_price_insert.sql```
4. Derive price data technical indicators and insert into ```access_layer.return_attibutes``` table with the R script ```return_attibutes.R```
5. Import SEC fundamental data with ```edgar_import.py```
6. Transform the raw SEC data with the ```edgar.edgar_fndmntl_all_vw``` view, inserting the results into ```edgar.edgar_fndmntl_all_tb``` table
5. Derive financial ratios and valuation metrics from the fundamental data and insert into the  ```access_layer.fndmntl_attibutes``` table with the R script ```fndmntl_attibutes.R```

## Airflow  
[Airflow](https://airflow.apache.org/) is being used as an orchestrator to retrieve and process data as laid out above.  Airflow is Linux only and hence has been installed on Windows Subsystem for Linux ("WSL").  The PostgreSQL database, R and Python are installed on Windows.  Thus Airflow needs to traverse WSL and Windows, it does this calling Windows batch files via the Airflow Bash Operator.  Batch files in turn call the individual R and Python scripts operating on the data.  Airflow artifacts (except for the Python DAG file) reside in the ```airflow``` folder.

Calling jobs with a two step process via batch files and in turn via bash commands is not the standard use case for Airflow.  This is quiet a long "chain of command" and passing parameters can be tricky.  However one benefit of this approach is ability to invoke scripts in specific environments.  Each batch file will utilise one of the following:  

1. ```conda activate STOCK_MASTER && python```
2. ```psql -U postgres```
3. ```C:\Program Files\R\R-4.1.0\bin\Rscript.exe```

Depending on whether Python, PostgeSQL or R is being used.


### New calendar year

In addition to monthly data collection, the "universe" of stocks for which analysis is performed requires updating.  The universe of stocks is determined with reference to fundamental data as of Q3 of the preceding year.  The following steps are required prior to updating data in a new calendar year:
* Update the ```reference.fundamental_universe``` table with data derived from the function ```edgar.edgar_fndmntl_fltr_fn``` function, parameterised as appropriate.  

## Date reporting conventions

The date at which data is available for modeling often differs to the release date, and in the case of financial statement information, the fiscal period to which it relates.  This necessitates careful tracking of a number of different dates.  Date reporting conventions are as follows.  

##### Report date
The ending date of the fiscal period to which a piece of financial information relates.  Applies only to financial statement information. Labelled ```report_date```.
    
##### Publish date  
The date on which financial statement information is made available to the public.  Labelled ```publish_date```. 
  
##### Capture date
The date data was downloaded.  Labelled ```capture_date```.

##### Date stamp
The date at which information is available for analysis. Labelled ```date_stamp```. 

An example of date reporting for financial statement information taken from the SEC website.   IBM filed it's 30 June 2020 quarterly results on 28 July 2020 (note that it reported EPS on 20 July per the AlphaVantage earnings data).  This information was made available by the SEC in its 2020 Q3 data set on 1 October 2020 and was downloaded on 5 October 2020.  In this case the reporting dates will look like this.

| Date reference        | Date       |
| --------------------- |:----------:|
| ```report_date```     | 2020-06-30 |
| ```publish_date```    | 2020-07-28 |
| ```capture_date```    | 2020-10-05 |
| ```date_stamp```      | 2020-10-31 |

The months subsequent to October 2020, and preceding the capture date of the next quarters financial statements, will have identical reporting, publish and capture dates.  

## Attribute names    
The table below shows attributes codes and descriptions.

| Attribute code        | Attribute description         |
| --------------------- |:------------------------------|
| symbol | the ticker symbol identifying the company |
| date_stamp | date_stamp |
| close | the closing price on the last day of the month |
| adjusted_close | the adjusted closing price on the last day of the month |
| volume | trading volume |
| rtn_log_1m | 1 month logarithic return |
| amihud_1m | "amihud" illiquidity measure - 1 month average |
| amihud_60d | "amihud" illiquidity measure - 3 month average |
| amihud_vol_60d | volatility of the daily amihud illiquidity measure |
| vol_ari_20d | annualised 1 month volatility of 1 day arithmetic returns |
| vol_ari_60d | annualised 3 month volatility of 1 day arithmetic returns |
| vol_ari_120d | annualised 3 month volatility of 1 day arithmetic returns |
| skew_ari_120d | skewness of 1 day arithmetic returns calculated over 6 months |
| kurt_ari_120d | kurtosis of 1 day arithmetic returns calculated over 6 months |
| smax_20d | average of the five highest daily returns over the trailing month divided by the trailing 20 day daily return volatility |
| cor_rtn_1d_mkt_120d | the correlation of the daily returns between the stock and the S&P500 index over trailing 6 months |
| beta_rtn_1d_mkt_120d | the slope of the regression line between the stocks daily returns and the S&P500 index daily returns over trailing 6 months |
| rtn_ari_1m | 1 month arithmetic returns |
| rtn_ari_3m | 3 month arithmetic returns |
| rtn_ari_6m | 6 month arithmetic returns |
| rtn_ari_12m | 12 month arithmetic returns |
| sector | the industry sector to which the stock belongs |
| suv | standardised unexpected volume |
| ipc | intra-portfolio correlation |


Each of these attributes are discretised into deciles.  These deciles have ```_dcl``` appended.  Some attributes have also been assigned to deciles by industry group.  These are appended with ```_sctr_dcl```.  The first decile is the lowest value of the attribute in question.  Ordering has not been aligned to the consensus view of the attribute / factor effect. 


## Functions

The following is an inventory of functions used in maintaining the database.

```pg_connect``` Conveniance function to connect to database.  
```get_edgar_fnl_stmt``` Extract financial statememt data from from SEC website.  Under development - to be adapted from script edgar_import.py.  
```get_alphavantage``` Grab price or EPS data from Alpha Vantage.  Return to pandas dataframe.  
```copy_from_stringio``` Save dataframe in memory and use copy_from() to copy it to database table.  Alternate to pandas to_sql function.
```update_av_data``` Write data retrieved with ```get_alphavantage``` to database.  
```update_sp500_yf``` Grab S&P 500 index price data using the yfinance library and write to database.  
```get_sp500_cnstnt_wiki``` Grab S&P 500 constituent list from Wikipedia. 
```get_sp500_cnstnt_dlt_wiki``` Grab S&P 500 constituent list change from Wikipedia.  
```get_sp1000_cnstnt_wiki``` Grab S&P 1000 constituent list from Wikipedia. 
```get_russell1000_cnstnt_wiki``` Grab Russell 1000 constituent list from Wikipedia. 



## Development

| Description | Status |
|:----------|:-------|
| Earnings data update script, ```alphavantage_import.py```, to reference last request time via the  ```capture_date``` field in order to filter out prior requests not returning data. This will entail adding ```capture_date``` to the ```alpha_vantage.tickers_to_update``` view and updating logic at line 117. | TO DO |
| Load edgar ```pre.txt``` file in order to accurately capture balance sheet and income statement line items using the ```stmt``` field | TO DO |
| Append the pre 2012 "universe" to the ```alpha_vantage.monthly_fwd_rtn``` table.  These are stocks that are not returned in the SEC edgar data, and are ranked by dollar volume for inclusion. | TO DO |
| Get industry / sector data for stocks pre 2012 that are not returned in the SEC edgar data. | TO DO |
| Size ranking uses a combination of total equity and total assets by financial and non-financial stocks.  Winsorise minimum of total equity at 5% of total assets.  This will negate the impact of negative equity stocks, when the negative equity is driven by buybacks.  See ticker MCD for example. | TO DO |


## Random ideas
1. Price return modeling
    1. Survival analysis assessing risk of drawdown - [link](https://lib.bsu.edu/beneficencepress/mathexchange/06-01/jiayi.pdf), and [link](https://github.com/daynebatten/keras-wtte-rnn)
2. Dimensionality reduction of returns at portfolio level
    1. PCA
    2. Autoencoders?
3. Historical earnings dates for PEAD calculation - [link](https://www.alphavantage.co/documentation/#earnings)
4. Default risk as return predictor
    1. Merton for Dummies: A Flexible Way of Modelling Default Risk - [link](https://econpapers.repec.org/paper/utsrpaper/112.htm)
    2. In Search of Distress Risk - [link](https://scholar.harvard.edu/files/campbell/files/campbellhilscherszilagyi_jf2008.pdf)
5. [LPPLS](https://github.com/Boulder-Investment-Technologies/lppls), see also - [link](https://youtu.be/6x4-GcIFDlM)


## Useful development links
1. [PostgreSQL Data Dictionary Query Toolbox](https://dataedo.com/kb/query/postgresql)
2. Create data dictionary with [COMMENT ON](https://www.postgresql.org/docs/current/sql-comment.html) 
