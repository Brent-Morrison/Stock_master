# Stock Master

The "Stock Master" database collates fundamental and price data for US stocks.  

Data is collected from:
* the Securities and Exchange Commission ("SEC"), via the Financial Statement Data Sets made available by the [Economic and Risk Analysis Office](https://www.sec.gov/dera/data/financial-statement-data-sets.html),
* the [Alpha Vantage](https://www.alphavantage.co/) API, and
* the [IEX Cloud](https://iexcloud.io/) API

This repo contains Python, R and SQL scripts for interacting with the PostgreSQL database housing this data.

## Update procedures

### Airflow  
[Airflow](https://airflow.apache.org/) is used as an orchestrator to retrieve and process data as laid out above.  Airflow is Linux only and hence has been installed on Windows Subsystem for Linux ("WSL").  The PostgreSQL database, R and Python are installed on Windows.  Thus Airflow needs to traverse WSL and Windows, it does this calling Windows batch files via the Airflow Bash Operator.  Batch files in turn call the individual R and Python scripts operating on the data.  Airflow artifacts (except for the Python DAG file) reside in the ```airflow``` folder.

Calling jobs with a two step process via batch files and in turn via bash commands is not the standard use case for Airflow.  This is quiet a long "chain of command" and passing parameters can be tricky.  However one benefit of this approach is ability to invoke scripts in specific environments.  Each batch file will utilise one of the following:  

1. ```conda activate STOCK_MASTER && python```
2. ```psql -U postgres```
3. ```C:\Program Files\R\R-4.1.0\bin\Rscript.exe```

depending on whether Python, PostgeSQL or R is being used.  

All scripts called by Airflow are in the [airflow](https://github.com/Brent-Morrison/Stock_master/tree/master/airflow) folder.

#### Airflow startup  
Via Ubuntu terminal:
1. ```sudo service postgresql start```
2. ```airflow db init```
3. ```airflow scheduler```  
4. ```airflow webserver``` (requires new Ubuntu window)  

### Database status  
The status of the database is queried with the script [here](https://github.com/Brent-Morrison/Stock_master/tree/master/misc/db_status.R)

### Monthly procedures - selecting stocks to retrieve   
The universe of stocks for which price data is to be retrieved is detemined with respect to stock size (assets and equity).  The diagram below shows the objects used in this assessment: 
<br>
```mermaid
    flowchart TD
        A[[fundamental_universe <sup>1</sup>]] --> C(tickers_to_update_fn <sup>3</sup>)
        B[[ticker_cik_sic_ind <sup>2</sup>]] --> C
        C --> D(update_iex_prices.py)
```

<sup>1</sup> Reference table, `reference.fundamental_universe` listing stocks by Central Index Key ("CIK") and year, detailing total assets and total book equity per stock / year.  
<sup>2</sup> Reference table, `reference.ticker_cik_sic_ind` listing stocks by CIK, ticker, name, Standard Industrial Classification ("SIC") code and delist date  
<sup>3</sup> This function returns a list of tickers with the last update date for both price and earnings data. 

The script `update_iex_prices.py` is used to retrieve price data.  The seeding of tables 1 and 2 above is detailed below.


### Monthly procedures - data retrieval  
The Airflow DAG executed to re-fresh the database is shown below.  

```mermaid
    flowchart TD
        A(update_iex_prices.py <sup>1</sup>) --> B(adj_price_insert_sc.sql <sup>2</sup>)
        B --> C(price_attributes.R <sup>4</sup>)
        D(update_sp500_prices.py <sup>3</sup>) --> C
        C --> E(update_sec_edgar.py <sup>5</sup>)
        E --> F(fndmntl_attributes.py <sup>6</sup>)
```


<sup>1</sup> Python script.  Retrieve data from the IEX server and write to the `iex.shareprices_daily` table  
<sup>2</sup> PL/pgSQL script.  Adjust prices for dividends and splits and insert into `access_layer.shareprices_daily`. See below.  
<sup>3</sup> Python script.  Retrieve index data from Yahoo Finance and write to the `iex.shareprices_daily` table.  
<sup>4</sup> R script.  Create features derived from stock prices and insert into `access_layer.return_attributes`.  
<sup>5</sup> Python script.  Retrieve data from the [SEC Edgar database](https://www.sec.gov/dera/data/financial-statement-data-sets.html) and inset into `edgar_fndmntl_all_tb`.  See below.  
<sup>6</sup> R script.  Create features derived from fundamental (and price) data, and insert into `access_layer.fundamental_attributes`.  


### Adjusting for dividends & splits  
WIP.  

Outline functionality of `adj_price_insert_sc.sql` >> `insert_adj_price` >> `adj_price_union`.  Note script is selecting stocks that are in the `iex.shareprices_daily` and not in `access_layer.shareprices_daily` (ie., not yet updated) and loop over calling `insert_adj_price`  


### New calendar year  
In addition to monthly data collection, the "universe" of stocks for which analysis is performed requires updating on an annual basis.  The universe of stocks is determined with reference to fundamental data as of Q3 of the preceding year.  The following steps are required prior to updating price data in a new calendar year:  
1. Update the table `edgar.company_tickers` with CIK and ticker data using the python function `update_sec_company_tickers`.  
2. Update the table `alpha_vantage.active_delisted` with IPO and delist date data using the python function `update_active_delisted`. 
3. Update the table `reference.ticker_cik_sic_ind` using the custom query and logic in the excel file `cik_fndmntl_univ.xlsx`.
3. Update the `reference.fundamental_universe` table with data derived from the function `edgar.edgar_fndmntl_fltr_fn` and logic in the excel file `cik_fndmntl_univ.xlsx`.  

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
