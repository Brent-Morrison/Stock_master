# Stock Master

The "Stock Master" database collates fundamental and price data for US stocks.  

Data is collected from:
* the Securities and Exchange Commission ("SEC"), via the Financial Statement Data Sets made available by the [Economic and Risk Analysis Office](https://www.sec.gov/dera/data/financial-statement-data-sets.html), and
* the [Alpha Vantage](https://www.alphavantage.co/) API

This repo contains Python, R and SQL scripts for interacting with the PostgreSQL database.

## Update procedures

1. Import price data with ```alphavantage_import.py```
2. Import fundamental data with ```edgar_import.py```
3. Transform the raw SEC data with the ```edgar.edgar_fndmntl_all_vw``` view, inserting the results into ```edgar.edgar_fndmntl_all_tb``` table
4. Derive technical indicators from the price data and insert into  ```access_layer.return_attibutes``` table with the R script ```return_attibutes.R```
5. Derive financial ratios and valuation metrics from the fundamental data and insert into the  ```access_layer.fndmntl_attibutes``` table with the R script ```fndmntl_attibutes.R```

### New calendar year
The "universe" of stocks for which analysis is performed is determined with fundamental data as of Q3 of the preceding year.  Therefore additional steps are required prior to updating data in a new calendar year:
* Update the ```reference.fundamental_universe``` table with data derived from the function ```edgar.edgar_fndmntl_fltr_fn``` function, parameterised as appropriate.  

## Development / random ideas
1. Price return modeling
2. Dimensionality reduction of returns at portfolio level
    1. PCA
    2. Autoencoders?
