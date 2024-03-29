
# --------------------------------------------------------------------------------------------------------------------------
#
# Script extracting SEC derived accounting data from the STOCK_MASTER database, 
# constructing fundamental and valuation attributes.
# 
# TO DO
# 1. Limit TS prediction to zero and 110% of the max mkt cap
# 2. Impute ROE for stocks with negative equity from ROA
# 3. Assign small positive equity value for negative equity stocks to return log-bp (5% / 10% of assets)) 
# 4. Add date of load column
#
# ISSUES
#
# Asset growth can be very large (think mergers & acquisitions).  We do not want to remove outliers as suspected spurious, rather
# winsorise to allow for better statistical properties.
#
# EQT - is a divestiture adjustment as opposed to stock split - the split impacts price only, not shares o/s
# https://www.fool.com/investing/2018/11/13/heres-how-eqt-corporation-stock-fell-46-today-but.aspx
# CRC - incorrect adjustment to shares O/s.  Implemented rule to toggle simfin source which has split adjusted shares o/s.
# RIG - incorrect shares o/s adjustment applied Oct-2020 with low  
# AA - intangible assets is inconsistent, nil in some months and positive in others
# IQV - Intangible assets overstated or nil (balance from note aggregated in error)
# GEF - reports lodged on consecutive quarters labeled as Q2 returned (no year end change) 
# AAP - Fiscal period is incorrectly labeled Q2 for two consecutive report dates (publish dates 2018-05-22 & 2018-08-14)
# PCG - share count doubles in qtr 2020-06-30, 'LiabilitiesSubjectToCompromise'? 2019-12-31
# KMB - return on equity
# MA/QCOM - shares outstanding errors, TRIGGERING DIVISION RULES. ADD FILTER TO CHECK IF PRIOR MONTH VALUE CHANGED.
#
# DUPLICATES
# Dupes are returned with the edgar.qrtly_fndmntl_ts_vw due to overlap of simfin and edgar data.  The query...
#
# select * from (select ticker, report_date, count(*) over (partition by ticker, report_date) as n 
# from edgar.qrtly_fndmntl_ts_vw) t1 where n > 1
#
# ...identifies the following dupes ('ALXN','AMC','CACI','CRL','DYN','FDS','HZNP','IKBR','OA').  These are excluded manully
#
# --------------------------------------------------------------------------------------------------------------------------

# Start
start_time <- Sys.time()


# Packages
library(slider)
library(dplyr)
library(tibble)
library(tidyr)
library(purrr)
library(broom)
library(mblm)
library(reticulate)
library(DescTools)
library(naniar)
library(moments)
library(DT)
library(forcats)
library(ggplot2)
library(ggridges)
library(lubridate)
library(DBI)
library(RPostgres)
library(jsonlite)


# External parameters
args <- commandArgs(trailingOnly = TRUE)

#database <- 'stock_master'
#update_to_date <- '2022-07-31'
database <- args[1]        
update_to_date <- args[2]  


# Database connection
config <- jsonlite::read_json('C:/Users/brent/Documents/VS_Code/postgres/postgres/config.json')

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host      = 'localhost',
  port      = '5432',
  dbname    = database,
  user      = 'postgres',
  password  = config$pg_password
)


# Parameters ---------------------------------------------------------------------------------------------------------------

reseed <- FALSE                   # Boolean - repopulate database for the full year in which the "update_to_date" resides
ts_regr <- TRUE                   # Boolean - run Multivariate Thiel Sen regression from Python
ts_iter = 50                      # Iterations for TS regression random seed starts
write_to_db <- TRUE               # Boolean - write results to database
lbmadj <- 0.01                    # Lower bound for market cap adjustment (NOT IN USE)
ubmadj <- 20                      # Upper bound for market cap adjustment (NOT IN USE)

# Parameters - last date in database
# Used to determine the "date_filter" re data to be loaded to db
# If data is up to date and a month is being re-run (ie. not a reseed), this is set to prior month end
sql <- "select max(date_stamp) as date_stamp from access_layer.fundamental_attributes"
qry <- dbSendQuery(conn = con, statement = sql)
last_date_in_db_raw <- DBI::dbFetch(qry)
last_date_in_db <- if (last_date_in_db_raw[[1]] == update_to_date) as.Date(update_to_date) %m-% months(1) else last_date_in_db_raw[[1]]
	
# Parameters - start date for data retrieval, needs to be adequate for rolling window look back. Dynamic based on reseed status.
start_date <- as.character(as.Date(update_to_date) %m-% months(if (reseed) 30 else 18))

# Parameters - year, the year in which the update_to_date resides
valid_year <- as.character(year(update_to_date))

# Parameters - range of dates to insert to db (range of month ends not in database)
start_date_load <- if (reseed) floor_date(as.Date(update_to_date), unit = 'year') - 1 else last_date_in_db

date_filter <- 
  ceiling_date(
    seq(min(floor_date(as.Date(start_date_load) + 1, unit = 'month')), 
        as.Date(update_to_date),
        by = "month"),
    unit = 'month') - 1

# Setting to prevent plots being generated
pdf(NULL)  # https://stackoverflow.com/questions/6535927/how-do-i-prevent-rplots-pdf-from-being-generated



# DATA RETRIEVAL -----------------------------------------------------------------------------------------------------------

# Read data and ensure correct ordering
sql1 <- "select * from edgar.qrtly_fndmntl_ts_vw where date_available >= ?start_date and date_available <= ?update_to_date and ticker not in ('ALXN','AMC','CACI','CRL','DYN','FDS','HZNP','IKBR','OA')"
sql1 <- DBI::sqlInterpolate(conn = con, sql = sql1, start_date = start_date, update_to_date = update_to_date)
qry1 <- DBI::dbSendQuery(conn = con, statement = sql1) 
qrtly_fndmntl_ts_raw <- DBI::dbFetch(qry1)
qrtly_fndmntl_ts_raw <- dplyr::arrange(qrtly_fndmntl_ts_raw, ticker, report_date)

# Share prices
sql2 <- "select * from access_layer.monthly_price_ts_fn(valid_year_param_ => ?valid_year, nonfin_cutoff_ => 900, fin_cutoff_ => 100)"
sql2 <- DBI::sqlInterpolate(conn = con, sql = sql2, valid_year = valid_year)
qry2 <- DBI::dbSendQuery(conn = con, statement = sql2) 
monthly_price_ts_raw <- DBI::dbFetch(qry2)
names(monthly_price_ts_raw) <- gsub("_$", "", names(monthly_price_ts_raw))

sql3 <- "select * from access_layer.splits_vw where date_stamp >= ?start_date and date_stamp <= ?update_to_date"
sql3 <- DBI::sqlInterpolate(conn = con, sql = sql3, start_date = start_date, update_to_date = update_to_date)
qry3 <- DBI::dbSendQuery(conn = con, statement = sql3) 
monthly_splits_raw <- DBI::dbFetch(qry3)




# CHECK DUPLICATES ---------------------------------------------------------------------------------------------------------

dupe_test1 <- qrtly_fndmntl_ts_raw %>%
  dplyr::group_by(ticker, report_date) %>% 
  dplyr::summarise(records = n()) %>% 
  dplyr::filter(records > 1) 

if(nrow(dupe_test1) > 0) {
  print(paste0('Duplicate records found in fundamental data for ticker: ', unique(unlist(dupe_test1[c("ticker")])),'. '))
  qrtly_fndmntl_ts_raw <- qrtly_fndmntl_ts_raw %>% dplyr::distinct(ticker, report_date, .keep_all = TRUE)
}

dupe_test2 <- monthly_price_ts_raw %>%
  dplyr::group_by(symbol, date_stamp) %>% 
  dplyr::summarise(records = n()) %>% 
  dplyr::filter(records > 1) 

if(nrow(dupe_test2) > 0) {
  print(paste0('Duplicate records found in price data for ticker: ', unique(unlist(dupe_test2[c("symbol")])),'. '))
  monthly_price_ts_raw <- monthly_price_ts_raw %>% dplyr::distinct(symbol, date_stamp, .keep_all = TRUE)
}


# CHECK MISSINGNESS --------------------------------------------------------------------------------------------------------

# Missing defined as nil or NA cash, total assets, total equity, net income,
# shares outstanding, less than 5 quarters or non-continuous data (ie., missing quarter)

# Stocks with missing data - includes indicator for missing qtr end (0 = non-consecutive qtrs)
qrtly_fndmntl_excl <- qrtly_fndmntl_ts_raw %>% 
  dplyr::group_by(ticker) %>% 
  dplyr::mutate(
    cash_test = sum(cash_equiv_st_invest, na.rm = FALSE),
    n_months = n(),
    miss_qtr_ind = dplyr::case_when(
      n_months < 5 ~ 0,
      is.na(lag(fiscal_period)) ~ 1,
      fiscal_period == 'Q1' & lag(fiscal_period) == 'Q4' ~ 1,
      fiscal_period == 'Q2' & lag(fiscal_period) == 'Q1' ~ 1,
      fiscal_period == 'Q3' & lag(fiscal_period) == 'Q2' ~ 1,
      fiscal_period == 'Q4' & lag(fiscal_period) == 'Q3' ~ 1,
      TRUE ~ 0)  # TO DO - THIS CAN LEAD TO INCORRECT EXCLUSIONS IF THE QTR END CHANGES
  ) %>% 
  dplyr::ungroup() %>% 
  dplyr::mutate(shares_os = pmax(shares_cso, shares_ecso, na.rm = TRUE)) %>% 
  naniar::replace_with_na(replace = list(
    cash_equiv_st_invest = c(0), 
    total_assets = c(0), 
    total_equity = c(0), 
    shares_os = c(0),
    miss_qtr_ind = c(0)
  ))



# Missingness plot 
if (any(is.na(qrtly_fndmntl_excl[, !(names(qrtly_fndmntl_excl) %in% c("date_available","shares_cso","shares_ecso"))]))) {
  fndmntl_vis_miss <- qrtly_fndmntl_excl %>% 
    dplyr::select(-date_available, -shares_cso, -shares_ecso) %>%
    naniar::vis_miss()
}


# Co-occurance plot 
if (any(is.na(qrtly_fndmntl_excl[, !(names(qrtly_fndmntl_excl) %in% c("shares_cso","shares_ecso"))]))) {
  fndmntl_miss_upset <- qrtly_fndmntl_excl %>% 
    select(-shares_cso, -shares_ecso) %>% 
    naniar::gg_miss_upset()
}


# Data for missingness bar plot
fnl_initial_popn <- qrtly_fndmntl_ts_raw %>% 
  select(ticker) %>% n_distinct()

fnl_shares_os <- qrtly_fndmntl_excl %>%
  select(ticker, shares_os) %>%  
  filter(is.na(shares_os)) %>% 
  select(ticker) %>% n_distinct()

fnl_miss_qtr <- qrtly_fndmntl_excl %>%
  select(ticker, shares_os, miss_qtr_ind) %>%  
  filter(!is.na(shares_os), is.na(miss_qtr_ind)) %>% 
  select(ticker) %>% n_distinct()


# List tickers with either nil or NA for cash, total assets, total equity, net income or shares outstanding,
# or non-continuous data in the year under analysis
excluded <- qrtly_fndmntl_excl %>%
  dplyr::select(
    ticker,
    report_date,
    n_months,
    cash_equiv_st_invest, 
    total_assets, 
    total_equity, 
    shares_os,
    miss_qtr_ind
  ) %>%  
  dplyr::filter_all(any_vars(is.na(.))) %>% 
  #filter(across(everything(), ~is.na(.x)))
  dplyr::mutate(
    miss_ind = dplyr::case_when(
      n_months < 5                ~ 'less_5_months',
      is.na(miss_qtr_ind)         ~ 'miss_qtr_ind',
      is.na(shares_os)            ~ 'shares_os',
      is.na(total_equity)         ~ 'total_equity',
      is.na(total_assets)         ~ 'total_assets',
      is.na(cash_equiv_st_invest) ~ 'cash_equiv_st_invest',
      TRUE ~ 'ok')
  ) 

excluded_summary <- excluded %>% group_by(miss_ind) %>% summarise(d = n_distinct(ticker))

# Exclude tickers with irreplaceable missing values and impute other missing values,
# excluded tickers are in the 'excluded' data frame, hence anti-join 
qrtly_fndmntl_ts <- qrtly_fndmntl_ts_raw %>% 
  anti_join(distinct(excluded, ticker), by = 'ticker') %>% 
  group_by(sector) %>% 
  mutate(
    cash_ratio = mean(cash_equiv_st_invest / total_assets, na.rm = TRUE),
    cash_equiv_st_invest = if_else(is.na(cash_equiv_st_invest) | cash_equiv_st_invest == 0, total_assets * cash_ratio, cash_equiv_st_invest)
  ) %>% 
  ungroup()


# Count of distinct list of excluded stocks
fnl_miss_qtr_chk <- qrtly_fndmntl_ts %>% 
  select(ticker) %>% 
  n_distinct()



# Create attributes requiring lagged quarterly data ------------------------------------------------------------------------

# Analysis to be performed prior to expanding to monthly time series
# Ref. for ttm earning ex worst quarter 'ttm_earnings_max' - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3443289

qrtly_fndmntl_ts <- qrtly_fndmntl_ts %>% 
  group_by(ticker) %>% 
  mutate(
    ttm_earnings     = -slide_dbl(net_income_qtly, sum, .before = 3, .complete = TRUE),
    ttm_earnings_max = -slide_dbl(net_income_qtly, ~sum(.[. != max(.)]), .before = 3, .complete = TRUE) / 3 * 4,
    total_equity_cln = if_else(total_equity >= 0, -total_assets * 0.1, total_equity),
    asset_growth     = (total_assets-lag(total_assets))/lag(total_assets),
    roa              = ttm_earnings / slide_dbl(total_assets, mean, .before = 4, .complete = TRUE),
    roe              = ttm_earnings / slide_dbl(total_equity_cln, mean, .before = 4, .complete = TRUE),
    leverage         = -total_liab / total_assets,
    cash_ratio       = cash_equiv_st_invest / total_assets,
    other_ca_ratio   = (total_cur_assets - cash_equiv_st_invest) / total_assets,
    intang_ratio     = intang_asset / total_assets,
    # TO DO - STANDARDISED UNEXPECTED EARNINGS TO GO HERE
    sue              = 1
    # SUE denotes Standardized Unexpected Earnings, and is calculated as the change in quarterly earnings 
    # divided by from its value four quarters ago divided by the standard deviation of this change in
    # quarterly earnings over the prior eight quarters
  ) %>% 
  ungroup()



 
# Expand to monthly, join price data ---------------------------------------------------------------------------------------

# Generate date sequence
date_range <- 
  ceiling_date(
    seq(min(floor_date(qrtly_fndmntl_ts$date_available, unit = 'month')), 
        as.Date(update_to_date),
        by = "month"),
    unit = 'month') - 1


# Expand dates to monthly periodicity and fill forward
monthly_fndmntl_ts <- qrtly_fndmntl_ts %>% 
  group_by(ticker) %>% 
  complete(date_available = date_range, ticker) %>% 
  fill(valid_year:last_col()) %>% 
  filter(!is.na(sector)) %>% 
  ungroup()


# Add month end date to price data for join
monthly_price_ts_raw$date_available <- ceiling_date(monthly_price_ts_raw$date_stamp, unit = 'month') - 1


# Summarise missing price data pre inner join
missing_price <- left_join(
  x = select(monthly_fndmntl_ts, date_available, ticker, sector),
  y = select(monthly_price_ts_raw, symbol, date_stamp, close, adjusted_close, volume, date_available), 
  by = c('date_available' = 'date_available', 'ticker' = 'symbol')
  ) %>% 
  mutate(na_flag = if_else(is.na(adjusted_close), TRUE, FALSE)) %>% 
  group_by(ticker, sector, na_flag) %>% 
  summarise(min_date = min(date_available), max_date = max(date_available))

# TO DO - Compare length to that for bar chart
#length(unique(missing_price$ticker[missing_price$na_flag == TRUE]))

# Join price and split data
monthly_fndmntl_ts <- 
  inner_join(
    x = monthly_fndmntl_ts, 
    y = select(monthly_price_ts_raw, symbol, date_stamp, close, adjusted_close, volume, date_available), 
    by = c('date_available' = 'date_available', 'ticker' = 'symbol')
    ) %>% 
  left_join(
    y = monthly_splits_raw, 
    by = c('date_available' = 'me_date', 'ticker' = 'symbol')
  ) %>% 
  # Clean shares outstanding data, inferring adjustment factor based on resultant book value / mkt cap ratio
  # If the adjustment results in a book value / mkt cap ratio between 'lbmadj' and 'ubmadj', use that adjustment
  mutate(
    shares_cso_fact = case_when(
      -total_equity / (.000001 * shares_cso * close) > lbmadj & -total_equity / (.000001 * shares_cso * close) < ubmadj ~ .000001,
      -total_equity / (.001    * shares_cso * close) > lbmadj & -total_equity / (.001    * shares_cso * close) < ubmadj ~ .001,
      -total_equity / (1       * shares_cso * close) > lbmadj & -total_equity / (1       * shares_cso * close) < ubmadj ~ 1,
      -total_equity / (1000    * shares_cso * close) > lbmadj & -total_equity / (1000    * shares_cso * close) < ubmadj ~ 1000,
      -total_equity / (1000000 * shares_cso * close) > lbmadj & -total_equity / (1000000 * shares_cso * close) < ubmadj ~ 1000000,
      TRUE ~ 0
      ),
    shares_ecso_fact = case_when(
      -total_equity / (.000001 * shares_ecso * close) > lbmadj & -total_equity / (.000001 * shares_ecso * close) < ubmadj ~ .000001,
      -total_equity / (.001    * shares_ecso * close) > lbmadj & -total_equity / (.001    * shares_ecso * close) < ubmadj ~ .001,
      -total_equity / (1       * shares_ecso * close) > lbmadj & -total_equity / (1       * shares_ecso * close) < ubmadj ~ 1,
      -total_equity / (1000    * shares_ecso * close) > lbmadj & -total_equity / (1000    * shares_ecso * close) < ubmadj ~ 1000,
      -total_equity / (1000000 * shares_ecso * close) > lbmadj & -total_equity / (1000000 * shares_ecso * close) < ubmadj ~ 1000000,
      TRUE ~ 0
      ),
    shares_os_unadj = round(pmax(shares_ecso, shares_cso), 2)
    #shares_os_unadj = round(
    #  case_when(
    #    shares_cso_fact == 1 ~ shares_ecso,  
    #    shares_ecso_fact == 1 ~ shares_cso,
    #    shares_cso_fact != 0 ~ shares_ecso * shares_ecso_fact,
    #    shares_ecso_fact != 0 ~ shares_cso * shares_cso_fact,
    #    TRUE ~ pmax(shares_ecso, shares_cso)
    #  ), 2)
    ) %>% 
  group_by(ticker) %>% 
  # TO DO - DOCUMENT THE LOGIC BELOW
  # Account for splits - required for mkt cap calc. pre SEC data reporting of new share count
  mutate(
    split_adj_start  = if_else(!is.na(split_coef), 1, 0),
    split_adj_end1   = if_else(cumsum(replace_na(split_adj_start, 0)) > 0 & 
                                 abs((shares_os_unadj / lag(shares_os_unadj)) / cumsum(replace_na(split_coef, 0)) - 1) < 0.1, 1, 0),
    split_date       = if_else(!is.na(split_coef), date_available, as.Date('1980-01-01')),
    split_adj_end    = case_when(
      cumsum(replace_na(split_adj_start, 0)) > 0 & abs((shares_os_unadj / lag(shares_os_unadj)) / cumsum(replace_na(split_coef, 0)) - 1) < 0.1 ~ 1, 
      publish_date > max(split_date) ~ 1,
      TRUE ~ 0),
    split_adj_ind    = if_else(cumsum(replace_na(split_adj_end, 0)) > 0, 0, cumsum(replace_na(split_adj_start, 0))),
    split_adj        = if_else(split_adj_ind == 1, cumsum(replace_na(split_coef, 0)), 1),
    shares_os        = if_else(src == 'simfin', shares_os_unadj, shares_os_unadj * split_adj),
    mkt_cap          = if_else(src == 'simfin', round(shares_os * adjusted_close, 3), round(shares_os * close, 3)),
    book_price       = round(-total_equity_cln / mkt_cap, 3)
    ) %>% 
  ungroup() %>% 
  mutate(
    ttm_earn_yld      = round(ttm_earnings / mkt_cap, 3),
    ttm_earn_yld_max  = round(ttm_earnings_max / mkt_cap, 3)
    ) %>% 
  select(-date_stamp.x, -date_stamp.y) %>% 
  rename(date_stamp = date_available) 


# Log count of valid tickers
fnl_no_price_data <- monthly_fndmntl_ts %>% select(ticker) %>% n_distinct()


# Replace Inf with NA and remove and remove any row with NA
monthly_fndmntl_ts[monthly_fndmntl_ts == -Inf] <- NA
monthly_fndmntl_ts[monthly_fndmntl_ts == Inf] <- NA

monthly_fndmntl_ts <- monthly_fndmntl_ts %>%
  select(-split_coef) %>% 
  filter(across(.cols = everything(), .fns = ~ !is.na(.x)))


# Log count of final set of valid tickers
fnl_remainder <- monthly_fndmntl_ts %>% 
  select(ticker) %>% 
  n_distinct()





# PB-ROE and other valuation models ----------------------------------------------------------------------------------------
# 
# Robust / Theil Sen regression references 
# https://www.jamesuanhoro.com/post/2017/09/21/theil-sen-regression-in-r/
# https://cran.r-project.org/web/packages/mblm/mblm.pdf
# https://education.wayne.edu/eer_dissertations/ahmad_farooqi_dissertation.pdf
# http://extremelearning.com.au/the-siegel-and-theil-sen-non-parametric-estimators-for-linear-regression/
#
# --------------------------------------------------------------------------------------------------------------------------

# Function to extract r-squared
get_rsq <- function(x) glance(x)$r.squared


# Model
monthly_fndmntl_ts <- monthly_fndmntl_ts %>% 
  group_by(date_stamp, sector) %>% 
  
  # Remove groups with 1 stock, prevent TS regression error
  filter(n() > 1) %>% 
  
  # Impute mean value per group - https://cran.r-project.org/web/packages/broom/vignettes/broom_and_dplyr.html
  # TO DO 2 - Impute ROE for stocks with negative equity from ROA
  # TO DO 3 - Assign small positive equity for negative equity stocks to return log(bp)
  mutate(
    roe           = if_else(is.na(roe), mean(roe, na.rm = TRUE), roe),
    roe           = Winsorize(roe, minval = -1, maxval = 1, na.rm = TRUE),
    book_price    = if_else(is.na(book_price), mean(book_price, na.rm = TRUE), book_price),
    book_price    = if_else(book_price <= 0, 0.01, book_price),
    log_pb        = log(1/book_price),
    log_pb        = Winsorize(log_pb, probs = c(0.01, 0.99), na.rm = TRUE)
    ) %>% 
  
  # Nest for regression
  nest() %>% 
  mutate(
    pbroe_rsdl_ols = map(data, ~residuals(lm(log_pb ~ roe, data = .x))),
    fit_ols = map(data, ~lm(log_pb ~ roe, data = .x)),
    pbroe_rsq_ols = map_dbl(fit_ols, get_rsq),
    pbroe_rsdl_ts = map(data, ~residuals(mblm(log_pb ~ roe, data = .x, repeated = TRUE))),
    ) %>% 
  unnest(cols = c(data, pbroe_rsdl_ols, pbroe_rsq_ols, pbroe_rsdl_ts)) %>% 
  select(-fit_ols) %>%
  ungroup()



# Valuation measures requiring Python functions (multivariate Thiel Sen) ---------------------------------------------------
if (ts_regr) {
  
  # Reticulate for multivariate Thiel Sen regression
  use_condaenv(condaenv = 'STOCK_MASTER', required = TRUE)
  source_python('C:/Users/brent/Documents/VS_Code/postgres/postgres/python/ts_regression.py')
  
  
  # Regression inputs - Non-financial data
  tsreg_data <- monthly_fndmntl_ts %>% 
    filter(date_stamp %in% date_filter, sector != 5) %>% 
    mutate(
      total_cur_liab_abs        = -total_cur_liab,
      lt_debt_abs               = -lt_debt,
      non_cash_cur_assets       = total_cur_assets - cash_equiv_st_invest,
      other_noncur_liab_abs     = -(total_noncur_liab - lt_debt),
      total_equity_abs          = -total_equity_cln,
      ttm_earnings_abs          = -ttm_earnings
    ) %>% 
    select(
      date_stamp, sector, ticker, mkt_cap,
      cash_equiv_st_invest, non_cash_cur_assets, total_noncur_assets, 
      total_cur_liab_abs, lt_debt_abs, other_noncur_liab_abs, total_equity_abs, ttm_earnings_abs
    )
  
  
  # Regression inputs - Financial data (note the different attributes used for model)
  tsreg_data_fin <- monthly_fndmntl_ts %>% 
    filter(date_stamp %in% date_filter, sector == 5) %>% 
    mutate(
      total_cur_liab_abs        = -total_cur_liab,
      lt_debt_abs               = -lt_debt,
      total_liab                = -total_liab,
      total_equity_abs          = -total_equity_cln,
      ttm_earnings_abs          = -ttm_earnings
    ) %>% 
    select(
      date_stamp, sector, ticker, mkt_cap,
      total_assets, total_liab, total_equity_abs, ttm_earnings_abs
    )
  
  
  # Apply model
  # Excluding the '.id' argument to map_dfr results in the date_stamp being returned from python/pandas as a list.  This is not usable in the data frame.
  # Including the '.id' argument as below requires converting this back to date per the 'mutate' below
  #
  # TO DO 1: Limit TS prediction to zero and 110% of the max mkt cap
  
  tsreg_pred <- tsreg_data %>% 
    split(list(.$date_stamp,.$sector)) %>% 
    map_dfr(., theil_sen_py, .id = 'date_stamp', iter = as.integer(ts_iter)) %>% 
    mutate(
      date_stamp = as.Date(substr(date_stamp, 1, 10)),
      actual = prediction + residual
    )
  
  tsreg_pred_fin <- tsreg_data_fin %>% 
    split(list(.$date_stamp,.$sector)) %>% 
    map_dfr(., theil_sen_py, .id = 'date_stamp', iter = as.integer(ts_iter)) %>% 
    mutate(
      date_stamp = as.Date(substr(date_stamp, 1, 10)),
      actual = prediction + residual
    )
  
  
  # Union financial and non-financial regression results
  tsreg_pred_all <- bind_rows(tsreg_pred, tsreg_pred_fin)
  
  
  # Join model residuals back to original data frame
  monthly_fndmntl_ts <- 
    left_join(
      x = monthly_fndmntl_ts, 
      y = select(tsreg_pred_all, date_stamp, ticker, residual), 
      by = c('date_stamp' = 'date_stamp', 'ticker' = 'ticker')
    ) %>% 
    mutate(fnmdl_rsdl_ts = round(residual / mkt_cap, 3))

} else {
  
  monthly_fndmntl_ts$residual <- 0
  monthly_fndmntl_ts$fnmdl_rsdl_ts <- 0

}



# Aggregate valuation measure ----------------------------------------------------------------------------------------------

# Per http://www.econ.yale.edu/~shiller/behfin/2013_04-10/asness-frazzini-pedersen.pdf 
# "In order to put each measure on equal footing and combine them, each month we convert each variable 
# into ranks and standardize to obtain a z-score.  The average of the individual z-scores is then computed."
# Low (cheap) values stocks will be in decile 1.

monthly_fndmntl_ts <- monthly_fndmntl_ts %>% 
  group_by(date_stamp, sector) %>% 
  mutate(
    pbroe_rsdl_ols_rnk = dense_rank(pbroe_rsdl_ols),
    pbroe_rsdl_ts_rnk  = dense_rank(pbroe_rsdl_ts),
    book_price_rnk     = dense_rank(desc(book_price)),
    ttm_earn_yld_rnk   = dense_rank(desc(ttm_earn_yld)),
    fnmdl_rsdl_ts_rnk  = dense_rank(desc(fnmdl_rsdl_ts)),
    pbroe_rsdl_ols_z   = scale(pbroe_rsdl_ols_rnk),
    pbroe_rsdl_ts_z    = scale(pbroe_rsdl_ts_rnk),
    book_price_z       = scale(book_price_rnk),
    ttm_earn_yld_z     = scale(ttm_earn_yld_rnk),
    fnmdl_rsdl_ts_z    = scale(fnmdl_rsdl_ts_rnk)
  ) %>% 
  ungroup()

monthly_fndmntl_ts$agg_valuation  = rowMeans(
  monthly_fndmntl_ts[,c('pbroe_rsdl_ols_z', 'pbroe_rsdl_ts_z', 'book_price_z', 'ttm_earn_yld_z', 'fnmdl_rsdl_ts_z')],
  na.rm = TRUE)




# Visualize missingness ----------------------------------------------------------------------------------------------------

# Dataframe for bar plot
stages <- tibble(
  initial_popn = fnl_initial_popn,
  shares_os = initial_popn - fnl_shares_os,
  missing_qtr = shares_os - fnl_miss_qtr,
  total_assets_equity = fnl_miss_qtr_chk,
  no_price_data = fnl_no_price_data,
  other = fnl_remainder
  ) %>% 
  pivot_longer(
    cols = initial_popn:other,
    names_to = 'stage', 
    values_to = 'tickers'
  ) %>% 
  mutate(stage = as_factor(stage))

# Bar plot showing stocks lost to bad / missing data
miss_summary <- ggplot(stages, aes(x = stage, y= tickers)) +
  geom_col() +
  geom_text(aes(label = tickers, vjust = -0.5), size = 3.5) +
  labs(
    title = 'Factors reducing final population of valid stocks',
    subtitle = 'Counts represent number of distinct stocks over the full look back period',
    #caption = 'xxxx'
    x = '',
    y = ''
  ) +
  theme(plot.caption = element_text(size = 8, margin = margin(t = 10), color = "grey", hjust = 0))






# Descriptive statistics presented in data table ---------------------------------------------------------------------------

# Data
data <- select(monthly_fndmntl_ts, date_stamp, cash_ratio:intang_ratio, mkt_cap:pbroe_rsdl_ts)

desc_stats_func <- function(x) {
  bind_rows(
    x %>% summarise(across(where(is.numeric), ~ n())),
    x %>% summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ sd(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ skewness(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ kurtosis(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ quantile(.x, probs = c(0,.02,.05,.1,.25,.5,.75,.9,.95,.98,1), na.rm = TRUE)))
  ) %>% round(3) %>% 
  mutate(statistic = c('count','mean','sd','skew','kurt','min','qtl02','qtl05','qtl10','q1','med','q3','qtl90','qtl95','qtl98','max'))
}

desc_stats <- data %>% split(.$date_stamp) %>% map_dfr(., desc_stats_func, .id = 'date_stamp')
  


# Write to DT for inspection
desc_stats <- datatable(
  select(desc_stats, date_stamp, statistic, cash_ratio:ttm_earn_yld_max, -ttm_earnings, -ttm_earnings_max),
  filter = 'top', 
  options = list(
    pageLength = 16
    )
  )



# Write to postgres database & disconnect ----------------------------------------------------------------------------------

# Ticker that are only present in the period not written to the db
df_to_db_all <- monthly_fndmntl_ts %>% distinct(ticker)

# Filter and select for data to insert
df_to_db <- monthly_fndmntl_ts %>% 
  select(-(shares_cso_fact:split_adj_end)) %>% 
  filter(date_stamp %in% date_filter) %>% 
  arrange(date_stamp, ticker)

excl_no_curr_date <- df_to_db_all %>% 
  anti_join(distinct(df_to_db, ticker), by = 'ticker')


if (write_to_db) {

	if (reseed) {

	  # Clear existing data
	  sql4 <- "delete from access_layer.fundamental_attributes where extract(year from date_stamp) = ?valid_year_param"
	  sql4 <- sqlInterpolate(conn = con, sql = sql4, valid_year_param = valid_year)
	  qry4 <- dbSendQuery(conn = con, statement = sql4)
	  
	}
  
  # Write to db
  dbWriteTable(
	  conn = con, 
	  name = SQL('access_layer.fundamental_attributes'), 
	  value = df_to_db, 
	  row.names = FALSE, 
	  append = TRUE
	  )
}


# Disconnect
dbDisconnect(con)


end_time <- Sys.time()

execution_time <- end_time - start_time


# Save plot objects for use in rmarkdown report
output_list <- list(
  'excluded' = excluded,
  'missing_price' = missing_price,
  'miss_summary' = miss_summary,
  'desc_stats' = desc_stats,
  'execution_time' = execution_time
)

saveRDS(output_list, file = 'C:/Users/brent/Documents/VS_Code/postgres/postgres/fndmntl_attributes.rda')