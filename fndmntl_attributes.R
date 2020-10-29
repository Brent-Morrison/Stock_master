#==============================================================================
#
# Script to extract data from STOCK_MASTER database and enrich fundamental data
# data with various attributes
#
#==============================================================================

library(slider)
library(dplyr)
library(tidyr)
library(forcats)
library(ggplot2)
library(ggridges)
library(lubridate)
library(DBI)
library(RPostgres)

# Connect to postgres database
con <- dbConnect(
  RPostgres::Postgres(),
  host      = 'localhost',
  port      = '5432',
  dbname    = 'stock_master',
  user      = rstudioapi::askForPassword("User"),
  password  = rstudioapi::askForPassword("Password")
  )

# Parameter and query string
date_param <- '2017-01-01'
sql1 <- "select * from edgar.qrtly_fndmntl_ts_vw where date_available >= ?date_param"
sql1 <- sqlInterpolate(conn = con, sql = sql1, date_param = date_param)

sql2 <- "select * from alpha_vantage.monthly_price_ts_vw where date_stamp >= ?date_param"
sql2 <- sqlInterpolate(conn = con, sql = sql2, date_param = date_param)

# Read data
qry1 <- dbSendQuery(
  conn = con, 
  statement = sql1
  ) 
qrtly_fndmntl_ts_raw <- dbFetch(qry1)

qry2 <- dbSendQuery(
  conn = con, 
  statement = sql2
) 
monthly_price_ts_raw <- dbFetch(qry2)


# Impute missing values
qrtly_fndmntl_ts <- qrtly_fndmntl_ts_raw %>% 
  group_by(sector) %>% 
  mutate(
    cash_ratio = mean(cash_equiv_st_invest / total_assets, na.rm = TRUE),
    cash_equiv_st_invest = if_else(is.na(cash_equiv_st_invest) | cash_equiv_st_invest == 0, total_assets * cash_ratio, cash_equiv_st_invest)
    ) %>% 
  ungroup()

# Attributes requiring lagged quarterly data
qrtly_fndmntl_ts <- qrtly_fndmntl_ts %>% 
  group_by(ticker) %>% 
  mutate(
    asset_growth  = (total_assets-lag(total_assets))/lag(total_assets),
    roa           = slide_dbl(net_income_qtly, sum, .before = 3, .complete = TRUE) / -slide_dbl(total_assets, mean, .before = 4, .complete = TRUE),
    roe           = slide_dbl(net_income_qtly, sum, .before = 3, .complete = TRUE) / slide_dbl(total_equity, mean, .before = 4, .complete = TRUE)
  ) %>% 
  ungroup()


# Expand to monthly, join price data

# Complete date sequence
date_range <- 
  ceiling_date(
    seq(min(floor_date(qrtly_fndmntl_ts$date_available, unit = 'month')), 
        max(floor_date(qrtly_fndmntl_ts$date_available, unit = 'month')), 
        by = "month"),
    unit = 'month') - 1

# Expand date and join price
qrtly_fndmntl_ts <- qrtly_fndmntl_ts %>% 
  #filter(ticker %in% c('A','AA')) %>% 
  group_by(ticker) %>% 
  complete(date_available = date_range, ticker) %>% 
  fill(sector:last_col()) %>% 
  filter(!is.na(sector)) %>% 
  ungroup()

#xx1 <- qrtly_fndmntl_ts %>% filter(ticker %in% c('A','AA'))

# Add month end date for join
monthly_price_ts_raw$date_available = ceiling_date(monthly_price_ts_raw$date_stamp, unit = 'month') - 1

qrtly_fndmntl_ts <- 
  left_join(
    x = qrtly_fndmntl_ts, 
    y = monthly_price_ts_raw, 
    by = c('date_available' = 'date_available', 'ticker' = 'symbol')
    ) %>% 
  mutate(
    mkt_cap     = round(shares_cso * close, 3),   # THIS TO BE UPDATED FOR RULES RE CSO / ESCO
    book_price  = round(-total_equity / mkt_cap, 3)
    )







# Disconnect
dbDisconnect(con)