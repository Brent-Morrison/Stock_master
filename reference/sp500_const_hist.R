# Script loading S&P500 constituent history
# to be updated by ????

# Libraries
library(DBI)
library(RPostgres)
library(jsonlite)
library(dplyr)
library(tidyr)
library(readr)


# Database connection
config <- jsonlite::read_json('C:/Users/brent/Documents/VS_Code/postgres/postgres/config.json')

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host      = 'localhost',
  port      = '5432',
  dbname    = 'stock_master',
  user      = 'postgres',
  password  = config$pg_password
)



# Read data
# https://github.com/fja05680/sp500
df_raw <- read_csv("https://raw.githubusercontent.com/fja05680/sp500/master/S%26P%20500%20Historical%20Components%20%26%20Changes(12-29-2022).csv")
df <- df_raw
df['ticker_list'] <- lapply(df['tickers'], function(x) strsplit(x,","))
df <- unnest(df[c('date','ticker_list')], ticker_list) %>% 
  group_by(ticker_list) %>% 
  summarise(min_date = min(date),
            max_date = max(date)) %>% 
  ungroup() %>% 
  mutate(capture_date = as.Date('2022-12-31')) %>% #as.Date(Sys.Date())
  rename(ticker = ticker_list)

cur_date <- max(df$max_date)
df['max_date'][df['max_date'] == as.character(cur_date)] <- as.Date('9998-12-31')

# Write to db
dbWriteTable(
  conn = con, 
  name = SQL('reference.sp500_cons'), 
  value = df,
  append = TRUE
)
