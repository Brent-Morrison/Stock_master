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




# Disconnect
dbDisconnect(con)