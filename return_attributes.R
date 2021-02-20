
#==============================================================================
#
# Script to extract stock price data from postgres database, enrich with
# various "technical indicator" attributes and write to database.
#
# TO DO
# 1. Add Standardised Unexplained Volume per https://www.biz.uiowa.edu/faculty/jgarfinkel/pubs/divop_JAR.pdf (s.3.1.2). 
#    Efficacy per https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212934
# 2. Change set-up so script takes year as a parameter and outputs results for that year,
#    ensure burn-in data is collected
# 3. Function dropna() at last step will remove all forward return values and hence the last month of data. 
#    Require functionality to latest months records from DB (those with NA fwd returns) and re-insert
# 4. Add deciles for SUV
# 5. Confirm dollar volume for amihud is correct (volume * close vs adjusted close?) 
#
#==============================================================================

start_time <- Sys.time()

# Parameters
end_date <- '2019-12-31'          # Latest date of available data (character string for SQL)
months_to_load = 12               # Months prior to start date to load to DB
month_to_delete = 0               # Months to delete from DB in order to remove NA's as a result of fwd returns (0 = last months data only, 9 to not run function)
write_to_db <- FALSE              # Boolean - write results to database
disconnect <- FALSE               # Disconnect from DB


# Packages

# https://statsandr.com/blog/an-efficient-way-to-install-and-load-r-packages/

# RollingWindow installation
# https://github.com/andrewuhl/RollingWindow
# library("devtools")
# install_github("andrewuhl/RollingWindow")

library(RollingWindow)
library(slider)
library(dplyr)
library(tidyr)
library(purrr)
library(forcats)
library(ggplot2)
library(lubridate)
library(DBI)
library(RPostgres)





#==============================================================================
#
# Database connection and price data retrieval
#
#==============================================================================

# Connect to postgres database
if (!exists('con')) {
  con <- dbConnect(
    RPostgres::Postgres(),
    host      = 'localhost',
    port      = '5432',
    dbname    = 'stock_master',
    user      = rstudioapi::askForPassword("User"),
    password  = rstudioapi::askForPassword("Password")
    )
}


# Start date as string for query
start_date <- paste(
  year(as.Date(end_date) - years(2)), 
  sprintf('%02d', month(as.Date(end_date) %m-% months(3))), 
  '01', 
  sep = '-')


# Read data
sql1 <- "select * from alpha_vantage.daily_price_ts_vw where date_stamp >= ?start_date and date_stamp <= ?end_date"
sql1 <- sqlInterpolate(conn = con, sql = sql1, start_date = start_date, end_date = end_date)
qry1 <- dbSendQuery(conn = con, statement = sql1) 
df_raw <- dbFetch(qry1)

# Workflow to delete months with forward returns as NA's
# if (month_to_delete != 9) {
#   sql2 <- "select max(date_stamp) as max_date from access_layer.return_attributes"
#   sql2 <- sqlInterpolate(conn = con, sql = sql2)
#   qry2 <- dbSendQuery(conn = con, statement = sql2) 
#   max_db_date <- dbFetch(qry2)
#   max_db_date <- as.Date(max_db_date[1,1])
#   first_delete_month <- max_db_date %m-% months(month_to_delete)
#   # As string
#   first_delete_month <- paste(
#     year(as.Date(first_delete_month)), 
#     sprintf('%02d', month(as.Date(first_delete_month))), 
#     day(as.Date(first_delete_month)), 
#     sep = '-')
#   
#   sql3 <- "delete from access_layer.return_attributes where date_stamp >= ?first_delete_month"
#   sql3 <- sqlInterpolate(conn = con, sql = sql3, first_delete_month = first_delete_month)
#   qry3 <- dbSendQuery(conn = con, statement = sql3) 
# }




#==============================================================================
#
# Custom functions
#
#==============================================================================

# Standardised Unexplained Volume
suv <- function(df) { 
  max_date = max(df$date_stamp)
  mdl <- summary(lm(volume ~ pos + neg, data = df))
  res <- mdl$residuals / mdl$sigma
  suv <- mean(res[(max(length(res),21)-21):length(res)])
  return(tibble(date_stamp = max_date, suv = suv))
}

# SUV by group
suv_by_grp <- function(df) { 
  df %>% 
    split(.$symbol) %>%
    map_dfr(., suv, .id = 'symbol')
}

# Mean of matrix
mean_mtrx <- function(x) {
  mean(x[upper.tri(x)])
}

# Intra Portfolio Correlation function
ipc <- function(df) {
  max_date <- max(df$date_stamp)
  ipc <- df %>%
    select(date_stamp, symbol, rtn_log_1d) %>%
    pivot_wider(names_from = symbol, values_from = rtn_log_1d) %>% 
    select(-date_stamp) %>% 
    cor(use = 'pairwise.complete.obs') %>%
    mean_mtrx()
  return(tibble(date_stamp = max_date, ipc = ipc))
}

# Intra Portfolio Correlation by group
ipc_by_grp <- function(df) { 
  df %>% 
    split(.$sector) %>%
    map_dfr(., ipc, .id = 'sector')
}





#==============================================================================
#
# Daily stock data 
#
#==============================================================================

daily <- df_raw %>% 
  group_by(symbol) %>% 
  # Filter out symbols that do not have enough records
  filter(n() > 500) %>% 
  mutate(
    sector                  = as.numeric(sector)
    ,rtn_ari_1d             = (adjusted_close-lag(adjusted_close))/lag(adjusted_close)
    ,rtn_ari_1d_mkt         = (sp500 - lag(sp500))/lag(sp500)
    ,rtn_log_1d             = log(adjusted_close) - lag(log(adjusted_close))
    ,vol_ari_20d            = RollingStd(rtn_ari_1d, window = 20, na_method = 'window') * sqrt(252)
    ,vol_ari_60d            = RollingStd(rtn_ari_1d, window = 60, na_method = 'window') * sqrt(252)
    ,vol_ari_120d           = RollingStd(rtn_ari_1d, window = 120, na_method = 'window') * sqrt(252)
    ,skew_ari_120d          = RollingSkew(rtn_ari_1d, window = 120, na_method = 'window')
    ,kurt_ari_120d          = RollingKurt(rtn_ari_1d, window = 120, na_method = 'window')
    ,amihud                 = abs(rtn_ari_1d) / (volume * adjusted_close / 10e7)
    ,amihud_60d             = RollingMean(amihud, window = 60, na_method = 'window')
    ,amihud_vol_60d         = RollingStd(amihud, window = 60, na_method = 'window') * sqrt(252)
    ,smax_20d               = slide_dbl(rtn_ari_1d, ~mean(tail(sort(.x), 5)), .before = 20) / vol_ari_20d
    ,cor_rtn_1d_mkt_120d    = RollingCorr(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'window')
    ,beta_rtn_1d_mkt_120d   = RollingBeta(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'window')
    ,pos                    = if_else(rtn_ari_1d >= 0, abs(rtn_ari_1d), 0) # for SUV calc.
    ,neg                    = if_else(rtn_ari_1d < 0, abs(rtn_ari_1d), 0)  # for SUV calc.
  ) %>% 
  ungroup()


# Aggregate to monthly
monthly1 <- daily %>% 
  group_by(symbol, date_stamp = floor_date(date_stamp, "month")) %>% 
  mutate(date_stamp = ceiling_date(date_stamp, unit = "month") - 1) %>% 
  summarise(
    close                   = last(close),
    adjusted_close          = last(adjusted_close),
    volume                  = mean(volume),
    rtn_log_1m              = sum(rtn_log_1d),
    amihud_1m               = mean(amihud),
    amihud_60d              = last(amihud_60d),
    amihud_vol_60d          = last(amihud_vol_60d),
    vol_ari_20d             = last(vol_ari_20d),
    vol_ari_60d             = last(vol_ari_60d),
    vol_ari_120d            = last(vol_ari_120d),
    skew_ari_120d           = last(skew_ari_120d),
    kurt_ari_120d           = last(kurt_ari_120d),
    smax_20d                = last(smax_20d),
    cor_rtn_1d_mkt_120d     = last(cor_rtn_1d_mkt_120d),
    beta_rtn_1d_mkt_120d    = last(beta_rtn_1d_mkt_120d)
    ) %>% 
  ungroup() %>% 
  group_by(symbol) %>% 
  mutate(
    rtn_ari_1m              = (adjusted_close-lag(adjusted_close))/lag(adjusted_close),
    rtn_ari_3m              = (adjusted_close-lag(adjusted_close, 3))/lag(adjusted_close, 3),
    rtn_ari_6m              = (adjusted_close-lag(adjusted_close, 6))/lag(adjusted_close, 6),
    rtn_ari_12m             = (adjusted_close-lag(adjusted_close, 12))/lag(adjusted_close, 12)
    ) %>% 
  ungroup() %>% 
  # Add stock sector column (discarded with group by) via join to df_raw 
  left_join(
    group_by(df_raw, symbol) %>% summarise(sector = mean(as.numeric(sector))), 
    by = 'symbol'
    )





#==============================================================================
#
# Calculate & join SUV and IPC
#
#==============================================================================


# Derive Standardised Unexplained Volume
suv_df <- daily %>% 
  ungroup() %>% 
  arrange(date_stamp) %>% 
  slide_period_dfr(
    .x =  .,
    .i = .$date_stamp,
    .period = "month",
    .f = suv_by_grp,
    .before = 5,
    .complete = TRUE
  ) %>% 
  mutate(date_stamp = ceiling_date(date_stamp, unit = "month") - 1)

# Derive sector Intra Portfolio Correlation
ipc_df <- daily %>% 
  ungroup() %>% 
  arrange(date_stamp) %>% 
  slide_period_dfr(
    .x =  .,
    .i = .$date_stamp,
    .period = "month",
    .f = ipc_by_grp,
    .before = 5,
    .complete = TRUE
  ) %>% 
  mutate(
    date_stamp = ceiling_date(date_stamp, unit = "month") - 1,
    sector = as.numeric(sector))

# Join
monthly1 <- inner_join(monthly1, suv_df, by = c('date_stamp', 'symbol'))
monthly1 <- left_join(monthly1, ipc_df, by = c('date_stamp', 'sector'))


# Market wide deciles
monthly2 <- monthly1 %>% 
  group_by(date_stamp) %>% 
  mutate(
    rtn_ari_1m_dcl           = ntile(rtn_ari_1m, 10),
    rtn_ari_3m_dcl           = ntile(rtn_ari_3m, 10),
    rtn_ari_6m_dcl           = ntile(rtn_ari_6m, 10),
    rtn_ari_12m_dcl          = ntile(rtn_ari_12m, 10),
    amihud_1m_dcl            = ntile(amihud_1m, 10),
    amihud_60d_dcl           = ntile(amihud_60d, 10),
    amihud_vol_60d_dcl       = ntile(amihud_vol_60d, 10),
    vol_ari_20d_dcl          = ntile(vol_ari_20d, 10),
    vol_ari_60d_dcl          = ntile(vol_ari_60d, 10),
    vol_ari_120d_dcl         = ntile(vol_ari_120d, 10),
    skew_ari_120d_dcl        = ntile(skew_ari_120d, 10),
    kurt_ari_120d_dcl        = ntile(kurt_ari_120d, 10), 
    smax_20d_dcl             = ntile(smax_20d, 10),
    cor_rtn_1d_mkt_120d_dcl  = ntile(cor_rtn_1d_mkt_120d, 10),    
    beta_rtn_1d_mkt_120d_dcl = ntile(beta_rtn_1d_mkt_120d, 10),
    suv_120d_dcl             = ntile(suv, 10), ###
    ipc_120d_dcl             = ntile(ipc, 10), ###
  ) %>% 
  ungroup() %>% 
  select(symbol, date_stamp, rtn_ari_1m_dcl:ipc_120d_dcl)

  
# Sector deciles
monthly3 <- monthly1 %>% 
  group_by(date_stamp, sector) %>% 
  mutate(
    rtn_ari_1m_sctr_dcl           = ntile(rtn_ari_1m, 10),
    rtn_ari_3m_sctr_dcl           = ntile(rtn_ari_3m, 10),
    rtn_ari_6m_sctr_dcl           = ntile(rtn_ari_6m, 10),
    rtn_ari_12m_sctr_dcl          = ntile(rtn_ari_12m, 10),
    amihud_1m_sctr_dcl            = ntile(amihud_1m, 10),
    amihud_60d_sctr_dcl           = ntile(amihud_60d, 10),
    amihud_vol_60d_sctr_dcl       = ntile(amihud_vol_60d, 10),
    vol_ari_20d_sctr_dcl          = ntile(vol_ari_20d, 10),
    vol_ari_60d_sctr_dcl          = ntile(vol_ari_60d, 10),
    vol_ari_120d_sctr_dcl         = ntile(vol_ari_120d, 10),
    skew_ari_120d_sctr_dcl        = ntile(skew_ari_120d, 10),
    kurt_ari_120d_sctr_dcl        = ntile(kurt_ari_120d, 10), 
    smax_20d_sctr_dcl             = ntile(smax_20d, 10),
    cor_rtn_1d_mkt_120d_sctr_dcl  = ntile(cor_rtn_1d_mkt_120d, 10),    
    beta_rtn_1d_mkt_120d_sctr_dcl = ntile(beta_rtn_1d_mkt_120d, 10),
    suv_120d_sctr_dcl             = ntile(suv, 10),
  ) %>% 
  ungroup() %>% 
  select(symbol, date_stamp, rtn_ari_1m_sctr_dcl:suv_120d_sctr_dcl)


# Join dataframes
monthly <- inner_join(monthly1, monthly2, by = c('date_stamp', 'symbol'))
monthly <- inner_join(monthly, monthly3, by = c('date_stamp', 'symbol'))


# Filter for date range required for update
monthly <- filter(monthly, date_stamp > as.Date(end_date) %m-% months(months_to_load))


# Check nulls
# Number of stock months with NA's
na_count <- monthly %>% filter_all(any_vars(is.na(.))) %>% tally()


# Convert to data frame for upload to database (drop NA's except fwd return)
#comp_case_param <- match('fwd_rtn_1m', names(monthly1))
#monthly <- monthly %>% filter(complete.cases(.[,-comp_case_param])) %>% as.data.frame(monthly)
monthly <- monthly %>% drop_na() %>% as.data.frame(monthly)







#==============================================================================
#
# Write to postgres database
#
#==============================================================================

# Filter & select prior to DB load
#monthly <- monthly %>% 
#  select()



if (write_to_db) {
  dbWriteTable(
  conn = con, 
  name = SQL('access_layer.return_attributes'), 
  value = monthly, 
  row.names = FALSE, 
  append = TRUE
  )
}


# Disconnect
if (disconnect) {
  dbDisconnect(con)
}


end_time <- Sys.time()

execution_time <- end_time - start_time







#====================================================================================================================
#====================================================================================================================
#====================================================================================================================

price_attributes(
  end_date = '2019-12-31', 
  months_to_load = 12, 
  write_to_db = TRUE, 
  disconnect = FALSE, 
  return_df = TRUE
  )

price_attributes <- function(end_date, months_to_load = 12, write_to_db = TRUE, disconnect = FALSE, return_df = TRUE) {
  
  #==============================================================================
  #
  # Script to extract stock price data from postgres database, enrich with
  # various "technical indicator" attributes and write to database.
  #
  # TO DO
  # 1. Add Standardised Unexplained Volume per https://www.biz.uiowa.edu/faculty/jgarfinkel/pubs/divop_JAR.pdf (s.3.1.2). 
  #    Efficacy per https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212934
  # 2. Change set-up so script takes year as a parameter and outputs results for that year,
  #    ensure burn-in data is collected
  # 3. Function dropna() at last step will remove all forward return values and hence the last month of data. 
  #    Require functionality to latest months records from DB (those with NA fwd returns) and re-insert
  # 4. Add deciles for SUV
  # 5. Confirm dollar volume for amihud is correct (volume * close vs adjusted close?) 
  #
  #==============================================================================
  
  start_time <- Sys.time()
  
  # Parameters
  #end_date <- '2020-12-31'          # Latest date of available data (character string for SQL)
  #months_to_load = 12               # Months prior to start date to load to DB
  #write_to_db <- TRUE              # Boolean - write results to database
  #disconnect <- FALSE               # Disconnect from DB
  
  
  # Packages
  
  # https://statsandr.com/blog/an-efficient-way-to-install-and-load-r-packages/
  
  # RollingWindow installation
  # https://github.com/andrewuhl/RollingWindow
  # library("devtools")
  # install_github("andrewuhl/RollingWindow")
  
  #library(RollingWindow)
  #library(slider)
  #library(dplyr)
  #library(tidyr)
  #library(purrr)
  #library(forcats)
  #library(ggplot2)
  #library(lubridate)
  #library(DBI)
  #library(RPostgres)

  
  
  #==============================================================================
  #
  # Database connection and price data retrieval
  #
  #==============================================================================
  
  # Connect to postgres database
  if (!exists('con')) {
    con <- dbConnect(
      RPostgres::Postgres(),
      host      = 'localhost',
      port      = '5432',
      dbname    = 'stock_master',
      user      = rstudioapi::askForPassword("User"),
      password  = rstudioapi::askForPassword("Password")
    )
  }
  
  
  # Start date as string for query
  start_date <- paste(
    year(as.Date(end_date) - years(2)), 
    sprintf('%02d', month(as.Date(end_date) %m-% months(3))), 
    '01', 
    sep = '-')
  
  
  # Read data
  sql1 <- "select * from alpha_vantage.daily_price_ts_vw where date_stamp >= ?start_date and date_stamp <= ?end_date"
  sql1 <- sqlInterpolate(conn = con, sql = sql1, start_date = start_date, end_date = end_date)
  qry1 <- dbSendQuery(conn = con, statement = sql1) 
  df_raw <- dbFetch(qry1)
  
  # Check data for duplicates
  dupe_test <- df_raw %>% select(date_stamp, symbol, close) %>%
    group_by(date_stamp, symbol) %>% 
    summarise(records = n()) %>% 
    filter(records > 1) 
  
  if(nrow(dupe_test) > 0) {
    stop(paste0('Duplicate records found for tickers: ', unique(unlist(dupe_test[c("symbol")]))))
  }
  
  
  #==============================================================================
  #
  # Custom functions
  #
  #==============================================================================
  
  # Standardised Unexplained Volume
  suv <- function(df) { 
    max_date = max(df$date_stamp)
    mdl <- summary(lm(volume ~ pos + neg, data = df))
    res <- mdl$residuals / mdl$sigma
    suv <- mean(res[(max(length(res),21)-21):length(res)])
    return(tibble(date_stamp = max_date, suv = suv))
  }
  
  # SUV by group
  suv_by_grp <- function(df) { 
    df %>% 
      split(.$symbol) %>%
      map_dfr(., suv, .id = 'symbol')
  }
  
  # Mean of matrix
  mean_mtrx <- function(x) {
    mean(x[upper.tri(x)])
  }
  
  # Intra Portfolio Correlation function
  ipc <- function(df) {
    max_date <- max(df$date_stamp)
    ipc <- df %>%
      select(date_stamp, symbol, rtn_log_1d) %>%
      pivot_wider(names_from = symbol, values_from = rtn_log_1d) %>% 
      select(-date_stamp) %>% 
      cor(use = 'pairwise.complete.obs') %>%
      mean_mtrx()
    return(tibble(date_stamp = max_date, ipc = ipc))
  }
  
  # Intra Portfolio Correlation by group
  ipc_by_grp <- function(df) { 
    df %>% 
      split(.$sector) %>%
      map_dfr(., ipc, .id = 'sector')
  }
  

    
  #==============================================================================
  #
  # Daily stock data 
  #
  #==============================================================================
  
  daily <- df_raw %>% 
    group_by(symbol) %>% 
    # Filter out symbols that do not have enough records
    filter(n() > 500) %>% 
    mutate(
      sector                  = as.numeric(sector)
      ,rtn_ari_1d             = (adjusted_close-lag(adjusted_close))/lag(adjusted_close)
      ,rtn_ari_1d_mkt         = (sp500 - lag(sp500))/lag(sp500)
      ,rtn_log_1d             = log(adjusted_close) - lag(log(adjusted_close))
      ,vol_ari_20d            = RollingStd(rtn_ari_1d, window = 20, na_method = 'window') * sqrt(252)
      ,vol_ari_60d            = RollingStd(rtn_ari_1d, window = 60, na_method = 'window') * sqrt(252)
      ,vol_ari_120d           = RollingStd(rtn_ari_1d, window = 120, na_method = 'window') * sqrt(252)
      ,skew_ari_120d          = RollingSkew(rtn_ari_1d, window = 120, na_method = 'window')
      ,kurt_ari_120d          = RollingKurt(rtn_ari_1d, window = 120, na_method = 'window')
      ,amihud                 = abs(rtn_ari_1d) / (volume * adjusted_close / 10e7)
      ,amihud_60d             = RollingMean(amihud, window = 60, na_method = 'window')
      ,amihud_vol_60d         = RollingStd(amihud, window = 60, na_method = 'window') * sqrt(252)
      ,smax_20d               = slide_dbl(rtn_ari_1d, ~mean(tail(sort(.x), 5)), .before = 20) / vol_ari_20d
      ,cor_rtn_1d_mkt_120d    = RollingCorr(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'window')
      ,beta_rtn_1d_mkt_120d   = RollingBeta(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'window')
      ,pos                    = if_else(rtn_ari_1d >= 0, abs(rtn_ari_1d), 0) # for SUV calc.
      ,neg                    = if_else(rtn_ari_1d < 0, abs(rtn_ari_1d), 0)  # for SUV calc.
    ) %>% 
    ungroup()
  
  
  # Aggregate to monthly
  monthly1 <- daily %>% 
    group_by(symbol, date_stamp = floor_date(date_stamp, "month")) %>% 
    mutate(date_stamp = ceiling_date(date_stamp, unit = "month") - 1) %>% 
    summarise(
      close                   = last(close),
      adjusted_close          = last(adjusted_close),
      volume                  = mean(volume),
      rtn_log_1m              = sum(rtn_log_1d),
      amihud_1m               = mean(amihud),
      amihud_60d              = last(amihud_60d),
      amihud_vol_60d          = last(amihud_vol_60d),
      vol_ari_20d             = last(vol_ari_20d),
      vol_ari_60d             = last(vol_ari_60d),
      vol_ari_120d            = last(vol_ari_120d),
      skew_ari_120d           = last(skew_ari_120d),
      kurt_ari_120d           = last(kurt_ari_120d),
      smax_20d                = last(smax_20d),
      cor_rtn_1d_mkt_120d     = last(cor_rtn_1d_mkt_120d),
      beta_rtn_1d_mkt_120d    = last(beta_rtn_1d_mkt_120d)
    ) %>% 
    ungroup() %>% 
    group_by(symbol) %>% 
    mutate(
      rtn_ari_1m              = (adjusted_close-lag(adjusted_close))/lag(adjusted_close),
      rtn_ari_3m              = (adjusted_close-lag(adjusted_close, 3))/lag(adjusted_close, 3),
      rtn_ari_6m              = (adjusted_close-lag(adjusted_close, 6))/lag(adjusted_close, 6),
      rtn_ari_12m             = (adjusted_close-lag(adjusted_close, 12))/lag(adjusted_close, 12)
    ) %>% 
    ungroup() %>% 
    # Add stock sector column (discarded with group by) via join to df_raw 
    left_join(
      group_by(df_raw, symbol) %>% summarise(sector = mean(as.numeric(sector))), 
      by = 'symbol'
    )
  
  
  
  #==============================================================================
  #
  # Calculate & join SUV and IPC
  #
  #==============================================================================
  
  # Derive Standardised Unexplained Volume
  suv_df <- daily %>% 
    ungroup() %>% 
    arrange(date_stamp) %>% 
    slide_period_dfr(
      .x =  .,
      .i = .$date_stamp,
      .period = "month",
      .f = suv_by_grp,
      .before = 5,
      .complete = TRUE
    ) %>% 
    mutate(date_stamp = ceiling_date(date_stamp, unit = "month") - 1)
  
  # Derive sector Intra Portfolio Correlation
  ipc_df <- daily %>% 
    ungroup() %>% 
    arrange(date_stamp) %>% 
    slide_period_dfr(
      .x =  .,
      .i = .$date_stamp,
      .period = "month",
      .f = ipc_by_grp,
      .before = 5,
      .complete = TRUE
    ) %>% 
    mutate(
      date_stamp = ceiling_date(date_stamp, unit = "month") - 1,
      sector = as.numeric(sector))
  
  # Join
  monthly1 <- inner_join(monthly1, suv_df, by = c('date_stamp', 'symbol'))
  monthly1 <- left_join(monthly1, ipc_df, by = c('date_stamp', 'sector'))
  
  
  # Market wide deciles
  monthly2 <- monthly1 %>% 
    group_by(date_stamp) %>% 
    mutate(
      rtn_ari_1m_dcl           = ntile(rtn_ari_1m, 10),
      rtn_ari_3m_dcl           = ntile(rtn_ari_3m, 10),
      rtn_ari_6m_dcl           = ntile(rtn_ari_6m, 10),
      rtn_ari_12m_dcl          = ntile(rtn_ari_12m, 10),
      amihud_1m_dcl            = ntile(amihud_1m, 10),
      amihud_60d_dcl           = ntile(amihud_60d, 10),
      amihud_vol_60d_dcl       = ntile(amihud_vol_60d, 10),
      vol_ari_20d_dcl          = ntile(vol_ari_20d, 10),
      vol_ari_60d_dcl          = ntile(vol_ari_60d, 10),
      vol_ari_120d_dcl         = ntile(vol_ari_120d, 10),
      skew_ari_120d_dcl        = ntile(skew_ari_120d, 10),
      kurt_ari_120d_dcl        = ntile(kurt_ari_120d, 10), 
      smax_20d_dcl             = ntile(smax_20d, 10),
      cor_rtn_1d_mkt_120d_dcl  = ntile(cor_rtn_1d_mkt_120d, 10),    
      beta_rtn_1d_mkt_120d_dcl = ntile(beta_rtn_1d_mkt_120d, 10),
      suv_120d_dcl             = ntile(suv, 10), ###
      ipc_120d_dcl             = ntile(ipc, 10), ###
    ) %>% 
    ungroup() %>% 
    select(symbol, date_stamp, rtn_ari_1m_dcl:ipc_120d_dcl)
  
  
  # Sector deciles
  monthly3 <- monthly1 %>% 
    group_by(date_stamp, sector) %>% 
    mutate(
      rtn_ari_1m_sctr_dcl           = ntile(rtn_ari_1m, 10),
      rtn_ari_3m_sctr_dcl           = ntile(rtn_ari_3m, 10),
      rtn_ari_6m_sctr_dcl           = ntile(rtn_ari_6m, 10),
      rtn_ari_12m_sctr_dcl          = ntile(rtn_ari_12m, 10),
      amihud_1m_sctr_dcl            = ntile(amihud_1m, 10),
      amihud_60d_sctr_dcl           = ntile(amihud_60d, 10),
      amihud_vol_60d_sctr_dcl       = ntile(amihud_vol_60d, 10),
      vol_ari_20d_sctr_dcl          = ntile(vol_ari_20d, 10),
      vol_ari_60d_sctr_dcl          = ntile(vol_ari_60d, 10),
      vol_ari_120d_sctr_dcl         = ntile(vol_ari_120d, 10),
      skew_ari_120d_sctr_dcl        = ntile(skew_ari_120d, 10),
      kurt_ari_120d_sctr_dcl        = ntile(kurt_ari_120d, 10), 
      smax_20d_sctr_dcl             = ntile(smax_20d, 10),
      cor_rtn_1d_mkt_120d_sctr_dcl  = ntile(cor_rtn_1d_mkt_120d, 10),    
      beta_rtn_1d_mkt_120d_sctr_dcl = ntile(beta_rtn_1d_mkt_120d, 10),
      suv_120d_sctr_dcl             = ntile(suv, 10),
    ) %>% 
    ungroup() %>% 
    select(symbol, date_stamp, rtn_ari_1m_sctr_dcl:suv_120d_sctr_dcl)
  
  
  # Join dataframes
  monthly <- inner_join(monthly1, monthly2, by = c('date_stamp', 'symbol'))
  monthly <- inner_join(monthly, monthly3, by = c('date_stamp', 'symbol'))
  
  
  # Filter for date range required for update
  monthly <- filter(monthly, date_stamp > as.Date(end_date) %m-% months(months_to_load))
  
  
  # Check nulls
  # Number of stock months with NA's
  na_count <- monthly %>% filter_all(any_vars(is.na(.))) %>% tally()
  
  
  # Convert to data frame for upload to database (drop NA's except fwd return)
  #comp_case_param <- match('fwd_rtn_1m', names(monthly1))
  #monthly <- monthly %>% filter(complete.cases(.[,-comp_case_param])) %>% as.data.frame(monthly)
  monthly <- monthly %>% drop_na() %>% as.data.frame(monthly)
  
  
  #==============================================================================
  #
  # Write to postgres database
  #
  #==============================================================================
  
  if (write_to_db) {
    dbWriteTable(
      conn = con, 
      name = SQL('access_layer.return_attributes'), 
      value = monthly, 
      row.names = FALSE, 
      append = TRUE
    )
  }
  
  
  # Disconnect
  if (disconnect) {
    dbDisconnect(con)
  }
  
  
  end_time <- Sys.time()
  
  execution_time <- end_time - start_time
  
  print(execution_time)
  
  # Return data frame
  if (return_df) {
    return(monthly)
  }
  
} 