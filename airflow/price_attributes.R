
# ------------------------------------------------------------------------------------------------------------------------
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
# 4. Confirm dollar volume for amihud is correct (volume * close vs adjusted close?) 
# 5. Error capture for amihud calculation if volume is zero or return nil
# 6. Add upside and downside versions of correlation, beta and volatility
#
# ------------------------------------------------------------------------------------------------------------------------

start_time <- Sys.time()

# Libraries
library(jsonlite)
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
library(jsonlite)



# Parameters ---------------------------------------------------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)

#database <- 'stock_master'
#update_to_date <- '2022-12-31'
database <- args[1]        
update_to_date <- args[2]  
deciles = FALSE
write_to_db = TRUE
disconnect = TRUE 
delete_existing = FALSE





# Database connection ------------------------------------------------------------------------------------------------------

config <- jsonlite::read_json('C:/Users/brent/Documents/VS_Code/postgres/postgres/config.json')

con <- dbConnect(
  RPostgres::Postgres(),
  host      = 'localhost',
  port      = '5432',
  dbname    = database,
  user      = 'postgres',
  password  = config$pg_password
)





# Check database connection and price data retrieval -----------------------------------------------------------------------
  
# Check database connection exists
if (!exists('con')) {
  stop('No database connection')
}

# Maximum date of data in the database
sql_max_date <- "select max(date_stamp) from access_layer.return_attributes"
qry_max_date <- dbSendQuery(conn = con, statement = sql_max_date) 
start_date_df <- dbFetch(qry_max_date)
start_date <- as.Date(start_date_df[[1]])

# Valid year parameter for extract function
valid_year_param <- as.integer(year(as.Date(update_to_date)))


# Read price data
sql1 <- "select * from access_layer.daily_price_ts_fn(valid_year_param_ => ?valid_year_param, nonfin_cutoff_ => 900, fin_cutoff_ => 100)"
sql1 <- sqlInterpolate(conn = con, sql = sql1, valid_year_param = valid_year_param)
qry1 <- dbSendQuery(conn = con, statement = sql1) 
df_raw <- dbFetch(qry1)
colnames(df_raw) <- sub("_$","",colnames(df_raw)) # Remove underscores


# TO DO - check that data has been returned
  
  
# Extract valid trading days from S&P500 series
sql2 <- "select * from access_layer.daily_sp500_ts_vw where extract(year from date_stamp) = ?valid_year_param"
sql2 <- sqlInterpolate(conn = con, sql = sql2, valid_year_param = valid_year_param)
qry2 <- dbSendQuery(conn = con, statement = sql2) 
valid_year_trade_days <- dbFetch(qry2)

  
# Check data for duplicates
dupe_test <- df_raw %>% select(date_stamp, symbol, close) %>%
  group_by(date_stamp, symbol) %>% 
  summarise(records = n()) %>% 
  filter(records > 1) 


# TO DO - remove duplicates

#if(nrow(dupe_test) > 0) {
#  stop(paste0('Duplicate records found for tickers: ', unique(unlist(dupe_test[c("symbol")]))))
#}
  
  

# Custom functions ------------------------------------------------------------------------------


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
    select(-date_stamp)
  ipc[ipc == 0] <- NA
  ipc <- as.data.frame(ipc)
  na_ind <- which(is.na(ipc), arr.ind = TRUE)
  ipc[na_ind] <- rowMeans(ipc, na.rm = TRUE)[na_ind[,1]]
  ipc <- cor(ipc, use = 'pairwise.complete.obs') %>%
    mean_mtrx()
  return(tibble(date_stamp = max_date, ipc = ipc))
}

# Intra Portfolio Correlation by group
ipc_by_grp <- function(df) { 
  df %>% 
    split(.$sector) %>%
    map_dfr(., ipc, .id = 'sector')
}
  

    
# Daily stock data ---------------------------------------------------------------------------------------------------------


# Find stocks without a full current (most recent) month of trading
# - find the last trade date for each month based on S&P500 data
valid_trade_days_pm <- valid_year_trade_days %>% 
  group_by(month = floor_date(date_stamp, "month")) %>% 
  summarise(n_valid = n(), max_date_valid = max(date_stamp))

# - stocks that did not trade on the last day of the most recent month
month_trade_days_excptn <- select(df_raw, date_stamp, symbol) %>% 
  group_by(symbol, month = floor_date(date_stamp, "month")) %>% 
  summarise(n_actual = n(), max_date_actual = max(date_stamp)) %>% 
  left_join(valid_trade_days_pm, by = 'month') %>% 
  filter(
    year(month) == year(as.Date(update_to_date)),
    max_date_valid > max_date_actual
  ) %>% 
  mutate(label = 'month_trade_days_excptn', year = valid_year_param) %>% 
  select(label, year, symbol, month, max_date_actual, n_actual) %>% 
  rename(min_date = month, max_date = max_date_actual, n = n_actual)


# Reference df - stocks with invalid sector / industry data
invalid_sctr <- df_raw %>% 
  filter(sector == '13') %>% 
  group_by(symbol) %>% 
  summarise(min_date = min(date_stamp), max_date = max(date_stamp), n = n()) %>% 
  mutate(label = 'invalid_sctr', year = valid_year_param) %>% 
  select(label, year, symbol, min_date, max_date, n)


# Reference df - stock with not enough trading days to satisfy attribute look-back calculation
daily_less_500 <- df_raw %>% 
  group_by(symbol) %>% 
  filter(n() <= 500) %>% 
  summarise(min_date = min(date_stamp), max_date = max(date_stamp), n = n()) %>% 
  mutate(label = 'daily_less_500', year = valid_year_param) %>% 
  select(label, year, symbol, min_date, max_date, n)


# Remove stocks without a full current (most recent) month of trading (see df "month_trade_days_excptn")
df_raw$floor_date <- floor_date(df_raw$date_stamp, "month")

daily <- df_raw %>% 
  anti_join(
    x = df_raw,
    y = month_trade_days_excptn, 
    by = c("symbol" = "symbol", "floor_date" = "min_date")
  )
  
  
daily <- daily %>% 
  select(-valid_year_ind) %>% 
  group_by(symbol) %>% 
  # Filter out symbols that do not have enough records (see df "daily_less_500") , filter S&P500 
  # and filter stocks with invalid (13) sector / industry data 
  filter(
    n() > 500, 
    symbol != 'GSPC',
    sector != '13',
  ) %>% 
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
  # Add stock sector & industry column (discarded with group by) via join to df_raw 
  left_join(
    group_by(df_raw, symbol) %>% 
      summarise(
        sector   = mean(as.numeric(sector)),
        industry = mean(as.numeric(industry))
        ), 
    by = 'symbol'
  )




# Calculate & join SUV and IPC ------------------------------------------------------------------


# THIS IS PRODUCING AN ERROR, DUPLICATES FOR CERTAIN STOCKS IN 2019, EG. MBFI, USG
# SHORT TIME FIX VIA GROUP BY AT END OF CODE BLOCK

# Derive Standardised Unexplained Volume
suv_df <- daily %>% 
  ungroup() %>% 
  arrange(date_stamp) %>% 
  # Drop potential NA's entering the regression
  drop_na(any_of(c('volume', 'pos', 'neg'))) %>% 
  slide_period_dfr(
    .x = .,
    .i = .$date_stamp,
    .period = "month",
    .f = suv_by_grp,
    .before = 11,
    .complete = TRUE
  ) %>% 
  mutate(date_stamp = ceiling_date(date_stamp, unit = "month") - 1) %>% 
  group_by(symbol, date_stamp) %>% 
  summarise(suv = round(mean(suv),4))

# Derive sector Intra Portfolio Correlation (TO DO - should there be some error capture to ensure enough data across groups)
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
    suv_120d_dcl             = ntile(suv, 10), 
    ipc_120d_dcl             = ntile(ipc, 10), 
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
if (deciles) {
  monthly <- inner_join(monthly1, monthly2, by = c('date_stamp', 'symbol'))
  monthly <- inner_join(monthly, monthly3, by = c('date_stamp', 'symbol'))
} else {
  monthly <- monthly1
}


# Filter for date range required for update
monthly <- filter(monthly, date_stamp > as.Date(start_date) & date_stamp <= as.Date(update_to_date))


# Check nulls - number of stock months with NA's
monthly_na_records <- monthly %>% 
  filter_all(any_vars(is.na(.))) %>% 
  group_by(symbol) %>% 
  summarise(min_date = min(date_stamp), max_date = max(date_stamp), n = n()) %>% 
  mutate(label = 'monthly_na_records', year = valid_year_param) %>% 
  select(label, year, symbol, min_date, max_date, n)


# Convert to data frame for upload to database (drop NA's except fwd return)
monthly <- monthly %>% drop_na() %>% as.data.frame(monthly)



# Write to postgres database ---------------------------------------------------------------------------------------------

if (write_to_db) {
  
  # Clear existing data
  if (delete_existing) {
    sql3 <- "delete from access_layer.return_attributes where extract(year from date_stamp) = ?valid_year_param"
    sql3 <- sqlInterpolate(conn = con, sql = sql3, valid_year_param = valid_year_param)
  }

  # Write to db
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
  dbClearResult(qry1)
  dbClearResult(qry2)
  dbClearResult(qry_max_date)
  dbDisconnect(con)
}


end_time <- Sys.time()

execution_time <- end_time - start_time

output_list <- list(
  'upload_data' = monthly, 
  'execution_time' = execution_time, 
  'missing_data' = bind_rows(daily_less_500, month_trade_days_excptn, monthly_na_records, invalid_sctr)
  )

saveRDS(output_list, file = 'C:/Users/brent/Documents/VS_Code/postgres/postgres/price_attributes.rda')