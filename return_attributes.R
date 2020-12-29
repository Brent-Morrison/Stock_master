
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
#
#==============================================================================


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
library(ggridges)
library(lubridate)
library(DBI)
library(RPostgres)





#==============================================================================
# Database connection and price data retrieval
#==============================================================================

# Connect to postgres database
con <- dbConnect(
  RPostgres::Postgres(),
  host      = 'localhost',
  port      = '5432',
  dbname    = 'stock_master',
  user      = rstudioapi::askForPassword("User"),
  password  = rstudioapi::askForPassword("Password")
)


# Parameters and query string
end_date <- '2020-10-31'
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





#==============================================================================
# Custom functions
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
# Daily stock data 
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
    ,vol_ari_120d           = RollingStd(rtn_ari_1d, window = 120, na_method = 'window')
    ,skew_ari_120d          = RollingSkew(rtn_ari_1d, window = 120, na_method = 'window')
    ,kurt_ari_120d          = RollingKurt(rtn_ari_1d, window = 120, na_method = 'window')
    ,amihud                 = abs(rtn_ari_1d) / (volume * adjusted_close / 10e7)
    ,amihud_60d             = RollingMean(amihud, window = 60, na_method = 'window')
    ,amihud_vol_60d         = RollingStd(amihud, window = 60, na_method = 'window') * sqrt(252)
    ,smax_20d               = slide_dbl(rtn_ari_1d, ~mean(tail(sort(.x), 5)), .before = 20) / vol_ari_20d
    ,cor_rtn_1d_mkt_120d    = RollingCorr(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'window')
    ,beta_rtn_1d_mkt_120d   = RollingBeta(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'window')
    ,pos                    = if_else(rtn_ari_1d >= 0, abs(rtn_ari_1d), 0)
    ,neg                    = if_else(rtn_ari_1d < 0, abs(rtn_ari_1d), 0)
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
    rtn_ari_12m             = (adjusted_close-lag(adjusted_close, 12))/lag(adjusted_close, 12),
    fwd_rtn_1m              = lead(rtn_log_1m)
    ) %>% 
  ungroup() %>% 
  # Add stock sector column (discarded with group by) via join to df_raw 
  left_join(group_by(df_raw, symbol) %>% summarise(sector = mean(as.numeric(sector))), by = 'symbol')


# Calculate & join SUV and IPC

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
    beta_rtn_1d_mkt_120d_dcl = ntile(beta_rtn_1d_mkt_120d, 10)  
  ) %>% 
  ungroup() %>% 
  select(symbol, date_stamp, rtn_ari_1m_dcl:beta_rtn_1d_mkt_120d_dcl)

  
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
    smax_20d_vdcl                 = ntile(smax_20d, 10),
    cor_rtn_1d_mkt_120d_sctr_dcl  = ntile(cor_rtn_1d_mkt_120d, 10),    
    beta_rtn_1d_mkt_120d_sctr_dcl = ntile(beta_rtn_1d_mkt_120d, 10)  
  ) %>% 
  ungroup() %>% 
  select(symbol, date_stamp, rtn_ari_1m_sctr_dcl:beta_rtn_1d_mkt_120d_sctr_dcl)


# Join dataframes
monthly <- inner_join(monthly1, monthly2, by = c('date_stamp', 'symbol'))
monthly <- inner_join(monthly, monthly3, by = c('date_stamp', 'symbol'))


# Filter for date range required for update
monthly <- monthly %>% 
  filter(
    year(date_stamp) == year(end_date)#, 
    #month(date_stamp) == month(max(date_stamp))
  )


# Check nulls
# Number of stock months with NA's
xxx %>% select(-fwd_rtn_1m) %>% filter_all(any_vars(is.na(.))) %>% tally()


# Convert to data frame for upload to database
monthly <- monthly %>% drop_na() %>% as.data.frame(monthly)


# Write to postgres database
dbWriteTable(
  conn = con, 
  name = SQL('access_layer.return_attributes'), 
  value = monthly, 
  row.names = FALSE, 
  append = TRUE
  )


# Disconnect
dbDisconnect(con)







#### TEST ####
dfm_smax <- dfm %>% drop_na() %>% 
  group_by(rtn_ari_12m_dcl) %>% 
  summarise(avg = mean(rtn_ari_12m), fwd_rtn_1m = mean(fwd_rtn_1m, na.rm = TRUE))

# ggridges
# https://cmdlinetips.com/2018/03/how-to-plot-ridgeline-plots-in-r/
dfm %>% drop_na() %>% 
  select(date_stamp, kurt_ari_120d_dcl, fwd_rtn_1m) %>% 
  filter(kurt_ari_120d_dcl %in% c(1, 10)) %>% 
  mutate(kurt_ari_120d_dcl = as.factor(kurt_ari_120d_dcl),
         date_stamp        = fct_rev(as.factor(date_stamp))) %>% 
  ggplot(aes(
    x = fwd_rtn_1m, 
    y = date_stamp, 
    fill = kurt_ari_120d_dcl
  )) + 
  geom_density_ridges(
    alpha = .6, 
    color = 'white', 
    from = -.6, 
    to = .6, 
    panel_scaling = FALSE
  )


# Scratch
tge <- df_raw %>% filter(symbol == 'TGE')










# INTRA-PORTFOLIO CORRELATION

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

# Apply to data frame
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
    )
