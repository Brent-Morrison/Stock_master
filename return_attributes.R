
#==============================================================================
#
# Script to extract stock price data from postgres database, enrich with
# various "technical indicator" attributes and write to database.
#
# TO DO
# 1. Add Standardised Unexplained Volume per https://www.biz.uiowa.edu/faculty/jgarfinkel/pubs/divop_JAR.pdf (s.3.1.2). 
#    Efficacy per https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212934
# 2. Change set-up so script takes year as a parameter and outputes results for that year,
#    ensure burn-in data is collected
#
#==============================================================================

# RollingWindow installation

# https://github.com/andrewuhl/RollingWindow
#library("devtools")
#install_github("andrewuhl/RollingWindow")

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
date_param <- '2018-01-01'
sql <- "select * from alpha_vantage.daily_price_ts_vw where date_stamp > ?date_param"
sql <- sqlInterpolate(conn = con, sql = sql, date_param = date_param)

# Read data
qry <- dbSendQuery(
  conn = con, 
  statement = sql
  ) 
df_raw <- dbFetch(qry)



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
    ,suv = slide(
      .x = tibble(volume = volume, rtn_ari_1d = rtn_ari_1d),  #., See https://davisvaughan.github.io/slider/articles/rowwise.html
      .f = suv, 
      .before = 59, 
      .complete = TRUE
    )
  )

monthly <- daily %>% 
  group_by(symbol, date_stamp = floor_date(date_stamp, "month")) %>% 
  mutate(date_stamp = ceiling_date(date_stamp, unit = "month") - 1) %>% 
  summarise(
    close                   = last(close),
    adjusted_close          = last(adjusted_close),
    volume                  = mean(volume ),
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
    ) %>% ungroup()

monthly <- monthly %>% 
  group_by(symbol) %>% 
  mutate(
    rtn_ari_1m              = (adjusted_close-lag(adjusted_close))/lag(adjusted_close),
    rtn_ari_3m              = (adjusted_close-lag(adjusted_close, 3))/lag(adjusted_close, 3),
    rtn_ari_6m              = (adjusted_close-lag(adjusted_close, 6))/lag(adjusted_close, 6),
    rtn_ari_12m             = (adjusted_close-lag(adjusted_close, 12))/lag(adjusted_close, 12),
    fwd_rtn_1m              = lead(rtn_log_1m)
    ) %>% ungroup()

monthly <- monthly %>% 
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
  ) %>% ungroup()


# Convert to dataframe for upload to database
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







# Test
dfm_smax <- dfm %>% drop_na() %>% 
  group_by(rtn_ari_12m_dcl) %>% 
  summarise(avg = mean(rtn_ari_12m), fwd_rtn_1m = mean(fwd_rtn_1m, na.rm = TRUE))

# ggridges
# https://cmdlinetips.com/2018/03/how-to-plot-ridgeline-plots-in-r/
dfm %>% drop_na() %>% 
  select(date_stamp, kurt_ari_120d_dcl, fwd_rtn_1m) %>% 
  filter(kurt_ari_120d_dcl %in% c(1, 10)) %>% 
  mutate(kurt_ari_120d_dcl = as.factor(kurt_ari_120d_dcl),
         date_stamp   = fct_rev(as.factor(date_stamp))) %>% 
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





# STANDARDISED UNEXPLAINED VOLUME CALCULATION - 1

# SUV function
suv <- function(df) { 
  df <- df %>% 
    select(volume, rtn_ari_1d) %>% 
    mutate(
      pos = if_else(rtn_ari_1d >= 0, abs(rtn_ari_1d), 0),
      neg = if_else(rtn_ari_1d < 0, abs(rtn_ari_1d), 0)
      )
  mdl <- lm(volume ~ pos + neg, data = df)
  last(residuals(mdl))/sigma(mdl)
}

# Test data
suv_test_data <- daily %>% filter(symbol %in% c('A','AAPL')) %>% select(symbol:rtn_ari_1d) %>% ungroup()

# Results 1
suv_test_result <- suv(suv_test_data)

# Results 2
suv_test_result <- suv_test_data %>% 
  group_by(symbol) %>% 
  mutate(
    suv = slide(
      .x = tibble(volume = volume, rtn_ari_1d = rtn_ari_1d),  #., See https://davisvaughan.github.io/slider/articles/rowwise.html
      .f = suv, 
      .before = 59, 
      .complete = TRUE
      )
    )

write.csv(suv_test_data, 'suv_test_data.csv')



# STANDARDISED UNEXPLAINED VOLUME CALCULATION - 2

# SUV function
suv <- function(df) { 
  max_date = max(df$date_stamp)
  df <- df %>% 
    select(volume, rtn_ari_1d) %>% 
    mutate(
      pos = if_else(rtn_ari_1d >= 0, abs(rtn_ari_1d), 0),
      neg = if_else(rtn_ari_1d < 0, abs(rtn_ari_1d), 0)
    )
  mdl <- lm(volume ~ pos + neg, data = df)
  suv <- last(residuals(mdl))/sigma(mdl)
  return(tibble(date_stamp = max_date, suv = suv))
}


# SUV by group
suv_by_grp <- function(df) { 
  df %>% 
    split(.$symbol) %>%
    map_dfr(., suv, .id = 'symbol')
}


# Apply to data frame
suv_test <- daily %>% 
  ungroup() %>% 
  arrange(date_stamp) %>% 
  slide_period_dfr(
    .x =  .,
    .i = .$date_stamp,
    .period = "month",
    .f = suv_by_grp,
    .before = 5,
    .complete = TRUE
  )









# INTRA-PORTFOLIO CORRELATION

# Mean of matrix
mean_mtrx <- function(x) {
  mean(x[upper.tri(x)])
}

# IPC function
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

# IPC by group
ipc_by_grp <- function(df) { 
  df %>% 
    split(.$sector) %>%
    map_dfr(., ipc, .id = 'sector')
}

# Apply to data frame
ipc_test <- daily %>% 
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
