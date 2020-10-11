##############################################################################
#
# Script to extract data from postgres database and enrich stock return
# data with various attributes
#
##############################################################################

# Installation

# https://github.com/andrewuhl/RollingWindow

# library("devtools")
# install_github("andrewuhl/RollingWindow")
library(RollingWindow)
library(slider)
library(dplyr)
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

# Read data
qry <- dbSendQuery(
  conn = con, 
  "
    select * 
    from alpha_vantage.daily_price_ts_view 
    where symbol in ('AKRX','CMA','A','AAPL','C')
  "
)
df_raw <- dbFetch(qry)


########################
### Daily stock data ###
########################

df <- df_raw %>% 
  group_by(symbol) %>% 
  mutate(
    sector                  = as.numeric(sector)
    ,rtn_ari_1d             = (adjusted_close-lag(adjusted_close))/lag(adjusted_close)
    ,rtn_ari_1d_mkt         = (sp500 - lag(sp500))/lag(sp500)
    ,rtn_log_1d             = log(adjusted_close) - lag(log(adjusted_close))
    ,vol_ari_20d            = RollingStd(rtn_ari_1d, window = 20, na_method = 'ignore') * sqrt(252)
    ,vol_ari_60d            = RollingStd(rtn_ari_1d, window = 60, na_method = 'ignore') * sqrt(252)
    ,vol_ari_120d           = RollingStd(rtn_ari_1d, window = 120, na_method = 'ignore')
    ,skew_ari_120d          = RollingSkew(rtn_ari_1d, window = 120, na_method = 'ignore')
    ,kurt_ari_120d          = RollingKurt(rtn_ari_1d, window = 120, na_method = 'ignore')
    ,amihud                 = abs(rtn_ari_1d) / (volume * adjusted_close / 10e7)
    ,amihud_3m              = RollingMean(amihud, window = 60, na_method = 'ignore')
    ,amihud_vol_3m          = RollingStd(amihud, window = 60, na_method = 'ignore') * sqrt(252)
    ,smax                   = slide_dbl(rtn_ari_1d, ~mean(tail(sort(.x), 5)), .before = 20) / vol_ari_20d
    ,cor_rtn_1d_mkt_120d    = RollingCorr(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'ignore')
    ,beta_rtn_1d_mkt_120d   = RollingBeta(rtn_ari_1d, rtn_ari_1d_mkt, window = 120, na_method = 'ignore')
  )



# Disconnect
dbDisconnect(con)
