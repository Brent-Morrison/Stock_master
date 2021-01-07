
#==============================================================================
#
# Script extracting SEC derived accounting data from the STOCK_MASTER database, 
# and constructing fundamental and valuation attributes
# 
# ISSUES
#
# Asset growth can be very large (think mergers & acquisitions).  We do not want to remove outliers as suspected spurious, rather
# winsorise to allow for better statistical properties.
#
# EQT is only a divestiture adjustment as opposed to stock split - the split impacts price only, not shares o/s
# https://www.fool.com/investing/2018/11/13/heres-how-eqt-corporation-stock-fell-46-today-but.aspx
# 
# CRC - incorrect adjustment to shares O/s.  Implemented rule to toggle simfin source which has split adjusted shares o/s.
# 
# RIG - incorrect shares o/s adjustment applied Oct-2020 with low  
#
# AA - intangible assets is inconsistent, nil in some months and positive in others
#
# GEF - reports lodged on consecutive quarters labeled as Q2 returned (no year end change) 
#
# AAP - Fiscal period is incorrectly labeled Q2 for two consecutive report dates (publish dates 2018-05-22 & 2018-08-14)
#
# PCG - share count doubles in qtr 2020-06-30, 'LiabilitiesSubjectToCompromise'? 2019-12-31
#
# KMB - return on equity
#
# MA - shares outstanding errors
#
#==============================================================================

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




#==============================================================================
#
# DATABASE CONNECTION AND DATA RETRIEVAL
#
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


# Parameters and query string, dates to be formatted as character string
end_date <- '2020-12-31'
start_date <- paste(
  year(as.Date(end_date) - years(2)), 
  sprintf('%02d', month(as.Date(end_date) %m-% months(3))), 
  '01', 
  sep = '-')


# Read data and ensure correct ordering
sql1 <- "select * from edgar.qrtly_fndmntl_ts_vw where date_available >= ?start_date and date_available <= ?end_date"
sql1 <- sqlInterpolate(conn = con, sql = sql1, start_date = start_date, end_date = end_date)
qry1 <- dbSendQuery(conn = con, statement = sql1) 
qrtly_fndmntl_ts_raw <- dbFetch(qry1)
qrtly_fndmntl_ts_raw <- arrange(qrtly_fndmntl_ts_raw, ticker, report_date)

sql2 <- "select * from alpha_vantage.monthly_price_ts_vw where date_stamp >= ?start_date and date_stamp <= ?end_date"
sql2 <- sqlInterpolate(conn = con, sql = sql2, start_date = start_date, end_date = end_date)
qry2 <- dbSendQuery(conn = con, statement = sql2) 
monthly_price_ts_raw <- dbFetch(qry2)

sql3 <- "select * from alpha_vantage.splits_vw where date_stamp >= ?start_date and date_stamp <= ?end_date"
sql3 <- sqlInterpolate(conn = con, sql = sql3, start_date = start_date, end_date = end_date)
qry3 <- dbSendQuery(conn = con, statement = sql3) 
monthly_splits_raw <- dbFetch(qry3)





#==============================================================================
# 
# CHECK MISSINGNESS
# missing defined as nil or NA cash, total assets, total equity, net income,
# shares outstanding or non-continuous data (ie., missing quarter)
#
#==============================================================================

# Stocks with missing data
qrtly_fndmntl_excl <- qrtly_fndmntl_ts_raw %>% 
  group_by(ticker) %>% 
  mutate(
    miss_qtr_ind = case_when(
      is.na(lag(fiscal_period)) ~ 1,
      fiscal_period == 'Q1' & lag(fiscal_period) == 'Q4' ~ 1,
      fiscal_period == 'Q2' & lag(fiscal_period) == 'Q1' ~ 1,
      fiscal_period == 'Q3' & lag(fiscal_period) == 'Q2' ~ 1,
      fiscal_period == 'Q4' & lag(fiscal_period) == 'Q3' ~ 1,
      TRUE ~ 0)
  ) %>% 
  ungroup() %>% 
  mutate(shares_os = pmax(shares_cso, shares_ecso, na.rm = TRUE)) %>% 
  replace_with_na(replace = list(
    cash_equiv_st_invest = c(0), 
    total_assets = c(0), 
    total_equity = c(0), 
    shares_os = c(0),
    miss_qtr_ind = c(0)
  ))


# Missingness plot (FILTER BY VALID_YEAR)
qrtly_fndmntl_excl %>% filter(year(report_date) == year(end_date)) %>% select(-date_available, -shares_cso, -shares_ecso) %>% vis_miss()


# Co-occurance plot 
qrtly_fndmntl_excl %>% filter(year(report_date) == year(end_date)) %>% select(-shares_cso, -shares_ecso) %>% gg_miss_upset()


# Data for bar plot
fnl_initial_popn <- qrtly_fndmntl_ts_raw %>% 
  filter(year(report_date) == year(end_date)) %>% 
  select(ticker) %>% n_distinct()

fnl_shares_os <- qrtly_fndmntl_excl %>%
  filter(year(report_date) == year(end_date)) %>% 
  select(ticker, shares_os) %>%  
  filter(is.na(shares_os)) %>% 
  select(ticker) %>% n_distinct()

fnl_miss_qtr <- qrtly_fndmntl_excl %>%
  filter(year(report_date) == year(end_date)) %>% 
  select(ticker, shares_os, miss_qtr_ind) %>%  
  filter(!is.na(shares_os), is.na(miss_qtr_ind)) %>% 
  select(ticker) %>% n_distinct()


# List tickers with either nil or NA for cash, total assets, total equity, net income or shares outstanding,
# or non-continuous data in the year under analysis
excluded <- qrtly_fndmntl_excl %>%
  filter(year(report_date) == year(end_date)) %>% 
  select(
    ticker,
    cash_equiv_st_invest, 
    total_assets, 
    total_equity, 
    shares_os,
    miss_qtr_ind
  ) %>%  
  filter_all(any_vars(is.na(.))) %>% 
  #filter(across(everything(), ~is.na(.x))) %>% 
  select(ticker) %>% 
  distinct()


# Exclude tickers with irreplaceable missing values (current year) and impute other missing values
qrtly_fndmntl_ts <- qrtly_fndmntl_ts_raw %>% 
  anti_join(excluded, by = 'ticker') %>% 
  group_by(sector) %>% 
  mutate(
    cash_ratio = mean(cash_equiv_st_invest / total_assets, na.rm = TRUE),
    cash_equiv_st_invest = if_else(is.na(cash_equiv_st_invest) | cash_equiv_st_invest == 0, total_assets * cash_ratio, cash_equiv_st_invest)
  ) %>% 
  ungroup()


fnl_miss_qtr_chk <- qrtly_fndmntl_ts %>% 
  filter(year(report_date) == year(end_date)) %>% 
  select(ticker) %>% 
  n_distinct()



#==============================================================================
# 
# CREATE ATTRIBUTES REQUIRING LAGGED QUARTERLY DATA
# analysis to be performed prior to expanding to monthly time series
# Re ttm earning ex worst quarter 'ttm_earnings_max' - https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3443289
#
#==============================================================================

qrtly_fndmntl_ts <- qrtly_fndmntl_ts %>% 
  group_by(ticker) %>% 
  mutate(
    ttm_earnings     = slide_dbl(net_income_qtly, sum, .before = 3, .complete = TRUE),
    ttm_earnings_max = slide_dbl(net_income_qtly, ~sum(.[. != max(.)]), .before = 3, .complete = TRUE) / 3 * 4,
    asset_growth     = (total_assets-lag(total_assets))/lag(total_assets),
    roa              = ttm_earnings / -slide_dbl(total_assets, mean, .before = 4, .complete = TRUE),
    roe              = ttm_earnings / slide_dbl(total_equity, mean, .before = 4, .complete = TRUE),
    leverage         = -total_liab / total_assets,
    cash_ratio       = cash_equiv_st_invest / total_assets,
    other_ca_ratio   = (total_cur_assets - cash_equiv_st_invest) / total_assets,
    intang_ratio     = intang_asset / total_assets
    # STANDARDISED UNEXPECTED EARNINGS TO GO HERE
    # SUE denotes Standardized Unexpected Earnings, and is calculated as the change in quarterly earnings 
    # divided by from its value four quarters ago divided by the standard deviation of this change in
    # quarterly earnings over the prior eight quarters
  ) %>% 
  ungroup()


# Expand to monthly, join price data

# Generate date sequence
date_range <- 
  ceiling_date(
    seq(min(floor_date(qrtly_fndmntl_ts$date_available, unit = 'month')), 
        as.Date(end_date),
        #max(floor_date(qrtly_fndmntl_ts$date_available, unit = 'month')),
        by = "month"),
    unit = 'month') - 1


# Expand dates to monthly periodicity
monthly_fndmntl_ts <- qrtly_fndmntl_ts %>% 
  group_by(ticker) %>% 
  complete(date_available = date_range, ticker) %>% 
  fill(valid_year:last_col()) %>% 
  filter(!is.na(sector)) %>% 
  ungroup()


# Add month end date to price data for join
monthly_price_ts_raw$date_available <- ceiling_date(monthly_price_ts_raw$date_stamp, unit = 'month') - 1

lbmadj <- 0.01
ubmadj <- 20

# Join price and split data 
monthly_fndmntl_ts <- 
  inner_join(
    x = monthly_fndmntl_ts, 
    y = monthly_price_ts_raw, 
    by = c('date_available' = 'date_available', 'ticker' = 'symbol')
    ) %>% 
  left_join(
    y = monthly_splits_raw, 
    by = c('date_available' = 'me_date', 'ticker' = 'symbol')
  ) %>% 
  # Clean shares outstanding data, inferring adjustment factor based on resultant book value / mkt cap ratio
  # If the adjustment results in a book value / mkt cap ratio between 'lbmadj' and 'umadj', use that adjustment
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
    shares_os_unadj = round(
      case_when(
        shares_cso_fact == 1 ~ shares_cso,  # chooses this first for PCG, when Entity (esco) is correct (larger value)
        shares_ecso_fact == 1 ~ shares_ecso,
        shares_cso_fact != 0 ~ shares_cso * shares_cso_fact,
        shares_ecso_fact != 0 ~ shares_ecso * shares_ecso_fact,
        TRUE ~ pmax(shares_cso, shares_ecso)
      ), 2)
    ) %>% 
  group_by(ticker) %>% 
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
    book_price       = round(-total_equity / mkt_cap, 3)
    ) %>% 
  ungroup() %>% 
  mutate(
    ttm_earn_yld      = round(ttm_earnings / mkt_cap, 3),
    ttm_earn_yld_max  = round(ttm_earnings_max / mkt_cap, 3)
    ) %>% 
  select(-date_stamp.x, -date_stamp.y) %>% 
  rename(date_stamp = date_available) %>% 
  # Filter for most recent year
  filter(year(date_stamp) == year(end_date))

fnl_no_price_data <- monthly_fndmntl_ts %>% filter(year(report_date) == year(end_date)) %>% select(ticker) %>% n_distinct()


# Replace Inf with NA and remove
monthly_fndmntl_ts[monthly_fndmntl_ts == -Inf] <- NA
monthly_fndmntl_ts[monthly_fndmntl_ts == Inf] <- NA

monthly_fndmntl_ts <- monthly_fndmntl_ts %>%
  select(-split_coef) %>% 
  filter(across(.cols = everything(), .fns = ~ !is.na(.x)))

fnl_remainder <- monthly_fndmntl_ts %>% filter(year(report_date) == year(end_date)) %>% select(ticker) %>% n_distinct()





#==============================================================================
# 
# PB-ROE model
# Robust / Theil Sen regression references 
# https://www.jamesuanhoro.com/post/2017/09/21/theil-sen-regression-in-r/
# https://cran.r-project.org/web/packages/mblm/mblm.pdf
# https://education.wayne.edu/eer_dissertations/ahmad_farooqi_dissertation.pdf
# http://extremelearning.com.au/the-siegel-and-theil-sen-non-parametric-estimators-for-linear-regression/
#
#==============================================================================
 
# Function to extract r-squared
get_rsq <- function(x) glance(x)$r.squared

# Model
monthly_fndmntl_ts <- monthly_fndmntl_ts %>% 
  group_by(date_stamp, sector) %>% 
  
  # Remove groups with 1 stock, TS regression will error
  filter(n() > 1) %>% 
  
  # Impute mean value per group - https://cran.r-project.org/web/packages/broom/vignettes/broom_and_dplyr.html
  # TO DO - impute ROE for stocks with negative equity from ROA
  mutate(
    roe           = if_else(is.na(roe), mean(roe, na.rm = TRUE), roe),
    roe           = Winsorize(roe, minval = -0.5, maxval = 0.75, na.rm = TRUE),
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


# Plot log_pb against roe 
monthly_fndmntl_ts %>% 
  filter(date_stamp == as.Date('2020-07-31')) %>% 
  ggplot(aes(x = log_pb, y = roe)) +
  facet_wrap(~sector, ncol = 4) + 
  geom_point() 
  # Add TS regression line of fit via 
  # geom_abline(intercept = 0, slope = 1, alpha = .2) +


# Plot actual log_pb against predicted log_pb - Theil Sen
monthly_fndmntl_ts %>% 
  filter(date_stamp > as.Date('2020-06-30'), fin_nonfin == 'non_financial', sector %in% c(3,4,7)) %>% 
  mutate(
    actual     = log_pb,
    prediction = actual + pbroe_rsdl_ts
    ) %>% 
  ggplot(aes(x = actual, y = prediction)) +
  geom_abline(intercept = 0, slope = 1, alpha = .2) +
  geom_point() +
  facet_grid(rows = vars(date_stamp), cols = vars(sector)) +
  labs(
    title = 'Actual versus predicted log book / price ratio',
    subtitle = 'Prediction derived from PB-ROE model applied at sector level using Theil Sen estimator',
    caption = 'A negative residual has the predicted or modeled log(bp) ratio lower than the actual log(bp) ratio.\nSince a lower log(pb) ratio represents a higher valuation, this indicates the model considers the stock should \nbe valued higher than the it is. Therefore the stock is considered undervalued.'
    ) +
  theme(plot.caption = element_text(size = 8, margin = margin(t = 10), color = "grey", hjust = 0))


# Plot actual log_pb against predicted log_pb - OLS
monthly_fndmntl_ts %>% 
  filter(date_stamp > as.Date('2020-06-30'), fin_nonfin == 'non_financial', sector %in% c(3,4,7)) %>% 
  mutate(
    actual     = log_pb,
    prediction = actual + pbroe_rsdl_ols
  ) %>% 
  ggplot(aes(x = actual, y = prediction)) +
  geom_abline(intercept = 0, slope = 1, alpha = .2) +
  geom_point() +
  facet_grid(rows = vars(date_stamp), cols = vars(sector)) +
  labs(
    title = 'Actual versus predicted log book / price ratio',
    subtitle = 'Prediction derived from PB-ROE model applied at sector level using OLS estimator',
    caption = 'A negative residual has the predicted or modeled log(bp) ratio lower than the actual log(bp) ratio.\nSince a lower log(pb) ratio represents a higher valuation, this indicates the model considers the stock should \nbe valued higher than the it is. Therefore the stock is considered undervalued.'
  ) +
  theme(plot.caption = element_text(size = 8, margin = margin(t = 10), color = "grey", hjust = 0))




#==============================================================================
# 
# Visualize missingness
#
#==============================================================================

# DF for bar plot
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

# Bar plot
ggplot(stages, aes(x = stage, y= tickers)) +
  geom_col() +
  geom_text(aes(label = tickers, vjust = -0.5), size = 3.5)





#==============================================================================
#
# Descriptive statistics
#
#==============================================================================

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
  


# Write to DT
datatable(
  select(desc_stats, date_stamp, statistic, cash_ratio:ttm_earn_yld_max, -ttm_earnings, -ttm_earnings_max),
  #rownames = FALSE,
  filter = 'top', 
  options = list(
    pageLength = 16
    )
  )


#==============================================================================
#
# Write to postgres database & disconnect
#
#==============================================================================



dbWriteTable(
  conn = con, 
  name = SQL('access_layer.fundamental_attributes'), 
  value = monthly_fndmntl_ts, 
  row.names = FALSE, 
  append = TRUE
)


# Disconnect
dbDisconnect(con)





#==============================================================================
#
# SCRATCH
#
#==============================================================================

# Reticulate for multivariate Thiel Sen regression
use_condaenv(condaenv = 'STOCK_MASTER', required = TRUE)
source_python('C:/Users/brent/Documents/VS_Code/postgres/postgres/ts_regression.py')


ts_data <- monthly_fndmntl_ts %>% 
  filter(date_stamp > as.Date('2020-06-30'), fin_nonfin == 'non_financial', sector %in% c(3,4,7)) %>% 
  mutate(
    total_cur_liab_abs        = -total_cur_liab,
    lt_debt_abs               = -lt_debt,
    non_cash_cur_assets       = total_cur_assets - cash_equiv_st_invest,
    other_noncur_liab_abs     = -(total_noncur_liab - lt_debt),
    total_equity_abs          = -total_equity,
    ttm_earnings_abs          = -ttm_earnings
    ) %>% 
  select(
    date_stamp, sector, ticker, mkt_cap,
    cash_equiv_st_invest, non_cash_cur_assets, total_noncur_assets, 
    total_cur_liab_abs, lt_debt_abs, other_noncur_liab_abs, total_equity_abs, ttm_earnings_abs
    )


# Excluding the '.id' argument to map_dfr results in the date_stamp being returned from python/pandas 
# as a list.  This is not usable in the data frame.
# Including the '.id' argument as below requires converting this back to date per the 'mutate'
# TO DO: LIMIT PREDICTION TO ZERO AND 110% OF THE MAX MKT CAP
ts_pred <- ts_data %>% 
  split(list(.$date_stamp,.$sector)) %>% 
  map_dfr(., theil_sen_py, .id = 'date_stamp', iter = as.integer(100)) %>% 
  mutate(
    date_stamp = as.Date(substr(date_stamp, 1, 10)),
    actual = prediction + residual
    )


# Summary scores
ts_scores <- ts_pred %>% 
  group_by(date_stamp, sector) %>% 
  nest() %>% 
  mutate(
    fit = map(data, ~lm(actual ~ prediction, data = .x)), 
    rsq = map_dbl(fit, get_rsq)
    ) %>% 
  unnest(cols = c(data, rsq)) %>% 
  select(-fit) %>% 
  summarise(
    median_mkt_cap = median(actual),
    max_mkt_cap = max(actual),
    min_mkt_cap = min(actual),
    median_pred = median(prediction),
    max_pred = max(prediction),
    min_pred = min(prediction),
    median_rsdl = median(residual),
    rsq = mean(rsq)
  ) %>% 
  ungroup() %>% 
  mutate(across(3:7, round, 0)) %>% 
  mutate(across(8, round, 2))



# Plot actual vs predicted 
ts_pred %>% 
  mutate(actual = prediction + residual) %>% 
  ggplot(aes(x = log(actual), y = log(prediction))) +
  geom_abline(intercept = 0, slope = 1, alpha = .2) +
  geom_point() +
  facet_grid(rows = vars(date_stamp), cols = vars(sector))




residuals(lm(mkt_cap ~ ., data = select(zdf, -ticker)))

write.csv(ts_df, 'ts_test.csv')
