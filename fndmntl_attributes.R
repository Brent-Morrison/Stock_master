
#==============================================================================
#
# Script to extract data from STOCK_MASTER database and enrich fundamental data
# data with various attributes
#
#==============================================================================

library(slider)
library(dplyr)
library(tibble)
library(tidyr)
library(purrr)
library(summarytools)
library(moments)
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


# Parameters and query string
end_date <- '2020-10-31'
start_date <- paste(
  year(as.Date(end_date) - years(2)), 
  sprintf('%02d', month(as.Date(end_date) %m-% months(1))), 
  '01', 
  sep = '-')


# Read data
sql1 <- "select * from edgar.qrtly_fndmntl_ts_vw where date_available >= ?start_date and date_available <= ?end_date"
sql1 <- sqlInterpolate(conn = con, sql = sql1, start_date = start_date, end_date = end_date)
qry1 <- dbSendQuery(conn = con, statement = sql1) 
qrtly_fndmntl_ts_raw<- dbFetch(qry1)

sql2 <- "select * from alpha_vantage.monthly_price_ts_vw where date_stamp >= ?start_date and date_stamp <= ?end_date"
sql2 <- sqlInterpolate(conn = con, sql = sql2, start_date = start_date, end_date = end_date)
qry2 <- dbSendQuery(conn = con, statement = sql2) 
monthly_price_ts_raw <- dbFetch(qry2)

sql3 <- "select * from alpha_vantage.splits_vw where date_stamp >= ?start_date and date_stamp <= ?end_date"
sql3 <- sqlInterpolate(conn = con, sql = sql3, start_date = start_date, end_date = end_date)
qry3 <- dbSendQuery(conn = con, statement = sql3) 
monthly_splits_raw <- dbFetch(qry3)


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
        max(floor_date(qrtly_fndmntl_ts$date_available, unit = 'month')), 
        by = "month"),
    unit = 'month') - 1

# Expand dates to monthly
monthly_fndmntl_ts <- qrtly_fndmntl_ts %>% 
  group_by(ticker) %>% 
  complete(date_available = date_range, ticker) %>% 
  fill(sector:last_col()) %>% 
  filter(!is.na(sector)) %>% 
  ungroup()


# Add month end date to price data for join
monthly_price_ts_raw$date_available <- ceiling_date(monthly_price_ts_raw$date_stamp, unit = 'month') - 1


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
  mutate(
    shares_cso_fact = case_when(
      -total_equity / (.000001 * shares_cso * close) > 0.025 & -total_equity / (.000001 * shares_cso * close) < 20 ~ .000001,
      -total_equity / (.001 * shares_cso * close) > 0.025 & -total_equity / (.001 * shares_cso * close) < 20 ~ .001,
      -total_equity / (1 * shares_cso * close) > 0.025 & -total_equity / (1 * shares_cso * close) < 20 ~ 1,
      -total_equity / (1000 * shares_cso * close) > 0.025 & -total_equity / (1000 * shares_cso * close) < 20 ~ 1000,
      -total_equity / (1000000 * shares_cso * close) > 0.025 & -total_equity / (1000000 * shares_cso * close) < 20 ~ 1000000,
      TRUE ~ 0
      ),
    shares_ecso_fact = case_when(
      -total_equity / (.000001 * shares_ecso * close) > 0.025 & -total_equity / (.000001 * shares_ecso * close) < 20 ~ .000001,
      -total_equity / (.001 * shares_ecso * close) > 0.025 & -total_equity / (.001 * shares_ecso * close) < 20 ~ .001,
      -total_equity / (1 * shares_ecso * close) > 0.025 & -total_equity / (1 * shares_ecso * close) < 20 ~ 1,
      -total_equity / (1000 * shares_ecso * close) > 0.025 & -total_equity / (1000 * shares_ecso * close) < 20 ~ 1000,
      -total_equity / (1000000 * shares_ecso * close) > 0.025 & -total_equity / (1000000 * shares_ecso * close) < 20 ~ 1000000,
      TRUE ~ 0
      ),
    shares_os_unadj = round(
      case_when(
        shares_cso_fact == 1 ~ shares_cso,
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
  select(-date_stamp.x, -date_stamp.y) %>% 
  rename(date_stamp = date_available) %>% 
  # Filter for most recent year
  filter(year(date_stamp) == year(end_date))

# Replace Inf with NA
monthly_fndmntl_ts[monthly_fndmntl_ts == -Inf] <- NA
monthly_fndmntl_ts[monthly_fndmntl_ts == Inf] <- NA


# Number of stocks months having equity less than 0
monthly_fndmntl_ts %>% filter(total_equity >= 0) %>% tally()
neg_equity <- length(which(monthly_fndmntl_ts$total_equity >= 0))
nas <- monthly_fndmntl_ts %>% select(-split_coef) %>% filter_all(any_vars(is.na(.)))


# Number of stock months with NA's
monthly_fndmntl_ts %>% select(-split_coef) %>% filter_all(any_vars(is.na(.))) %>% tally()


# Number of stocks with NA's
monthly_fndmntl_ts %>% select(-split_coef) %>% filter_all(any_vars(is.na(.))) %>% select(ticker) %>% n_distinct()


# Dataframe ex NA's
monthly_fndmntl_exna <- monthly_fndmntl_ts %>% 
  select(-date_stamp, -(sector:shares_ecso), -split_date,-split_coef, -shares_os_unadj, -split_adj_end1) %>% 
  filter_all(all_vars(!is.na(.)))


# Descriptive stats
data <- select(monthly_fndmntl_exna, date_stamp, asset_growth:roe, mkt_cap, book_price)

desc_stats_func <- function(x) {
  bind_rows(
    x %>% summarise(across(where(is.numeric), ~ n())),
    x %>% summarise(across(where(is.numeric), ~ mean(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ sd(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ skewness(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ kurtosis(.x, na.rm = TRUE))),
    x %>% summarise(across(where(is.numeric), ~ quantile(.x, probs = c(0,.02,.05,.1,.25,.5,.75,.9,.95,.98,1), na.rm = TRUE)))
  ) %>% round(3) %>% 
  mutate(statistic = c('count','mean','sd','skew','kurt','min','qtl02','qtl05','qtl10','q1','med','q3','qtl090','qtl095','qtl098','max'))
}

desc_stats <- data %>% split(.$date_stamp) %>% map_dfr(., desc_stats_func, .id = 'date_stamp')


# Missing values
missing_1 <- monthly_fndmntl_ts %>%
  select(-date_stamp, -(sector:shares_ecso), -split_date,-split_coef, -shares_os_unadj, -split_adj_end1) %>% 
  mutate(across(where(is.numeric), is.na)) %>%  # replace all NA with TRUE and else FALSE
  pivot_longer(-ticker, names_to = 'attribute') %>%  # pivot longer
  filter(value) %>%   # remove the FALSE rows
  select(-value) %>% 
  group_by(ticker,attribute) %>%    # group by the ID
  summarise(count = n()) %>% 
  ungroup(attribute) %>% 
  summarise(
    missing_type = toString(attribute), 
    records = max(count)) # convert the variable names to a string column

missing_2 <- missing_1 %>% 
  group_by(missing_type) %>% 
  summarise(records = -sum(records), tickers = -n_distinct(ticker))

start_rec <- length(monthly_fndmntl_ts$ticker)
start_tic <- n_distinct(monthly_fndmntl_ts$ticker)
exna_rec <- -length(monthly_fndmntl_exna$ticker) #sum(missing_2$records)
exna_tic <- -n_distinct(monthly_fndmntl_exna$ticker) #sum(missing_2$tickers)
missing_2 <- rbind(c('start_records', start_rec, start_tic), missing_2)
missing_2 <- rbind(missing_2, c('end_records', exna_rec, exna_tic))

# Waterfall plot
levels <- missing_2$missing_type
  
missing_3 <- missing_2 %>% 
  mutate(
    missing_type = factor(missing_type, levels = levels),
    ymin = round(cumsum(records), 3),
    ymax = lag(cumsum(records), default = 0),
    xmin = c(head(missing_type, -1), NA),
    xmax = c(tail(missing_type, -1), NA),
    Impact = ifelse(
      missing_type %in% c(as.character(missing_2$missing_type[1]), as.character(missing_2$missing_type[nrow(missing_2)])),'Total',
      ifelse(records > 0, 'Increase', 'Decrease'))
    )

ggplot(missing_3) +
  theme(legend.position = "none", 
        axis.text.x = element_text(angle = 45, vjust = 1, hjust = 1)) +
  labs(y = "Observations", x = "Missing type", title = "Missing data") +
  geom_rect(aes(
    xmin = as.integer(missing_type) - .3,
    xmax = as.integer(missing_type) + .3, 
    ymin = ymin,
    ymax = ymax,
    fill = Impact), 
    colour = "black") +
  scale_x_discrete(limits = levels) +
  coord_cartesian(ylim=c(-exna_rec * .95, max(missing_3$ymax) + .025))


# ISSUES

# Asset growth can be very large (think mergers & acquisitions).  We do not want to remove outliers as suspected spurious, rather
# winsorise to allow for better statistical properties.

# EQT is only a divestiture adjustment as opposed to stock split - the split impacts price only, not shares o/s
# https://www.fool.com/investing/2018/11/13/heres-how-eqt-corporation-stock-fell-46-today-but.aspx
# 
# CRC - incorrect adjustment to shares O/s.  Implemented rule to toggle simfin source which has split adjusted shares o/s.
# 
# RIG - incorect shares o/s adjustment applied Oct-2020 with low  
xx1 <- monthly_fndmntl_ts %>% 
  filter(ticker %in% c('AFL','AAPL','AGCL','CHDN','CRC','PCG','ARW','GRMN','SBUX','EQT','HLF','FAST','TSLA')) %>% 
  select(
    ticker, date_available, report_date, publish_date, shares_cso, shares_ecso, shares_os_unadj, total_equity, close, 
    split_coef, adjusted_close, split_adj, shares_os, mkt_cap, price_book) 


# Disconnect
dbDisconnect(con)


# Indicators to add
# Standardised Unexplained Volume - https://www.biz.uiowa.edu/faculty/jgarfinkel/pubs/divop_JAR.pdf (s.3.1.2), 
# https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3212934
