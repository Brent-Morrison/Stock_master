# --------------------------------------------------------------------------------------------------------------------------
# Development for "database_status.Rmd"
# --------------------------------------------------------------------------------------------------------------------------

# Libraries
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(DBI)
library(RPostgres)


# Source functions
source("C:/Users/brent/Documents/VS_Code/postgres/postgres/return_attributes.R")

# Connect to db
con <- stock_master_connect()



# ------------------
# Get raw edgar data
sql0 <- "select 'sub' as table, sec_qtr, count(*) as n from edgar.sub group by 1,2 order by 2 desc, 1 asc"
qry0 <- dbSendQuery(conn = con, statement = sql0) 
edgar_raw_data0 <- dbFetch(qry0)

# Function to convert string to date
qtr_to_date <- function(x){
  as.Date(ISOdate(
    ifelse(substr(x,6,6) == '4', as.numeric(substr(x,1,4))+1, substr(x,1,4)),
    ifelse(substr(x,6,6) == '4', 1, as.numeric(substr(x,6,6))*3+1),
    1
  ))-1
}

# Add date string
edgar_raw_data0$date_stamp <- qtr_to_date(edgar_raw_data0$sec_qtr)


# -------------------------------------------
# Get fundamental data (processed edgar data)
sql1 <- "select sec_qtr, count(*) as n from edgar.edgar_fndmntl_all_tb group by 1 order by 1 desc"
qry1 <- dbSendQuery(conn = con, statement = sql1) 
fndmntl_data1 <- dbFetch(qry1)

# Add date string
fndmntl_data1$date_stamp <- qtr_to_date(fndmntl_data1$sec_qtr)


# --------------------------
# Get price data (ex S&P500)
sql2 <- "
	select 
	symbol
	,max(date_stamp) as max_date
	from access_layer.shareprices_daily 
	where 1 = 1
	and symbol != 'GSPC'
	group by symbol
  "
qry2 <- dbSendQuery(conn = con, statement = sql2) 
price_data2 <- dbFetch(qry2)


# ----------------------
# Get S&P500 price data
sql3 <- "
	select 
	symbol
	,max(date_stamp) as max_date
	from access_layer.shareprices_daily 
	where 1 = 1
	and symbol = 'GSPC'
	group by symbol
  "
qry3 <- dbSendQuery(conn = con, statement = sql3) 
sp500_data3 <- dbFetch(qry3)


# -------------------------------
# Get processed price return data
sql4 <- "select date_stamp, count(*) as n from access_layer.return_attributes group by 1 order by 1 desc"
qry4 <- dbSendQuery(conn = con, statement = sql4) 
price_rtn_data4 <- dbFetch(qry4)


# ------------------------------
# Get fundamental indicator data  #### NOTE USED AS YET ####
sql5 <- "select date_stamp, count(*) as n from access_layer.fundamental_attributes group by 1 order by 1 desc"
qry5 <- dbSendQuery(conn = con, statement = sql5) 
fndmntl_ind_data5 <- dbFetch(qry5)


# Start and end date 
end_date <- lubridate::floor_date(Sys.Date(), unit='month')
start_date <- end_date %m+% months(-12) 


# Make sequence of dates to join to
date_seq <- data.frame(date_stamp = seq(from = start_date %m+% months(1), to = end_date %m+% months(1), by = "month") - 1)


# Join 
df0 <- dplyr::left_join(x = date_seq, y = edgar_raw_data0, by = 'date_stamp')
#df <- merge(x = date_seq, y = edgar_raw_data, by = "date_stamp", all.x = TRUE)

# Int64 from database to integer type
# https://github.com/tidyverse/tidyr/issues/1061
df0$n <- as.numeric(df0$n)

# Fill
df0 <- tidyr::fill(df0, n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'Raw edgar data', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


# Join 
df1 <- dplyr::left_join(x = date_seq, y = fndmntl_data1, by = 'date_stamp')


# Int64 from database to integer type
# https://github.com/tidyverse/tidyr/issues/1061
df1$n <- as.numeric(df1$n)

# Fill
df1 <- tidyr::fill(df1, n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'Transformed edgar data', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


df2 <- dplyr::left_join(
  x = date_seq, 
  y = group_by(price_data2, max_date) %>% summarise(n = n()) %>% filter(n > 750) %>% rename(date_stamp = max_date), 
  by = 'date_stamp'
  ) %>% 
  mutate(n = as.numeric(n)) %>% 
  tidyr::fill(n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'Share prices', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)

df3 <- dplyr::left_join(
  x = date_seq, 
  y = rename(sp500_data3, date_stamp = max_date) %>% mutate(n = 1), 
  by = 'date_stamp'
  ) %>% 
  mutate(n = as.numeric(n)) %>% 
  tidyr::fill(n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'S&P 500 data', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


df4 <- dplyr::left_join(x = date_seq, y = price_rtn_data4, by = 'date_stamp') %>% 
  mutate(n = as.numeric(n)) %>% 
  tidyr::fill(n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'Transformed price features', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)



# Union
df <- bind_rows(df0, df1)
df <- bind_rows(df, df2)
df <- bind_rows(df, df3)
df <- bind_rows(df, df4)



ggplot(df,aes(x = data_source, y = date_stamp)) + 
  geom_point(
    shape = 22, #21 
    fill = "black", #"lightgray"
    color = "black", 
    size = 5
    ) + 
  scale_y_date(
    limits= c(ceiling_date(start_date, unit = "month")-1, ceiling_date(end_date, unit = "month")-1), 
    #date_breaks = "1 month", 
    breaks = date_seq$date_stamp,
    date_labels = "%b", 
    minor_breaks = NULL
    ) +
  coord_flip() + labs(x = "", y = "")



#df1 <- data.frame(
#  date_stamp = seq(from = start_date %m+% months(1), to = as.Date('2021-12-31') %m+% months(1), by = "month") - 1,
#  data_source = rep('edgar_fundmntl_all',24)
#)





# --------------------------------------------------------------------------------------------------------------------------
# Test "price attributes" function
# --------------------------------------------------------------------------------------------------------------------------

# Libraries
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


# Source functions
source("C:/Users/brent/Documents/VS_Code/postgres/postgres/return_attributes.R")

# Connect to db
con <- stock_master_connect()

# Test
df <- price_attributes(
  end_date = '2021-12-31', 
  con = con,
  months_to_load = 12, 
  deciles = FALSE,
  write_to_db = FALSE, 
  disconnect = FALSE, 
  return_df = TRUE
)



# Interrogate data uploaded
upload_data <- df$upload_data
missing_data <- df$missing_data
df$execution_time


# Write to DB
dbWriteTable(
  conn = con, 
  name = SQL('access_layer.return_attributes'), 
  value = upload_data, 
  row.names = FALSE, 
  append = TRUE
)




# --------------------------------------------------------------------------------------------------------------------------
# Test "price attributes" function
# --------------------------------------------------------------------------------------------------------------------------

library(dagitty)

g <- dagitty('dag {
    X [pos="0,1"]
    Y [pos="1,1"]
    Z [pos="2,1"]
    W [pos="1,0"]
    T [pos="2,2"]
    
    X -> Y -> Z -> T
    X -> W -> Y -> T
    W -> Z
}')
plot(g)
