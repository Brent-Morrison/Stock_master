
# Libraries
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(DBI)
library(RPostgres)


args <- commandArgs(trailingOnly = TRUE)
#database <- args[1]        #'stock_master_test'
database <- 'stock_master'


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



# ------------------
# Get raw edgar data
sql0 <- "select 'sub' as table, sec_qtr, count(*) as n from edgar.sub group by 1,2 order by 2 desc, 1 asc"
qry0 <- dbSendQuery(conn = con, statement = sql0) 
d_edgar_raw_data <- dbFetch(qry0)

# Function to convert string to date
qtr_to_date <- function(x){
  as.Date(ISOdate(
    ifelse(substr(x,6,6) == '4', as.numeric(substr(x,1,4))+1, substr(x,1,4)),
    ifelse(substr(x,6,6) == '4', 1, as.numeric(substr(x,6,6))*3+1),
    1
  ))-1
}


# Add date string
d_edgar_raw_data$date_stamp <- qtr_to_date(d_edgar_raw_data$sec_qtr)


# -------------------------------------------
# Get fundamental data (processed edgar data)
sql1 <- "select sec_qtr, count(*) as n from edgar.edgar_fndmntl_all_tb group by 1 order by 1 desc"
qry1 <- dbSendQuery(conn = con, statement = sql1) 
e_fndmntl_data <- dbFetch(qry1)

# Add date string
e_fndmntl_data$date_stamp <- qtr_to_date(e_fndmntl_data$sec_qtr)


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
b_price_data <- dbFetch(qry2)
b_price_data <- b_price_data %>% mutate(date_stamp = if_else(substr(max_date,9,11) > 28, ceiling_date(max_date, unit = 'month') - 1, as.Date(max_date)))
# https://stackoverflow.com/questions/6668963/how-to-prevent-ifelse-from-turning-date-objects-into-numeric-objects
b_price_data['date_stamp1'] <- sapply(b_price_data$max_date, function(x) if (substr(x,9,11) > 28) ceiling_date(x, unit = 'month') - 1 else as.Date(x))


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
a_sp500_data <- dbFetch(qry3)   # THIS NEED TO BE CONVERTED TO A MONTH END
a_sp500_data['date_stamp'] <- ceiling_date(a_sp500_data$max_date, 'month')-1  #-----------------------------------------------------



# -------------------------------
# Get processed price return data
sql_c <- "select date_stamp, count(*) as n from access_layer.return_attributes group by 1 order by 1 desc"
qry_c <- dbSendQuery(conn = con, statement = sql_c) 
c_price_rtn_data <- dbFetch(qry_c)


# ------------------------------
# Get fundamental indicator data
sql5 <- "select date_stamp, count(*) as n from access_layer.fundamental_attributes group by 1 order by 1 desc"
qry5 <- dbSendQuery(conn = con, statement = sql5) 
f_fndmntl_ind_data <- dbFetch(qry5)


# Start and end date 
end_date <- lubridate::floor_date(Sys.Date(), unit='month')
start_date <- end_date %m+% months(-12) 


# Make sequence of dates to join to
date_seq <- data.frame(date_stamp = seq(from = start_date %m+% months(1), to = end_date %m+% months(1), by = "month") - 1)


# Join 
df_d <- dplyr::left_join(x = date_seq, y = d_edgar_raw_data, by = 'date_stamp')
#df <- merge(x = date_seq, y = edgar_raw_data, by = "date_stamp", all.x = TRUE)

# Int64 from database to integer type
# https://github.com/tidyverse/tidyr/issues/1061
df_d$n <- as.numeric(df_d$n)

# Fill
df_d <- tidyr::fill(df_d, n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'd. Raw SEC data \n   (edgar.sub)', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


# Join 
df_e <- dplyr::left_join(x = date_seq, y = e_fndmntl_data, by = 'date_stamp')


# Int64 from database to integer type
# https://github.com/tidyverse/tidyr/issues/1061
df_e$n <- as.numeric(df_e$n)

# Fill
df_e <- tidyr::fill(df_e, n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'e. Summarised SEC data\n   (edgar_fndmntl_all_tb)', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


df_b <- dplyr::left_join(
  x = date_seq, 
  y = group_by(b_price_data, date_stamp) %>% summarise(n = n()) %>% filter(n > 750), 
  by = 'date_stamp'
  ) %>% 
  mutate(n = as.numeric(n)) %>% 
  tidyr::fill(n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'b. Share prices', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)

df_a <- dplyr::left_join(
  x = date_seq, 
  y =a_sp500_data, 
  by = 'date_stamp'
  ) %>% 
  mutate(n = as.numeric(1)) %>% 
  tidyr::fill(symbol, .direction = "up") %>% 
  filter(!is.na(symbol)) %>% 
  mutate(data_source = 'a. S&P 500 data', size = if_else(is.na(symbol),0,1)) %>% 
  select(date_stamp, data_source, size)


df_c <- dplyr::left_join(x = date_seq, y = c_price_rtn_data, by = 'date_stamp') %>% 
  mutate(n = as.numeric(n)) %>% 
  tidyr::fill(n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'c. Transformed price features\n   (return_attributes)', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


df_f <- dplyr::left_join(x = date_seq, y = f_fndmntl_ind_data, by = 'date_stamp') %>% 
  mutate(n = as.numeric(n)) %>% 
  tidyr::fill(n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'f. Transformed fundamentals\n   (fundamental_attributes)', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)

# Union
df <- bind_rows(df_d, df_e)
df <- bind_rows(df, df_b)
df <- bind_rows(df, df_a)
df <- bind_rows(df, df_c)
df <- bind_rows(df, df_f)

# Resize non-quarter end months for SEC raw data
cols <- c(
  'd. Raw SEC data \n   (edgar.sub)',
  'e. Summarised SEC data\n   (edgar_fndmntl_all_tb)'
  )
df$size <- ifelse(
  month(df$date_stamp) %in% c(1,2,4,5,7,8,10,11) & df$data_source %in% cols, 
  df$size * 2, 
  df$size * 5
  )

#df$data_source <- factor(df$data_source, levels = order(unique(df$data_source)))
#df$data_source <- factor(df$data_source, levels = unique(df$data_source))

p1 <- ggplot(df, aes(x = reorder(data_source, desc(data_source)), y = date_stamp)) + 
  geom_point(
    shape = 22, #21 
    fill = "black", #"lightgray"
    color = "black", 
    size = df$size
    ) + 
  scale_y_date(
    limits= c(ceiling_date(start_date, unit = "month")-1, ceiling_date(end_date, unit = "month")-1), 
    breaks = date_seq$date_stamp,
    date_labels = "%b", 
    minor_breaks = NULL
    ) +
  coord_flip() +
  labs(
    x = "", y = "",
    title = paste('Stock Master database status report: ', Sys.time(), sep = " "),
    caption = "Raw SEC data dates are the download file availability date"
  ) +
  theme(axis.text.y = element_text(hjust = 0)) 

p1





# Run to here --------------------------------------------------------------------------------------------------------------


saveRDS(p1, file = 'C:/Users/brent/Documents/VS_Code/postgres/postgres/p1.rda')


p <- readRDS('C:/Users/brent/Documents/VS_Code/postgres/postgres/p1.rda')
p

# USE THIS DATA FOR VISUALISATIONS IN DATABASE STATUS REPORT
# INCLUDE LIST OF THE UNIVERSE
fndmntl_attributes <- readRDS('C:/Users/brent/Documents/VS_Code/postgres/postgres/fndmntl_attributes.rda')
excluded <- fndmntl_attributes$excluded
missing_price <- fndmntl_attributes$missing_price
miss_summary <- fndmntl_attributes$miss_summary
desc_stats <- fndmntl_attributes$desc_stats
execution_time <- fndmntl_attributes$execution_time

price_attributes <- readRDS('C:/Users/brent/Documents/VS_Code/postgres/postgres/price_attributes.rda')
execution_time <- price_attributes$execution_time
missing_data <- price_attributes$missing_data
upload_data <- price_attributes$upload_data
















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





# --------------------------------------------------------------------------------------------------------------------------

library(romerb)
data("stock_data")
data <- stock_data
rm(stock_data)
write.csv(data, 'C:/Users/brent/Documents/stock_data.csv')
