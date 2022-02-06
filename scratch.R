# Get edgar data
sql0 <- "select 'sub' as table, sec_qtr, count(*) as n from edgar.sub group by 1,2 order by 2 desc, 1 asc"
qry0 <- dbSendQuery(conn = con, statement = sql0) 
edgar_raw_data <- dbFetch(qry0)

# Function to convert string to date
qtr_to_date <- function(x){
  as.Date(ISOdate(
    ifelse(substr(x,6,6) == '4', as.numeric(substr(x,1,4))+1, substr(x,1,4)),
    ifelse(substr(x,6,6) == '4', 1, as.numeric(substr(x,6,6))*3+1),
    1
  ))-1
}

# Add date string
edgar_raw_data$date_stamp <- qtr_to_date(edgar_raw_data$sec_qtr)

# Get fundamental data
sql1 <- "select sec_qtr, count(*) as n from edgar.edgar_fndmntl_all_tb group by 1 order by 1 desc"
qry1 <- dbSendQuery(conn = con, statement = sql1) 
fndmntl_data <- dbFetch(qry1)

# Add date string
fndmntl_data$date_stamp <- qtr_to_date(fndmntl_data$sec_qtr)


# Get price data
sql2 <- "
	select 
	symbol
	,max(timestamp) as max_date
	from alpha_vantage.shareprices_daily 
	where 1 = 1
	and symbol != 'GSPC'
	group by symbol
  "
qry2 <- dbSendQuery(conn = con, statement = sql2) 
price_data <- dbFetch(qry2)


# Get price return data
sql3 <- "select date_stamp, count(*) as n from access_layer.return_attributes group by 1 order by 1 desc"
qry3 <- dbSendQuery(conn = con, statement = sql3) 
price_rtn_data <- dbFetch(qry3)


# Get fundamental indicator data
sql4 <- "select date_stamp, count(*) as n from access_layer.fundamental_attributes group by 1 order by 1 desc"
qry4 <- dbSendQuery(conn = con, statement = sql4) 
fndmntl_ind_data <- dbFetch(qry4)


# Find the data that is 1 January of the year prior to the max date 
#start_date <- as.Date(ISOdate(as.numeric(format(max(edgar_raw_data$date_stamp), "%Y")),1,1))
end_date <- lubridate::floor_date(Sys.Date(), unit='month')
start_date <- end_date %m+% months(-12) 

#df <- complete(edgar_raw_data, date_stamp = seq(from = start_date %m+% months(1), to = max(edgar_raw_data$date_stamp) %m+% months(1), by = "month") - 1)

# Sequence of dates to join to
date_seq <- data.frame(date_stamp = seq(from = start_date %m+% months(1), to = end_date %m+% months(1), by = "month") - 1)

# Join 
df0 <- dplyr::left_join(x = date_seq, y = edgar_raw_data, by = 'date_stamp')
#df <- merge(x = date_seq, y = edgar_raw_data, by = "date_stamp", all.x = TRUE)

# Int64 from database to integer type
# https://github.com/tidyverse/tidyr/issues/1061
df0$n <- as.numeric(df0$n)

# Fill
df0 <- tidyr::fill(df0, n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'edgar.num', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


# Join 
df1 <- dplyr::left_join(x = date_seq, y = fndmntl_data, by = 'date_stamp')
#df <- merge(x = date_seq, y = edgar_raw_data, by = "date_stamp", all.x = TRUE)

# Int64 from database to integer type
# https://github.com/tidyverse/tidyr/issues/1061
df1$n <- as.numeric(df1$n)

# Fill
df1 <- tidyr::fill(df1, n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'fndmntl_data', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)


df2 <- data.frame(
  date_stamp = seq(
    from = start_date, 
    to = lubridate::floor_date(max(price_data$max_date), unit='month'), by = "month") - 1
  ) %>% 
  mutate(data_source = 'shareprices_daily') %>% 
  filter(!is.na(n)) %>% 
  select(date_stamp, data_source) %>% 
  filter(date_stamp >= start_date)


df3 <- dplyr::left_join(x = date_seq, y = price_rtn_data, by = 'date_stamp') %>% 
  mutate(n = as.numeric(n)) %>% 
  tidyr::fill(n, .direction = "up") %>% 
  filter(!is.na(n)) %>% 
  mutate(data_source = 'price_return_data', size = if_else(is.na(n),0,1)) %>% 
  select(date_stamp, data_source, size)



# Union
df <- bind_rows(df0, df1)
df <- bind_rows(df, df2)



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
