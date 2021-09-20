
# --------------------------------------------------------------------------------------------------------------------------
# Stock master data update loop
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

# Loop loading multiple years
dates = c('2020-12-31')

counter <- 0

for (date in dates) {
  
  result_set <- price_attributes(
    end_date = date, 
    con = con,
    months_to_load = 12, 
    write_to_db = TRUE, 
    disconnect = FALSE, 
    return_df = TRUE
  )
  
  counter <- counter + 1
  
  if (counter == 1) {
    result_set_agg <- result_set$missing_data
  } else {
    result_set_agg <- rbind(result_set_agg$missing_data, result_set$missing_data)
  }
  
}


# Interrogate data uploaded
upload_data <- result_set$upload_data
missing_data <- result_set_agg$missing_data
result_set$execution_time
