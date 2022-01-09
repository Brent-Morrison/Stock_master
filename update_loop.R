
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
dates <- c('2021-11-30')
upload_data_list <- list()
missing_data_list <- list()
timing_list <- list()

#counter <- 0

for (date in dates) {
  
  result_set <- price_attributes(
    end_date = date, 
    con = con,
    months_to_load = 11, 
    write_to_db = TRUE, 
    disconnect = FALSE, 
    return_df = TRUE
  )
  
  #counter <- counter + 1
  #if (counter == 1) {
  #  result_set_agg <- result_set$missing_data
  #} else {
  #  result_set_agg <- rbind(result_set_agg$missing_data, result_set$missing_data)
  #}
  
  # Insert data frame results of "price_attributes" function into lists 
  upload_data_list[[date]] <- result_set$upload_data
  missing_data_list[[date]] <- result_set$missing_data
  timing_list[[date]] <- result_set$execution_time
  
}


# Data frames in lists to one data frame
upload_data <- bind_rows(upload_data_list)
missing_data <- bind_rows(missing_data_list)
timing_data <- bind_rows(timing_list)


# Interrogate data uploaded
upload_data <- result_set$upload_data
missing_data <- result_set_agg$missing_data
result_set$execution_time
