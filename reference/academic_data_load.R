# Libraries
library(data.table)
library(lubridate)

# Load data
d <- fread("C:/Users/brent/Documents/VS_Code/postgres/postgres/reference/signed_predictors_dl_wide.csv")
 
# Date stamp V1
# https://dachxiu.chicagobooth.edu/download/datashare.zip
# Convert integer to date & convert to prior month end (datashare.csv >> 'osap' table)
d[, date_stamp := strptime(DATE, format="%Y%m%d")]
d[, date_stamp := as.Date(floor_date(date_stamp, "month") - 1)]

# Date stamp V2
# https://www.openassetpricing.com/data/
# Convert integer to date & make month end (signed_predictors_dl_wide.csv >> 'eapvml' table)
d[, date_stamp := as.Date(paste0(as.character(yyyymm), '01'), format='%Y%m%d')]
d[, date_stamp := date_stamp %m+% months(1) - 1]


# Connect to stock_master db
library(DBI)
library(RPostgres)
library(jsonlite)

config <- jsonlite::read_json('C:/Users/brent/Documents/VS_Code/postgres/postgres/config.json')

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host      = 'localhost',
  port      = '5432',
  dbname    = 'stock_master',
  user      = 'postgres',
  password  = config$pg_password
)


# Create table with sample of data
db_write <- as.data.frame(d[permno == 14593, ])
dbWriteTable(con, Id(schema = "reference", table = "signed_predictors_dl_wide"), db_write)
dbSendQuery(conn = con, statement = "delete from reference.signed_predictors_dl_wide")


# https://stackoverflow.com/questions/62225835/fastest-way-to-upload-data-via-r-to-postgressql-12

URI <- sprintf("postgresql://%s:%s@%s:%s/%s", "postgres", config$pg_password, "localhost", "5432", "stock_master")
n <- 100000
w <- floor(nrow(d) / n)
r <- nrow(d) %% (n * w)

for (i in 1:(w+1)) {
  if (i == 1) {
    s <- 1
    e <- n
  } else if (i <= w) {
    s <- s + n
    e <- e + n
  } else {
    s <- s + n
    e <- e + r  
  }
  
  print(paste0(s, " to ", e))
  
  rng <- s:e
  
  fwrite(d[rng, ], "temp.csv")
  
  system(
    sprintf(
      "psql -U postgres -c \"\\copy %s from %s delimiter ',' csv header\" %s",
      "reference.signed_predictors_dl_wide", 
      sQuote("temp.csv", FALSE), 
      URI
    )
  )
}

DBI::dbDisconnect(con)

# Alternate
#system("psql -U postgres -d stock_master -c \"\\copy reference.datashare FROM 'temp.csv' delimiter ',' csv header\"")
