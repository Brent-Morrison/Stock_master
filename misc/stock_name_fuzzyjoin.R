# Packages
library(dplyr)
library(DBI)
library(RPostgres)
library(fuzzyjoin)
library(stringr)

# Connect
con <- dbConnect(
  RPostgres::Postgres(),
  host      = 'localhost',
  port      = '5432',
  dbname    = 'stock_master',
  user      = rstudioapi::askForPassword("User"),
  password  = rstudioapi::askForPassword("Password")
)

# Query for Edgar list
# The underlying data in the "company_tickers" table is from the SEC website
# https://www.sec.gov/files/company_tickers.json
# The inner join selects only large filers
qry <- dbSendQuery(
  conn = con, 
  "
  select 
  ct.cik_str
  ,ct.ticker
  ,ct.title as name 
  from edgar.company_tickers ct
  inner join (select distinct cik from edgar.sub where afs = '1-LAF') sb
  on ct.cik_str = sb.cik
  "
  )
edgar_raw1 <- dbFetch(qry)


# The 'sub' table with is used to references fundamental data provided by the SEC
# contains additional cik references not contained in the 'company_tickers' json file
# For example Disney is associated with cik's (1001039, 1744489) in the 'sub' 'table
# yet only cik 1744489 is contained in the 'company_tickers' json file.
qry <- dbSendQuery(
  conn = con, 
  "
  with t1 as (
  	select 
    distinct 
    cik
    ,upper(substring(instance, '[A-Za-z]{1,5}')) as ticker
    from edgar.sub
    where form in ('10-K', '10-Q')
    and afs = '1-LAF'
    and length(substring(instance, '[A-Za-z]{1,5}')) > 1
    and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FY'
    and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FORM'
    
    except 
    
    select 
    cik_str
    ,ticker
    from edgar.company_tickers
  )
  
  ,t2 as (
  select 
  distinct on (cik) cik
  ,upper(substring(instance, '[A-Za-z]{1,5}')) as ticker
  ,translate(name, '/,.,,', '') as name 
  from edgar.sub
  where form in ('10-K', '10-Q')
  and afs = '1-LAF'
  order by cik, period 
  )
  
  select 
  t1.cik as cik_str
  ,t1.ticker
  ,t2.name
  from t1
  left join t2
  on t1.cik = t2.cik
  "
)
edgar_raw2 <- dbFetch(qry)

# Query for Zacks list
qry <- dbSendQuery(
  conn = con, 
  "select ticker, company as name from zacks.zacks_gt_750"
  )
zacks_raw <- dbFetch(qry)

# Clean title
edgar <- bind_rows(edgar_raw1, edgar_raw2) %>% 
  mutate(
    name_clean = tolower(name),
    name_clean = str_replace_all(name_clean, c(
    "the " = "",
    "," = "",
    "\\." = "",
    "/.*"= "",   # note incorrect cleaning of "24/7 REAL MEDIA INC"
    "&" = "and",
    "limited" = "",
    "corporation" = "",
    " corp" = "",
    "company" = "",
    "\\b(co)\\b" = "",
    "incorporated" = "",
    "ltd" = "",
    "\\b(inc)\\b" = ""
    )),
    name_clean = trimws(name_clean)
  ) %>% 
  group_by(name) %>% 
  mutate(ticker_count = n()) %>% 
  ungroup

zacks <- zacks_raw %>% 
  mutate(
    name_clean = tolower(name),
    name_clean = str_replace_all(name_clean, c(
    "the " = "",
    "," = "",
    "\\." = "",
    "/.*" = "",
    "&" = "and",
    "limited" = "",
    "corporation" = "",
    " corp" = "",
    "company" = "",
    "\\b(co)\\b" = "",  
    # https://stackoverflow.com/questions/6713310/regex-specify-space-or-start-of-string-and-space-or-end-of-string
    # https://stackoverflow.com/questions/36183288/regex-difference-between-word-boundary-end-and-edge
    "incorporated" = "",
    "ltd" = "",
    "\\b(inc)\\b" = ""
    )),
    name_clean = trimws(name_clean)
  )

# Join by ticker 
hard_matched <- inner_join(
  x = edgar,
  #x = filter(edgar, ticker_count == 1),
  y = zacks,
  by = "ticker"
  #by = c("ticker", "name_clean")
  ) %>% 
  select(cik_str:name_clean.x) %>% 
  rename_all(list(~str_replace_all(., '.x', '')))


# Exclude hard matched tickers from dataframe to be used for fuzzy match
exclude_tck <- unlist(hard_matched$ticker)
exclude_name <- unlist(hard_matched$name_clean)

edgar_filtered <- edgar %>% 
  filter(!ticker %in% exclude_tck) %>% 
  filter(!name_clean %in% exclude_name)
zacks_filtered <- zacks %>% 
  filter(!ticker %in% exclude_tck) %>% 
  filter(!name_clean %in% exclude_name)


# Join by stock name
soft_matched <- stringdist_join(
  x = edgar_filtered, 
  y = zacks_filtered, 
  by = "name_clean",
  mode = "left",
  ignore_case = TRUE, 
  method = "jw", 
  max_dist = 99, 
  distance_col = "dist"
  ) %>%
  group_by(name.x) %>%
  slice_min(order_by = dist, n = 1, with_ties = FALSE)  %>% 
  mutate(
    filter = case_when(
      ticker.x == ticker.y & name_clean.x == name_clean.y ~ 1,
      ticker.x == ticker.y & dist < 0.1 ~ 1,
      substr(ticker.x, 1, 1) == substr(ticker.y, 1, 1) & dist < 0.1 ~ 1,
      TRUE ~ 0
      )
    )

# Join hard and soft matches
all_matched <- bind_rows(
  hard_matched,
  soft_matched %>% filter(filter == 1) %>% 
    group_by(ticker.y) %>% 
    slice_min(order_by = dist, n = 1, with_ties = FALSE) %>% 
    ungroup() %>% 
    select(cik_str, ticker.y, name.x:name_clean.x) %>% 
    rename_all(list(~str_replace_all(., '.x', ''))) %>% 
    rename_all(list(~str_replace_all(., '.y', '')))
  )

# Check multiple tickers and cik's
dupes <- all_matched %>% 
  group_by(ticker) %>% 
  mutate(dupe_tck = n()) %>% 
  ungroup() %>% 
  group_by(cik_str) %>% 
  mutate(dupe_cik = n()) %>% 
  ungroup() %>% 
  filter(dupe_tck > 1 | dupe_cik > 1)
  


# Save to csv
write.csv(all_matched, file = 'C:/Users/brent/Documents/VS_Code/postgres/postgres/all_matched.csv')

# Disconnect
dbDisconnect(con)


# Check - ZION, BROWN FORMAN, MOOG INC, Ameris Bancorp, BEMIS CO INC, Agnico Eagle Mines Limited
# American Campus Communities Inc, TFC/BBT
# cik in (23082,1688568,1001039,1744489,1363829) 
str = c("Agnico Eagle Mines Limited", 
  "Ameris Bancorp", 
  "AGCO Corporation", 
  "Shinhan Financial Group Co Ltd",
  "Hess Co",
  "Income Fund Inc")
str = tolower(str)
str_replace_all(str, "\\b(co)\\b", "")



date <- as.character(Sys.Date())
api_key <- 'J2MWHUOABDSEVS6P'
a <- 'active'
d <- 'delisted'

url_d <- paste(
  'https://www.alphavantage.co/query?function=LISTING_STATUS&date=',date,
  '&state=',d,
  '&apikey=',api_key,
  sep=""
  )

url_a <- paste(
  'https://www.alphavantage.co/query?function=LISTING_STATUS&date=',date,
  '&state=',a,
  '&apikey=',api_key,
  sep=""
  )

delisted <- read.csv(url_d) %>% mutate(capture_date = Sys.Date())
active <- read.csv(url_a) %>% mutate(capture_date = Sys.Date())