set database=stock_master_test
set database=%1
set start_date=%2
set end_date=%3
cd C:\Program Files\PostgreSQL\12\bin\
psql -U postgres -d %database% -h localhost -p 5432 -U postgres -f C:\Users\brent\Documents\VS_Code\postgres\postgres\airflow\adj_price_insert_sc.sql -v start_date="'%start_date%'" -v end_date="'%end_date%'"
