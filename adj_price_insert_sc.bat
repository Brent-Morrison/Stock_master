set database=stock_master_test
set sym=%1
set start_date=%2
REM set end_date=%4
cd C:\Program Files\PostgreSQL\12\bin\
psql -U postgres -d %database% -h localhost -p 5432 -U postgres -f C:\Users\brent\Documents\VS_Code\postgres\postgres\test\test.call_procedure.sql -v sym=%sym% -v start_date=%start_date% -v end_date='2022-12-31'
REM psql -U postgres -d %database% -h localhost -p 5432 -U postgres -f C:\Users\brent\Documents\VS_Code\postgres\postgres\postgres\access_layer.adj_price_insert_sc.sql -v sym=%sym% -v start_date='2021-12-31' -v end_date='2022-12-31'
pause
