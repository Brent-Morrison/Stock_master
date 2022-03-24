:: WSL/bash :: /c/Windows/System32/cmd.exe /c C:/Users/brent/Documents/VS_Code/postgres/postgres/test/psql_test_do.bat "'eee'" "'2021-01-01'" "'2022-12-31'"
:: In order to use the above directly from bash (as opposed to from unqouted strings via Airflow variables), remove the quotes below.  Ie, "'%sym%'" becomes %sym%.
set database=%1
set sym=%2
set start_date=%3
set end_date=%4
cd C:\Program Files\PostgreSQL\12\bin\
psql -U postgres -d %database% -h localhost -p 5432 -U postgres -f C:\Users\brent\Documents\VS_Code\postgres\postgres\test\psql_test_do.sql -v sym="'%sym%'" -v start_date="'%start_date%'" -v end_date="'%end_date%'"
:: pause
