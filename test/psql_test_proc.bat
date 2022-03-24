:: https://stackoverflow.com/questions/6523019/postgresql-scripting-psql-execution-with-password
:: https://stackoverflow.com/questions/35805973/setting-pgpassword-environment-variable-for-postgres-psql-process-executed-by
:: https://stackoverflow.com/questions/7389416/postgresql-how-to-pass-parameters-from-command-line/10337507#10337507

set sym=%1
set start_date=%2
set end_date=%3
cd C:\Program Files\PostgreSQL\12\bin
psql -U postgres -d stock_master -v sym="'%sym%'" -v start_date="'%start_date%'" -v end_date="'%end_date%'" -f "C:\Users\brent\Documents\VS_Code\postgres\postgres\test\psql_test_proc.sql"
:: pause