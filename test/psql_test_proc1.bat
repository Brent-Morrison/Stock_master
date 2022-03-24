:: https://stackoverflow.com/questions/6523019/postgresql-scripting-psql-execution-with-password
:: https://stackoverflow.com/questions/35805973/setting-pgpassword-environment-variable-for-postgres-psql-process-executed-by
:: from WSL/bash :: /c/Windows/System32/cmd.exe /c C:/Users/brent/Documents/VS_Code/postgres/postgres/test/psql_test_proc1.bat "'EEE'" "'2000-01-01'" "'2022-12-31'"
cd C:\Program Files\PostgreSQL\12\bin
psql -U postgres -d stock_master_test -c "call test.psql_test_proc('aaa', '2020-12-31', '2022-12-31');"
pause