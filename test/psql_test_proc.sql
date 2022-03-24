/******************************************************************************
* 
* TEST CALLING  PROCEDURES
* Bash call from WSL
* /c/Windows/System32/cmd.exe /c C:/Users/brent/Documents/VS_Code/postgres/postgres/test/psql_test_proc.bat "'EEE'" "'2021-01-01'" "'2022-12-31'"
* 
* 
******************************************************************************/

call test.psql_test_proc(sym => :sym, start_date => :start_date, end_date => :end_date);
--call test.psql_test_proc(sym => 'zzz', start_date => '2018-01-01', end_date => '2019-01-01');

--select * from test.test_table
--truncate test.test_table;