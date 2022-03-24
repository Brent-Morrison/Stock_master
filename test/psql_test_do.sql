/******************************************************************************
* 
* TEST CALLING  PROCEDURES
* Bash call from WSL
* /c/Windows/System32/cmd.exe /c C:/Users/brent/Documents/VS_Code/postgres/postgres/test/psql_test_do.bat "'eee'" "'2021-01-01'" "'2022-12-31'"
* 
******************************************************************************/

set sym.z to :sym;
set start_date.z to :start_date;
set end_date.z to :end_date;

do
$$
declare
 	r				record;
	sym text := current_setting('sym.z');
	start_date date := current_setting('start_date.z');
	end_date date := current_setting('end_date.z');
begin

	-- loop over the newly added data
	for r in
		select 
		symbol, 
		max(date_stamp) 
		from iex.shareprices_daily 
		group by 1 
		order by 1 desc 
		limit 10
		
	loop
		if length(r.symbol) > 4 then 

			insert into test.test_table
			values (r.symbol, 1, start_date, 99, 99, 9999, 1000);
			
		else
			insert into test.test_table
			values (concat(sym,r.symbol), 2, end_date, 99, 99, 9999, 2000);
	
		end if;
	
	end loop;
	
end;
$$


--select * from test.test_table
--truncate test.test_table;