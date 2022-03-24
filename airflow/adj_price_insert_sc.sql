/******************************************************************************
* 
* UPDATE SCRIPT
* For calling from Airflow
* 
* 
******************************************************************************/

set start_date.z to :start_date;
set end_date.z to :end_date;

do
$$
declare
    f record;
  	start_date date := current_setting('start_date.z');
	end_date date := current_setting('end_date.z');
begin
    for f in 
		select 
		symbol
		,sum(split_coefficient)
		from 
			(
			select 
			symbol
			,date_stamp 
			,split_coefficient
			--from test.shareprices_daily_iex 			-- TEST
			from iex.shareprices_daily 					-- PROD
			--where date_stamp between '2022-02-01' and '2022-02-28'
			where date_stamp between start_date and end_date
			
			except 
			
			select 
			symbol
			,date_stamp 
			,split_coefficient
			--from test.shareprices_daily_acc 			-- TEST
			from access_layer.shareprices_daily 		-- PROD
			--where date_stamp between '2022-02-01' and '2022-02-28'
			where date_stamp between start_date and end_date
			) t1
		group by 1
		order by 1
    loop 
		
    	-- call insert function
    	call access_layer.insert_adj_price(f.symbol, start_date, end_date);

    end loop;
end;
$$
