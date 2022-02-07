/******************************************************************************
* 
* INSERT ADJUSTED PRICE TO ACCESS_LAYER
* 
* 
* 
******************************************************************************/

alter table access_layer.shareprices_daily add constraint shareprices_daily_acc_con unique (symbol, date_stamp);

create or replace procedure access_layer.insert_adj_price
(
	sym 			text default 'XOM'
	,start_date		date default '2021-12-31'
	,end_date		date default '2021-01-31'
) as 

$body$

declare
 	r				record;

begin

	-- loop over the newly added data
	for r in
		select 
		symbol
		,avg(split_coefficient) as split_ind
		,sum(dividend_amount) as div_ind
		--from test.shareprices_daily_iex 	-- TEST
		from iex.shareprices_daily 		-- PROD
		where date_stamp between start_date and end_date
		and symbol = sym
		group by 1
		order by 1
		
	loop
		if (r.split_ind != 1 or r.div_ind != 0) then 
			-- there has been a split or dividend, therefore update adjusted close
			raise notice 'insert and update for : %', r.symbol;
			
			--insert into test.shareprices_daily_acc  			-- TEST
			insert into access_layer.shareprices_daily  		-- PROD
			select
			_symbol
			,_date_stamp
			,_open
			,_high
			,_low
			,_close	
			,_adjusted_close
			,_volume
			,_dividend_amount
			,_split_coefficient
			,_capture_date
			,_data_source
			from access_layer.adj_price_fn(r.symbol, start_date, end_date)
			on conflict on constraint shareprices_daily_acc_con 			-- TEST
			do update set adjusted_close = excluded.adjusted_close;
			
		else
			-- there has NOT been a split or dividend, adjusted close update NOT required
			-- insert directly from IEX table
			raise notice 'insert only for : %', r.symbol;
			
			--insert into test.shareprices_daily_acc  			-- TEST
			insert into access_layer.shareprices_daily  		-- PROD
			select 
			symbol
			,date_stamp
			,open
			,high
			,low
			,"close"
			,"close" as adjusted_close
			,volume
			,dividend_amount
			,split_coefficient
			,capture_date
			,data_source 
			--from test.shareprices_daily_iex						-- TEST
			from iex.shareprices_daily						-- PROD
			where symbol = r.symbol 
			and date_stamp between start_date and end_date
			on conflict on constraint shareprices_daily_acc_con
			do nothing;
	
		end if;
	
	end loop;
	
end;
$body$ language plpgsql;
