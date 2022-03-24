/******************************************************************************
* 
* UPDATE PROCEDURES
* 
* 
* 
******************************************************************************/

do
$$
declare
    f record;
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
			,dividend_amount
			--from test.shareprices_daily_iex 	-- TEST
			from iex.shareprices_daily 		-- PROD
			where date_stamp between '2022-02-01' and '2022-02-28'
			
			except 
			
			select 
			symbol
			,date_stamp 
			,split_coefficient
			,dividend_amount
			--from test.shareprices_daily_acc 	-- TEST
			from access_layer.shareprices_daily 		-- PROD
			where date_stamp between '2022-02-01' and '2022-02-28'
			) t1
		group by 1
		order by 1
    loop 
		
    	-- call insert function
    	call access_layer.insert_adj_price(f.symbol, '2022-02-01', '2022-02-28');

    end loop;
end;
$$


-- Update checks
select * from access_layer.shareprices_daily where date_stamp = '2022-01-31'

select symbol, capture_date, min(date_stamp), max(date_stamp) 
from access_layer.shareprices_daily 
where date_stamp > '2021-10-31'
group by 1,2
order by 1,2 desc






/******************************************************************************
* 
* PROCEDURES FOR TESTING FUNCTIONS AND RESETTING TEST DATA
* 
* 
* 
******************************************************************************/

alter table test.shareprices_daily_acc add constraint shareprices_daily_acc_con unique (symbol, date_stamp);

call test.insert_adj_price('ABBV', '2021-12-01', '2022-01-31');

-- 1. Reset adjusted close to dummy value (99)
update test.shareprices_daily_acc 
set adjusted_close = 99
where symbol in (select distinct symbol from (
	select 
	symbol
	,avg(split_coefficient) as split_ind
	,sum(dividend_amount) as div_ind
	from test.shareprices_daily_iex 	-- TEST
	--from iex.shareprices_daily 		-- PROD
	where date_stamp between '2021-12-01' and '2022-01-31'
	group by 1
	having avg(split_coefficient) != 1
	or sum(dividend_amount) != 0
	) t1);

-- Delete data appended
delete from test.shareprices_daily_acc where date_stamp between '2021-12-01' and '2022-01-31';

-- Check
select * from test.shareprices_daily_acc 
where 1= 1 --symbol not in ('AAP','AAT','AAN','A','ABBV') 
order by symbol, date_stamp desc;

select symbol, min(date_stamp), max(date_stamp) 
from access_layer.shareprices_daily sd 
--from test.shareprices_daily_acc 
group by 1 order by 1;






/******************************************************************************
* 
* INSERT ADJUSTED PRICE TO ACCESS_LAYER
* 
* 
* 
******************************************************************************/

drop procedure test.insert_adj_price();

create or replace procedure test.insert_adj_price
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
	-- (this loop is redundant with symbol filter, retain for convenience)
	for r in
		select 
		symbol
		,avg(split_coefficient) as split_ind
		,sum(dividend_amount) as div_ind
		from test.shareprices_daily_iex 	-- TEST
		--from iex.shareprices_daily 		-- PROD
		where date_stamp between start_date and end_date
		and symbol = sym
		group by 1
		order by 1
		
	loop
		if (r.split_ind != 1 or r.div_ind != 0) then 
			-- there has been a split or dividend, therefore update adjusted close
			raise notice 'insert and update for : %', r.symbol;
			
			insert into test.shareprices_daily_acc  			-- TEST
			--insert into access_layer.shareprices_daily  		-- PROD
			select
			_symbol				--as symbol
			,_date_stamp		--as date_stamp
			,_open				--as "open"
			,_high				--as high
			,_low				--as low
			,_close				--as "close"
			,_adjusted_close	--as adjusted_close
			,_volume			--as volume
			,_dividend_amount	--as dividend_amount
			,_split_coefficient	--as split_coefficient
			,_capture_date 		--as capture_date
			,_data_source 		--as data_source
			from test.adj_price_fn(r.symbol, start_date, end_date)
			on conflict on constraint shareprices_daily_acc_con 			-- TEST
			--on conflict on constraint access_layer.shareprices_daily_con 	-- PROD 
			do update set adjusted_close = excluded.adjusted_close;
			
		else
			-- there has NOT been a split or dividend, adjusted close update NOT required
			-- insert directly from IEX table
			raise notice 'insert only for : %', r.symbol;
			
			insert into test.shareprices_daily_acc  			-- TEST
			--insert into access_layer.shareprices_daily  		-- PROD
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
			from test.shareprices_daily_iex						-- TEST
			--from iex.shareprices_daily						-- PROD
			where symbol = r.symbol 
			and date_stamp between start_date and end_date
			on conflict on constraint shareprices_daily_acc_con
			do nothing;
	
		end if;
	
	end loop;
	
end;
$body$ language plpgsql;


call test.insert_adj_price('2021-12-01', '2022-01-31');






/******************************************************************************
* 
* CALCULATE ADJUSTED PRICE
* 
* 
* 
******************************************************************************/

drop function test.adj_price_fn(text,date,date);

-- Create type for use in loop
create or replace type test.adj_price_fn_loop as 
(
	symbol_ 			text
	,date_stamp_		date
	,open_				numeric
	,high_				numeric
	,low_				numeric
	,close_				numeric
	,volume_			numeric
	,dividend_amount_	numeric
	,split_coefficient_ numeric
	,capture_date_ 		date
	,data_source_ 		varchar(5)
	,prior_close_		numeric
	,rtn_ari_1d_		numeric
	,start_date_		integer
);


-- Function
create or replace function test.adj_price_fn
(
	sym 			text default 'XOM'
	,start_date		date default '2021-12-31'
	,end_date		date default '2022-01-31'
) 

returns table 
(
	_symbol 			text
	,_date_stamp		date
	,_open				numeric
	,_high				numeric
	,_low				numeric
	,_close				numeric
	,_adjusted_close	numeric
	,_volume			numeric
	,_dividend_amount	numeric
	,_split_coefficient numeric
	,_capture_date 		date
	,_data_source 		varchar(5)
) as 

$body$

declare
 	r 			test.adj_price_fn_loop%ROWTYPE;
 	next_rtn	numeric;
	next_close	numeric;

begin

	for r in
		select * 
		from test.adj_price_union(sym, start_date, end_date)
		--from test.adj_price_vw			-- NOTE 1. this needs to contain new and existing data, use new function
		--where symbol_ = sym 
	
	loop
		if r.start_date_ = 1 then
			_symbol 			:= r.symbol_;
			_date_stamp			:= r.date_stamp_;
			_open				:= r.open_;
			_high				:= r.high_;
			_low				:= r.low_;
			_close				:= r.close_;
			_adjusted_close		:= r.close_;
			_volume				:= r.volume_;
			_dividend_amount	:= r.dividend_amount_;
			_split_coefficient 	:= r.split_coefficient_;
			_capture_date 		:= r.capture_date_;
			_data_source 		:= r.data_source_;

		else
			_symbol 			:= r.symbol_;
			_date_stamp			:= r.date_stamp_;
			_open				:= r.open_;
			_high				:= r.high_;
			_low				:= r.low_;
			_close				:= r.close_;
			_adjusted_close		:= next_close / (1 + next_rtn);  
			_volume				:= r.volume_;
			_dividend_amount	:= r.dividend_amount_;
			_split_coefficient 	:= r.split_coefficient_;
			_capture_date 		:= r.capture_date_;
			_data_source 		:= r.data_source_;
		end if;
		
		return next;
		
		next_rtn 	:= r.rtn_ari_1d_;
		next_close	:= _adjusted_close;

	end loop;
	
	return;

end;
$body$ language plpgsql stable;


select * from test.adj_price_fn('ABBV','2021-12-31','2022-01-31');





/******************************************************************************
* 
* Union new and old price data
* 
* 
* 
******************************************************************************/

drop function test.adj_price_union(text, date, date);

create or replace function test.adj_price_union
(
	sym 			text default 'XOM'
	,start_date		date default '2021-12-31'
	,end_date		date default '2022-01-31'
) 

returns table 
(
	symbol_ 			text
	,date_stamp_		date
	,open_				numeric
	,high_				numeric
	,low_				numeric
	,close_				numeric
	,volume_			numeric
	,dividend_amount_	numeric
	,split_coefficient_ numeric
	,capture_date_ 		date
	,data_source_ 		varchar(5)
	,prior_close_		numeric
	,rtn_ari_1d_		numeric
	,start_date_		integer
) 

as $$

begin

return query

select 
t2.*
,(close - ((prior_close/split_coefficient)*((prior_close/split_coefficient)-dividend_amount) / (prior_close/split_coefficient)))
	/     ((prior_close/split_coefficient)*((prior_close/split_coefficient)-dividend_amount) / (prior_close/split_coefficient)) as rtn_ari_1d_ 
,case when lead("close",1) over (partition by symbol order by date_stamp) is null then 1 else 0 end as start_date_
from 
	(
		select 
		t1.* 
		,lag("close",1) over (partition by symbol order by date_stamp) as prior_close
		from 
		(
			select * 
			from test.shareprices_daily_iex		-- TEST
			--from iex.shareprices_daily		-- PROD
			where date_stamp between start_date and end_date
			and symbol = sym
			union all 
			select
			symbol
			,date_stamp
			,"open"
			,high
			,low
			,"close"
			,volume
			,dividend_amount
			,split_coefficient
			,capture_date 
			,data_source 
			from test.shareprices_daily_acc			-- TEST
			--from access_layer.shareprices_daily	-- PROD
			where date_stamp < start_date
			and symbol = sym
		) t1
	) t2
order by symbol, date_stamp desc;

end;
$$ language plpgsql stable;



select * from test.adj_price_union('ABBV', '2021-12-01', '2022-01-31')
