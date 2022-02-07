/******************************************************************************
* 
* Union new and old price data
* 
* 
* 
******************************************************************************/

drop function access_layer.adj_price_union(text, date, date);

create or replace function access_layer.adj_price_union
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
			--from test.shareprices_daily_iex		-- TEST
			from iex.shareprices_daily		-- PROD
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
			--from test.shareprices_daily_acc			-- TEST
			from access_layer.shareprices_daily	-- PROD
			where date_stamp < start_date
			and symbol = sym
		) t1
	) t2
order by symbol, date_stamp desc;

end;
$$ language plpgsql stable;
