/******************************************************************************
* 
* Example function - dynamic table selection
*  
* 
******************************************************************************/

drop function test.dynamic_table(text,date,date);

create or replace function test.dynamic_table
(
	test_bool		text default 'T'
	,start_date		date default '2021-12-31'
	,end_date		date default '2022-01-31'
) 

returns table 
(
	symbol 				text
	,date_stamp			date
	,"open"				numeric
	,high				numeric
	,low				numeric
	,"close"			numeric
	,volume				numeric
	,dividend_amount	numeric
	,split_coefficient 	numeric
	,capture_date 		date
	,data_source 		varchar(5)
	,prior_close		numeric
	,rtn_ari_1d_		numeric
	,start_date_		integer
) 

as $body$

declare
	iex_table_name text;
	acc_table_name text;

begin
	
if test_bool = 'T' then
	iex_table_name := 'test.shareprices_daily_iex'::regclass;
	acc_table_name := 'test.shareprices_daily_acc'::regclass;
else 
	iex_table_name := 'access_layer.shareprices_daily'::regclass;
	acc_table_name := 'access_layer.shareprices_daily'::regclass;
end if;


return query
	
execute format('
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
			from %1$s
			where date_stamp between %3$L and %4$L
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
			from %2$s
			where date_stamp < %3$L
		) t1
	) t2
order by symbol, date_stamp desc'
,iex_table_name
,acc_table_name
,start_date
,end_date 
);

end;
$body$ language plpgsql stable;


select * from test.dynamic_table('T', '2021-12-01', '2022-01-31') where symbol = 'AAPL'
select * from test.dynamic_table('F', '2021-12-01', '2022-01-31') where symbol = 'AAPL'

