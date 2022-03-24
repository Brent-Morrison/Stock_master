/******************************************************************************
* 
* CALCULATE ADJUSTED PRICE
* Selects the price data set returned from the "access_layer.adj_price_union"
* function encompassing new and existing data.
* 
* Calculates the adjusted close using the split and dividend adjusted
* 1 day arithmetic return ingested with the "access_layer.adj_price_union"
* function.
* 
* The result of this function is ingested into the "access_layer.insert_adj_price"
* stored procedure.
* 
******************************************************************************/

select * from access_layer.adj_price_fn(sym => 'AAPL', start_date => '2022-02-01', end_date => '2022-02-28')

drop function access_layer.adj_price_fn(text,date,date);

-- Create type for use in loop
create type access_layer.adj_price_fn_loop as 
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
create or replace function access_layer.adj_price_fn
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
 	r 			access_layer.adj_price_fn_loop%ROWTYPE;
 	next_rtn	numeric;
	next_close	numeric;

begin

	for r in
		select * from access_layer.adj_price_union(sym, start_date, end_date)
	
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