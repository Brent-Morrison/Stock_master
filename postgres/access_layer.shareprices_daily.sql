/******************************************************************************
* 
* access_layer.shareprices_daily
* 
* DESCRIPTION: 
* Make table for share price data
* 
* 
******************************************************************************/

create table access_layer.shareprices_daily (
	symbol text,
	date_stamp date,
	"open" numeric,
	high numeric,
	low numeric,
	"close" numeric,
	adjusted_close numeric,
	volume numeric,
	dividend_amount numeric,
	split_coefficient numeric,
	capture_date date,
	data_source varchar(5)
);
create unique index shareprices_daily_idx on access_layer.shareprices_daily using btree (symbol, date_stamp);


-- Seed with AV data
insert into access_layer.shareprices_daily
	(
		select 
		symbol
		,"timestamp" as date_stamp
		,"open"
		,high
		,low
		,"close"
		,adjusted_close
		,volume
		,dividend_amount
		,split_coefficient
		,capture_date 
		,data_source
		from 
			(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd
			) t1
		where row_num = 1
		order by 1,2
	)

	