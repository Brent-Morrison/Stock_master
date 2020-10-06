/******************************************************************************
* 
* DESCRIPTION: alpha_vantage.ticker_excl
* Make table for stocks to exclude from price download script
* Updated from script in "alphavantage_import.py"
* 
* 
******************************************************************************/

drop table if exists alpha_vantage.ticker_excl cascade;
			
create table alpha_vantage.ticker_excl			
	(		
		ticker	text
		,last_date_in_db date
		,price	numeric
		,last_av_date date
	);		
			
alter table alpha_vantage.ticker_excl owner to postgres;




/******************************************************************************
* 
* DESCRIPTION: alpha_vantage.active_delisted
* Make table for active and delisted stocks
* Updated from script in "alphavantage_import.py"
* 
* 
******************************************************************************/

select * from alpha_vantage.active_delisted

drop table if exists alpha_vantage.active_delisted cascade;
			
create table alpha_vantage.active_delisted			
	(
		symbol			text
		,name			text
		,exchange		text
		,ipo_date		date
		,delist_date	date
		,status			text
		,capture_date	date
	);		
			
alter table alpha_vantage.active_delisted owner to postgres;




/******************************************************************************
* 
* DESCRIPTION: alpha_vantage.tickers_to_update
* Create view returning list of tickers for which price data update is required
* 
* ERRORS
* Where cause could be entered as parameter
* 
******************************************************************************/

select * from alpha_vantage.tickers_to_update;

create or replace view alpha_vantage.tickers_to_update as 

	select 
		rfnc.ticker as symbol
		,max(av.max_date) as last_date_in_db
		,max(av.adjusted_close) as last_adj_close
	from 
		(
			select distinct ticker 
			from reference.universe_time_series_vw 
			where extract(year from month_end) = 2020
		) rfnc
	left join 
		(
			select 
			distinct on (symbol) symbol
			,timestamp as max_date
			,adjusted_close
			from alpha_vantage.shareprices_daily
			order by 
			symbol
			,timestamp desc
		) av
	on rfnc.ticker = av.symbol
	--where rfnc.ticker not in (select distinct ticker from alpha_vantage.ticker_excl) 
	group by 1
;




/******************************************************************************
* 
* DESCRIPTION: alpha_vantage.returns_view
* Create view to extract price data for porting to Python for technical indicator 
* creation.  Usd by Python script "return_attributes.py"
*  
* ERRORS
* To be amended to take account of multiple records for same day,
* use distinct on()
* 
* Change join from SP500 table to innerjoin on "reference.universe_time_series_vw"
*  
******************************************************************************/

select * from alpha_vantage.returns_view where symbol = 'A'

create or replace view alpha_vantage.returns_view as 

	select 
	sd.symbol,
	sp5.gics_sector,
	sd."timestamp",
	sd.close,
	sd.adjusted_close,
	sd.volume,
	sp.sp500
	from alpha_vantage.shareprices_daily sd
	join alpha_vantage.sp_500 sp5 
	on sd.symbol = sp5.symbol
	left join 
		(
			select 
			"timestamp",
		    adjusted_close as sp500
		    from alpha_vantage.shareprices_daily
		    where symbol = 'GSPC'
	    ) sp 
	on sd."timestamp" = sp."timestamp"
	where sd."timestamp" > '2018-01-01'::date
	order by 
	sd.symbol, 
	sd."timestamp", 
	sp5.gics_sector
;


