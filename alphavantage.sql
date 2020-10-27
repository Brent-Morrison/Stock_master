/******************************************************************************
* 
* alpha_vantage.ticker_excl
* 
* DESCRIPTION: 
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
* alpha_vantage.active_delisted
* 
* DESCRIPTION: 
* Make table for active and delisted stocks.
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
* alpha_vantage.tickers_to_update
* 
* DESCRIPTION: 
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
* alpha_vantage.daily_price_ts_view
* 
* DESCRIPTION:
* Create view to extract price data for porting to Python for technical indicator 
* creation. 
* 
* Used by Python script "return_attributes.py"
* 
* TO DO:
* - Universe CTE, rolling calc. burn in period required
* 
*  
******************************************************************************/

-- Test
select * 
from alpha_vantage.daily_price_ts_vw 
where symbol in ('A','AAL')--,'AAN','AAWW','ABM','ACCO','ACM','AAPL','ADBE','ADI','ADT','AKAM','AMD') 
and date_stamp > '2013-01-01'



create index shareprices_daily_idx on alpha_vantage.shareprices_daily (symbol, "timestamp")

create or replace view alpha_vantage.daily_price_ts_vw as 

with prices as 
	(
		select 
		"timestamp"
		,symbol
		,close
		,adjusted_close
		,volume
		from 
			(
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd 
				where 1 = 1 --"timestamp" > '2018-01-01'
			) t1
		where row_num = 1
		and symbol != 'GSPC'
	)

,sp_500 as 
	(
		select 
		"timestamp",
	    adjusted_close as sp500
	    from alpha_vantage.shareprices_daily
	    where symbol = 'GSPC'
	    --and "timestamp" > '2018-01-01'
    )

,ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case 
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
,universe as 
	(	-- Need to add a preceding year to the CTE to allow for rolling calc. burn in period
		select 
			t.ticker 
			,t.sic
			,i.sector
			,t.ipo_date as start_date
			,t.delist_date as end_date
			,make_date(f.valid_year,1,1) as start_year 
			,make_date(f.valid_year,12,31) as end_year
		from 
			reference.fundamental_universe f
			left join reference.ticker_cik_sic_ind t
			on f.cik = t.cik
			left join ind_ref i
			on t.ticker = i.ticker
		where 
			(i.fin_nonfin = 'financial' and f.combined_rank < 100) or
			(i.fin_nonfin != 'financial' and f.combined_rank < 900) 
		order by
			t.ticker 
			,f.valid_year
	)
	
select
	prices.symbol
	,universe.sector
	,prices."timestamp" as date_stamp 
	,prices."close"
	,prices.adjusted_close
	,prices.volume
	,sp_500.sp500
from 
	prices
	inner join universe
	on prices.symbol = universe.ticker
	and prices."timestamp" between universe.start_date and universe.end_date
	and prices."timestamp" between universe.start_year and universe.end_year
	left join sp_500
	on prices."timestamp" = sp_500."timestamp"
	order by 1,3 
;





/******************************************************************************
* 
* alpha_vantage.monthly_price_ts_view
* 
* DESCRIPTION:
* Create view to extract last monthly price data for porting to Python / R for 
* fundamental valuation models.
* 
* Used by R script "?????.r"
* 
* TO DO:
* - Universe CTE, rolling calc. burn in period required
* 
*  
******************************************************************************/

-- Test
select * from alpha_vantage.monthly_price_ts_vw where symbol = 'A'

create or replace view alpha_vantage.monthly_price_ts_vw as 

	select 
		symbol
		,date_stamp 
		,"close" 
		,adjusted_close 
		,volume
	from 
		alpha_vantage.daily_price_ts_vw dpts
		inner join 
			(
				select 
				max("timestamp") as last_trade_date
				from alpha_vantage.shareprices_daily
				where symbol = 'GSPC'
				group by date_trunc('month', "timestamp") 
				order by max("timestamp") 	
			) ltd
		on dpts.date_stamp = ltd.last_trade_date
	order by 1,2
;



	
	
/******************************************************************************
* 
* alpha_vantage.price_duplicates
* 
* DESCRIPTION:
* Return stocks with data that has been downloaded more than once
* 
*  
******************************************************************************/

select 
distinct symbol 
from 
	(
		select 
		symbol
		,"timestamp"
		,count(*) as record_count
		,max(adjusted_close)
		,min(adjusted_close) 
		from alpha_vantage.shareprices_daily 
		where 1 = 1 --"timestamp" > '2020-06-30' 
		group by 1,2 
		having count(*) > 1
		and max(adjusted_close) = min(adjusted_close)
	) t1

	

	


	
	
/******************************************************************************
* 
* SCRATCH
*  
******************************************************************************/
select * from (

with ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case 
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
select 
	t.ticker 
	,t.sic
	,i.sector
	,t.ipo_date as start_date
	,t.delist_date as end_date
	-- minus 1 as we need a burn in period for 1 yr returns
	,make_date(f.valid_year-1,1,1) as start_year 
	,make_date(f.valid_year,12,31) as end_year
	,i.fin_nonfin  --remove
from 
	reference.fundamental_universe f
	left join reference.ticker_cik_sic_ind t
	on f.cik = t.cik
	left join ind_ref i
	on t.ticker = i.ticker
where 
		(i.fin_nonfin = 'financial' and f.combined_rank < 10)
	or 	(i.fin_nonfin != 'financial' and f.combined_rank < 900) -- CHECK
order by
	f.valid_year
	,t.ticker
) t1 
where sector = '5' and end_year = '2018-12-31'