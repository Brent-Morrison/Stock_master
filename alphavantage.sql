/******************************************************************************
* 
* alpha_vantage.earnings
* 
* DESCRIPTION: 
* Make table for earnings / eps data from alphavantage
* Updated from script in "alphavantage_import.py"
* 
* 
******************************************************************************/

--truncate alpha_vantage.earnings;

select count(*) from alpha_vantage.earnings

create table alpha_vantage.earnings			
	(		
		symbol             text
		,date_stamp        date
		,report_date       date
		,reported_eps      numeric
		,estimated_eps     numeric
		,eps_surprise      numeric
		,eps_surprise_perc numeric
		,capture_date      date
	);		
			
alter table alpha_vantage.earnings owner to postgres;





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

select * from alpha_vantage.ticker_excl

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
* ERRORS:
* Where clause could be entered as parameter
* 
* TO DO:
* - "fu.valid_year = 2020" could be entered as parameter
* 
******************************************************************************/

select * from alpha_vantage.tickers_to_update;

create or replace view alpha_vantage.tickers_to_update as 

	select 
		rfnc.ticker as symbol
		,av.max_date as last_date_in_db
		,av.adjusted_close as last_adj_close
		,eps.max_date as last_eps_date
	from 
		(
			select 
			tcsi.ticker 
			from reference.fundamental_universe fu
			inner join reference.ticker_cik_sic_ind tcsi 
			on fu.cik = tcsi.cik 
			where fu.valid_year = 2020
			and tcsi.delist_date = '9998-12-31'
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
	left join
		(
			select 
			symbol
			,max(date_stamp) as max_date
			from alpha_vantage.earnings
			group by 1
		) eps
	on rfnc.ticker = eps.symbol
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
* - assess if the industry reference in "ind_ref" CTE should reference the simfin industry
*   (same as for edgar.qrtly_fndmntl_ts_vw)
* - see note around parameters, create SQl function in R to alleviate (Postgres function slow)
*  
******************************************************************************/

-- TEST
-- EBIX=814549, rank will exclude in 2020
-- TUSK=1679268 is valid only for 2019
-- BGNE=1651308 rank will include from 2019

select * 
from alpha_vantage.daily_price_ts_vw 
where 1 = 1 
and symbol in ('ADT','BGNE','EBIX','KOSN','TUSK') 
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
			(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd 
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
    )

,ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case -- see TO DO
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
,universe as 
	(	
		select 
			t.ticker 
			,t.sic
			,i.sector
			--,i.fin_nonfin
			,f.valid_year 
			,t.ipo_date as start_date
			,t.delist_date as end_date
			,case 
				when lag(f.valid_year) over (partition by t.ticker order by f.valid_year) is null then make_date(f.valid_year-2,1,1) 
				else make_date(f.valid_year,1,1) 
				end as start_year
			,make_date(f.valid_year,12,31) as end_year
		from 
			reference.fundamental_universe f
			left join reference.ticker_cik_sic_ind t
			on f.cik = t.cik
			left join ind_ref i
			on t.ticker = i.ticker
		where ( -----------------------------------------------------------------------------
			(i.fin_nonfin = 'financial' and f.combined_rank < 100) or --- parameter here ----
			(i.fin_nonfin != 'financial' and f.combined_rank < 900)   --- parameter here ----
			) -------------------------------------------------------------------------------
			and f.valid_year = 2019 ------------------------------------- parameter here ----
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
	,case when extract(year from prices."timestamp") = universe.valid_year then 'valid' else 'invalid' end as valid_year_ind
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
* alpha_vantage.daily_price_ts_fn
* 
* DESCRIPTION:
* Create function to extract monthly price data for defined universe 
* 
* https://www.endpoint.com/blog/2008/12/11/why-is-my-function-slow
* https://stackoverflow.com/questions/35914518/postgres-function-slower-than-query-postgres-8-4 
* 
* 
* TO DO:
* - assess if the industry reference in "ind_ref" CTE should reference the simfin industry
*   (same as for edgar.qrtly_fndmntl_ts_vw)
* - see note around parameters, create SQl function in R to alleviate (Postgres function slow)
*  
******************************************************************************/

select * from alpha_vantage.daily_price_ts_fn(valid_year_param => 20120, nonfin_cutoff => 10, fin_cutoff => 10)
where symbol_ in ('ADT','BGNE','EBIX','KOSN','TUSK')

create or replace function alpha_vantage.daily_price_ts_fn

(
	fin_cutoff 			int 	default 100
	,nonfin_cutoff 		int 	default 900
	,valid_year_param	int		default 2021
) 

returns table 
(
	symbol_ 			text
	,sector_ 			text
	,date_stamp_		date
	,"close_"			numeric
	,adjusted_close_	numeric
	,volume_			numeric
	,sp500_				numeric
	,valid_year_ind_ 	text
)

language plpgsql immutable

as $$

begin

return query

with prices as 
	(
		select 
		"timestamp"
		,symbol
		,close
		,adjusted_close
		,volume
		from 
			(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd 
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
    )

,ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case -- see TO DO
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
,universe as 
	(	
		select 
			t.ticker 
			,t.sic
			,i.sector
			,f.valid_year 
			,t.ipo_date as start_date
			,t.delist_date as end_date
			,case 
				when lag(f.valid_year) over (partition by t.ticker order by f.valid_year) is null then make_date(f.valid_year-2,1,1) 
				else make_date(f.valid_year,1,1) 
				end as start_year
			,make_date(f.valid_year,12,31) as end_year
		from 
			reference.fundamental_universe f
			left join reference.ticker_cik_sic_ind t
			on f.cik = t.cik
			left join ind_ref i
			on t.ticker = i.ticker
		where ( 
			(i.fin_nonfin  = 'financial' and f.combined_rank <= fin_cutoff) or 
			(i.fin_nonfin != 'financial' and f.combined_rank <= nonfin_cutoff)
			)
			and f.valid_year = valid_year_param
		--order by
		--	t.ticker 
		--	,f.valid_year
	)
	
select
	prices.symbol
	,universe.sector
	,prices."timestamp" as date_stamp 
	,prices."close"
	,prices.adjusted_close
	,prices.volume
	,sp_500.sp500
	,case when extract(year from prices."timestamp") = universe.valid_year then 'valid' else 'invalid' end as valid_year_ind
from 
	prices
	inner join universe
	on prices.symbol = universe.ticker
	and prices."timestamp" between universe.start_date and universe.end_date
	and prices."timestamp" between universe.start_year and universe.end_year
	inner join sp_500
	on prices."timestamp" = sp_500."timestamp" 
order by 1,3 

;
end; $$





/******************************************************************************
* 
* alpha_vantage.monthy_price_ts_fn
* 
* DESCRIPTION:
* Create function to extract monthly price data for defined universe 
* 
* https://www.endpoint.com/blog/2008/12/11/why-is-my-function-slow
* https://stackoverflow.com/questions/35914518/postgres-function-slower-than-query-postgres-8-4
* 
* 
* TO DO:
* - assess if the industry reference in "ind_ref" CTE should reference the simfin industry
*   (same as for edgar.qrtly_fndmntl_ts_vw)
*  
******************************************************************************/

select * from alpha_vantage.monthly_price_ts_fn(valid_year_param => 2017, nonfin_cutoff => 10, fin_cutoff => 10)
where symbol_ in ('ADT','BGNE','EBIX','KOSN','TUSK')

create or replace function alpha_vantage.monthly_price_ts_fn

(
	fin_cutoff 			int 	default 100
	,nonfin_cutoff 		int 	default 900
	,valid_year_param	int		default 2021
) 

returns table 
(
	symbol_ 			text
	,sector_ 			text
	,date_stamp_		date
	,"close_"			numeric
	,adjusted_close_	numeric
	,volume_			numeric
	,sp500_				numeric
	,valid_year_ind_ 	text
)

language plpgsql immutable

as $$

begin

return query

with prices as 
	(
		select 
		"timestamp"
		,symbol
		,close
		,adjusted_close
		,volume
		from 
			(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd 
			) t1
		where row_num = 1
		and symbol != 'GSPC'
	)

,sp_500 as 
	(
		select 
		"timestamp",
	    adjusted_close as sp500
	    from alpha_vantage.shareprices_daily spd
	    inner join 
			(
				select 
				max("timestamp") as last_trade_date
				from alpha_vantage.shareprices_daily
				where symbol = 'GSPC'
				group by date_trunc('month', "timestamp") 	
			) ltd
	    on spd."timestamp" = ltd.last_trade_date
		where symbol = 'GSPC'
    )

,ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case -- see TO DO
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
,universe as 
	(	
		select 
			t.ticker 
			,t.sic
			,i.sector
			,f.valid_year 
			,t.ipo_date as start_date
			,t.delist_date as end_date
			,case 
				when lag(f.valid_year) over (partition by t.ticker order by f.valid_year) is null then make_date(f.valid_year-2,1,1) 
				else make_date(f.valid_year,1,1) 
				end as start_year
			,make_date(f.valid_year,12,31) as end_year
		from 
			reference.fundamental_universe f
			left join reference.ticker_cik_sic_ind t
			on f.cik = t.cik
			left join ind_ref i
			on t.ticker = i.ticker
		where ( 
			(i.fin_nonfin  = 'financial' and f.combined_rank <= fin_cutoff) or 
			(i.fin_nonfin != 'financial' and f.combined_rank <= nonfin_cutoff)
			)
			and f.valid_year = valid_year_param
	)
	
select
	prices.symbol
	,universe.sector
	,prices."timestamp" as date_stamp 
	,prices."close"
	,prices.adjusted_close
	,prices.volume
	,sp_500.sp500
	,case when extract(year from prices."timestamp") = universe.valid_year then 'valid' else 'invalid' end as valid_year_ind
from 
	prices
	inner join universe
	on prices.symbol = universe.ticker
	and prices."timestamp" between universe.start_date and universe.end_date
	and prices."timestamp" between universe.start_year and universe.end_year
	inner join sp_500
	on prices."timestamp" = sp_500."timestamp" 
order by 1,3 

;
end; $$




/******************************************************************************
* 
* alpha_vantage.daily_sp500_ts_view
* 
* DESCRIPTION:
* Create view to extract S&P500 data for porting to Python for technical indicator 
* creation. 
* 
* Used by Python script "return_attributes.py"
* 
* TO DO:
*  
******************************************************************************/

-- Test
select * from alpha_vantage.daily_sp500_ts_vw where extract(year from date_stamp) = 2019

create or replace view alpha_vantage.daily_sp500_ts_vw as 

		select 
		"timestamp" as date_stamp
		,symbol
		,close
		,adjusted_close
		,volume
		from 
			(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd 
			) t1
		where row_num = 1
		and symbol = 'GSPC'




/******************************************************************************
* 
* alpha_vantage.monthly_price_ts_view
* 
* DESCRIPTION:
* Create view to extract last monthly price data for porting to Python / R for 
* fundamental valuation models.
* 
* Used by R script "fndmntl_attributes.r"
* 
* TO DO:
* - Universe CTE, rolling calc. burn in period required
* 
*  
******************************************************************************/

-- Test
select * from alpha_vantage.monthly_price_ts_vw where symbol = 'ABG'

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
* alpha_vantage.monthly_fwd_rtn
* 
* DESCRIPTION:
* Create table to create forward returns for dependent variable.
* 
* TO DO:
* - Write to table with population limited to only "valid" stocks for the year in question
*   Prior returns do not change so this can be static for all dates except the current year.
*   How to update to overwrite the most recent dates NULL's?
*   This table can then be used as a filter for the population to be predicted
* 
******************************************************************************/

-- Test
select *  from alpha_vantage.monthly_fwd_rtn where date_stamp = '2015-01-31' and sector = '7'
select date_stamp, count(*) as record_count from alpha_vantage.monthly_fwd_rtn group by 1


drop table alpha_vantage.monthly_fwd_rtn;

create table alpha_vantage.monthly_fwd_rtn as 

with prices as 
	(
		select 
		"timestamp"
		,symbol
		,adjusted_close
		,(lead(adjusted_close, 1) over (partition by symbol order by "timestamp") - adjusted_close) / nullif(adjusted_close,0) as fwd_rtn_1m
		,(lead(adjusted_close, 3) over (partition by symbol order by "timestamp") - adjusted_close) / nullif(adjusted_close,0) as fwd_rtn_3m
		from 
			(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd 
				inner join 
					(  -- Last trade date in month only
						select 
						max("timestamp") as last_trade_date
						from alpha_vantage.shareprices_daily
						where symbol = 'GSPC'
						group by date_trunc('month', "timestamp") 
						order by max("timestamp") 	
					) ltd
				on sd."timestamp" = ltd.last_trade_date
			) t1
		where row_num = 1
	)

,ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case 
			when lk.lookup_val1 = 'Financial Services' then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
,universe as 
	(	
		select 
			t.ticker 
			,t.sic
			,i.sector
			,t.ipo_date as start_date
			,t.delist_date as end_date
			,f.valid_year
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
	,(date_trunc('month', prices."timestamp") + interval '1 month - 1 day')::date as date_stamp 
	,prices.adjusted_close
	,prices.fwd_rtn_1m
	,prices.fwd_rtn_3m
	,ntile(10) over (partition by "timestamp" order by fwd_rtn_1m) as fwd_rtn_1m_dcl
	,ntile(10) over (partition by "timestamp" order by fwd_rtn_3m) as fwd_rtn_3m_dcl
	,ntile(10) over (partition by "timestamp", sector order by fwd_rtn_1m) as fwd_rtn_1m_sctr_dcl
	,ntile(10) over (partition by "timestamp", sector order by fwd_rtn_3m) as fwd_rtn_3m_sctr_dcl
from 
	prices
	inner join universe
	on prices.symbol = universe.ticker
	and extract(year from prices."timestamp") = universe.valid_year
where 
	prices."timestamp" >= '2012-01-01'
	--prices.symbol in ('ANF','BGNE','EBIX','TUSK','SANP') -- for test
order by 1,3 
;



	


/******************************************************************************
* 
* alpha_vantage.splits_vw
* 
* DESCRIPTION:
* List all stocks which have been the subject of a share split 
* 
* 
*  
******************************************************************************/

-- Test
select * from alpha_vantage.splits_vw 
where symbol in ('AAPL','AGCL','CHDN','GRMN','EQT','HLF','FAST')

--View
create or replace view alpha_vantage.splits_vw as 
	
	select 
	symbol 
	,"timestamp" as date_stamp
	,(date_trunc('month', "timestamp")  + interval '1 month')::date -1 as me_date
	,split_coefficient as split_coef
	from 
		(
			select 
			sd.* 
			,row_number() over (partition by "timestamp", symbol order by capture_date asc) as row_num
			from alpha_vantage.shareprices_daily sd 
		) t1
	where row_num = 1
	and split_coefficient != 1 
	and symbol != 'GSPC' 
	order by symbol, "timestamp"
;






/******************************************************************************
* 
* alpha_vantage.price_duplicates
* 
* DESCRIPTION:
* Delete stocks with data that has been downloaded more than once
* 
*  
******************************************************************************/

-- This ignores the order of the capture date
delete select * from alpha_vantage.shareprices_daily dupes
where exists 
	(
		select from alpha_vantage.shareprices_daily
		where ctid < dupes.ctid
		and symbol = dupes.symbol
		and "timestamp" = dupes."timestamp"
		and adjusted_close = dupes.adjusted_close
		and volume = dupes.volume
  	)
 ;

-- Dupes
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
* Alternate "monthly_price_ts_view"
*  
******************************************************************************/

select 
symbol 
,date_stamp 
,me_date 
,"close" 
,adjusted_close 
,case
	when coef_partition = 0 then 1
 	else max(split_coef) over (partition by symbol, coef_partition) 
 	end as cum_split_coef
from 
	(
		select 
			dpts.symbol
			,dpts.date_stamp 
			,(date_trunc('month', dpts.date_stamp)  + interval '1 month')::date -1 as me_date
			,dpts."close" 
			,dpts.adjusted_close 
			,dpts.volume
			,split.split_coef
			,sum(case when split.split_coef is null then 0 else 1 end) over (partition by dpts.symbol order by dpts.date_stamp asc) as coef_partition
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
			left join 
				(
					select 
					symbol 
					,"timestamp" as date_stamp
					,(date_trunc('month', "timestamp")  + interval '1 month')::date -1 as me_date
					,split_coefficient as split_coef
					--,sum(split_coefficient) over (partition by symbol) as split_coef
					from 
						(
							select 
							sd.* 
							,row_number() over (partition by "timestamp", symbol order by capture_date asc) as row_num
							from alpha_vantage.shareprices_daily sd 
							where 1 = 1 --"timestamp" > '2018-01-01'
						) t1
					where row_num = 1
					and split_coefficient != 1 
					and symbol != 'GSPC' 
					and symbol in ('AAPL','AGCL','CHDN','GRMN','EQT','HLF','FAST')
					order by symbol, "timestamp"
				) split
			on (date_trunc('month', dpts.date_stamp)  + interval '1 month')::date -1 = split.me_date
			and dpts.symbol = split.symbol
		where dpts.symbol in ('AAPL','AGCL','CHDN','GRMN','EQT','HLF','FAST') --- REMOVE
		order by 1,2
	) t1
order by 1,2

-- Examples of adjusted close and close data
select * from alpha_vantage.shareprices_daily where symbol = 'AAPL' and "timestamp" between '2014-06-01' and '2014-06-30'
select * from alpha_vantage.shareprices_daily where symbol = 'FAST' and "timestamp" between '2019-05-01' and '2019-05-31'
select * from alpha_vantage.shareprices_daily where symbol = 'CHDN' and "timestamp" between '2019-01-01' and '2019-01-31'



