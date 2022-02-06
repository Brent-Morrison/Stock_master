/******************************************************************************
* 
* alpha_vantage.daily_price_ts_fn
* 
* DESCRIPTION:
* Function to extract monthly price data for defined universe for year under analysis
* 
* https://www.endpoint.com/blog/2008/12/11/why-is-my-function-slow
* https://stackoverflow.com/questions/35914518/postgres-function-slower-than-query-postgres-8-4 
* 
* 
* TO DO:
* - assess if the industry reference in "ind_ref" CTE should reference the simfin industry
*   (same as for edgar.qrtly_fndmntl_ts_vw), as opposed to the sic category
* - see note around parameters, create SQl function in R to alleviate (Postgres function slow)
* - make the "prices" CTE into a table, truncate and re-populate after each price update
*  
******************************************************************************/

select * from alpha_vantage.daily_price_ts_fn(valid_year_param => 2021, nonfin_cutoff => 100, fin_cutoff => 50) where symbol_ in ('AAPL')

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
		date_stamp as "timestamp"
		,symbol
		,close
		,close as adjusted_close
		,volume
		from access_layer.shareprices_daily
		where symbol != 'GSPC'
	)

,sp_500 as 
	(
		select 
		"timestamp",
	    adjusted_close as sp500
	    from access_layer.shareprices_daily
		where symbol = 'GSPC'
    )

,ind_ref as 
	(	-- 
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		--,lk.lookup_val5 as industry
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

