/******************************************************************************
* 
* access_layer.monthly_price_ts_fn
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
* - SEE NOTE RE "CREATES A DUPE RE ALXN, TWO RECORDS IN THIS TABLE AFTER DELIST"
*  
******************************************************************************/

select * from access_layer.monthly_price_ts_fn(valid_year_param_ => 2021, nonfin_cutoff_ => 900, fin_cutoff_ => 100);

drop function access_layer.monthly_price_ts_fn;

create or replace function access_layer.monthly_price_ts_fn

(
	fin_cutoff_ 		int 	default 100
	,nonfin_cutoff_ 	int 	default 900
	,valid_year_param_	int		default 2021
) 

returns table 
(
	symbol_ 			text
	,sector_ 			text
	,industry_ 			text
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
		date_stamp
		,symbol
		,close
		,adjusted_close
		,volume
		from access_layer.shareprices_daily
		where symbol != 'GSPC'
	)

,sp_500 as 
	(
		select 
		date_stamp
	    ,adjusted_close as sp500
	    from access_layer.shareprices_daily spd
	    inner join 
			(
				select 
				max(date_stamp) as last_trade_date
				from access_layer.shareprices_daily
				where symbol = 'GSPC'
				group by date_trunc('month', date_stamp) 	
			) ltd
	    on spd.date_stamp = ltd.last_trade_date
		where symbol = 'GSPC'
    )
	
,universe as 
	(	
		select * 
		from reference.yearly_universe_fn
		(
		nonfin_cutoff => nonfin_cutoff_
		,fin_cutoff => fin_cutoff_
		,valid_year_param => valid_year_param_
		)

	)
	
select
	prices.symbol
	,universe.sector
	,universe.industry
	,prices.date_stamp 
	,prices."close"
	,prices.adjusted_close
	,prices.volume
	,sp_500.sp500
	,case when extract(year from prices.date_stamp) = universe.valid_year then 'valid' else 'invalid' end as valid_year_ind
from 
	prices
	inner join universe
	on prices.symbol = universe.symbol
	and prices.date_stamp between universe.start_date and universe.end_date
	and prices.date_stamp between universe.start_year and universe.end_year
	inner join sp_500
	on prices.date_stamp = sp_500.date_stamp
order by 1,4 

;
end; $$
