/******************************************************************************
* 
* access_layer.tickers_to_update_fn
* 
* DESCRIPTION: 
* Function returning list of tickers for which price data update is required.
* Returns stocks selected from the "reference.fundamental_universe" table
* based on the "valid_year" column
* 
* ERRORS:
* None
* 
* TO DO:
* None
* 
******************************************************************************/

select * from access_layer.tickers_to_update_fn(valid_year_param => 2021, nonfin_cutoff => 950, fin_cutoff => 150);

create or replace function access_layer.tickers_to_update_fn

-- Parameter
(
	fin_cutoff 			int 	default 100
	,nonfin_cutoff 		int 	default 900
	,valid_year_param	int		default 2021
) 

returns table 
(

	symbol_				text
	,last_date_in_db_	date
	,last_adj_close_	numeric
	,last_eps_date_		date
)

language plpgsql immutable

as $$

begin

return query

select 
	rfnc.ticker as symbol
	,av.max_date as last_date_in_db
	,av."close" as last_adj_close
	,eps.max_date as last_eps_date
from 
	(
		select 
		tcsi.ticker 
		from reference.fundamental_universe fu
		inner join reference.ticker_cik_sic_ind tcsi 
		on fu.cik = tcsi.cik 
		where fu.valid_year = valid_year_param  -- Parameter
		and ( 
			(fu.fin_nonfin  = 'financial' and fu.combined_rank <= fin_cutoff) or 
			(fu.fin_nonfin != 'financial' and fu.combined_rank <= nonfin_cutoff)
			)
		and tcsi.delist_date = '9998-12-31'
	) rfnc
left join 
	(
		select 
		distinct on (symbol) symbol
		,date_stamp as max_date
		,"close"
		from access_layer.shareprices_daily
		order by 
		symbol
		,date_stamp desc
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
end; $$