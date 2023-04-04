/******************************************************************************
* 
* edgar.qrtly_fndmntl_ts_fn
* 
* DESCRIPTION: 
* Function to extract quarterly fundamental data for defined universe for year 
* under analysis.
* Used in "fndmntl_attributes.R" script
* 
* ERRORS:
* - None
* 
* TO DO:
* - NA
* 
******************************************************************************/

-- Test function
--select * from edgar.qrtly_fndmntl_ts_fn(valid_year_param_ => 2021, nonfin_cutoff_ => 925, fin_cutoff_ => 125) where ticker_ in ('ABG','AYI')
--drop function edgar.qrtly_fndmntl_ts_fn;

create or replace function edgar.qrtly_fndmntl_ts_fn

(
	fin_cutoff_ 		int 	default 100
	,nonfin_cutoff_ 	int 	default 900
	,valid_year_param_	int		default 2021
) 

returns table
(
	valid_year_				float8
	,date_available_		date
	,sector_				text
	,fin_nonfin_			text
	,ticker_				text
	,fiscal_year_			smallint
	,fiscal_period_			text
	,src_					text
	,report_date_			date
	,publish_date_			date
	,shares_cso_			numeric
	,shares_ecso_			numeric
	,cash_equiv_st_invest_	numeric
	,total_cur_assets_		numeric
	,intang_asset_			numeric
	,total_noncur_assets_	numeric
	,total_assets_			numeric
	,st_debt_				numeric
	,total_cur_liab_		numeric
	,lt_debt_				numeric
	,total_noncur_liab_		numeric
	,total_liab_			numeric
	,total_equity_			numeric
	,net_income_qtly_		numeric
)


language plpgsql immutable

as $$

begin

return query

with fndmntl as 
	(
		-- Simfin fundamental data
		select 
			ticker
			,fiscal_year
			,fiscal_period
			,'simfin' as src
			,report_date
			,publish_date
			,shares_basic as shares_cso
			,0 as shares_ecso
			,cash_equiv_st_invest
			,total_cur_assets
			,intang_asset
			,total_noncur_assets
			,total_assets
			,-1 * st_debt as st_debt
			,-1 * total_cur_liab as total_cur_liab
			,-1 * lt_debt as lt_debt
			,-1 * total_noncur_liab as total_noncur_liab
			,-1 * total_liab as total_liab
			,-1 * total_equity as total_equity
			,-1 * net_income as net_income_qtly
		
		from 
			simfin.smfn_fndmntl_vw
		where
			publish_date < '2016-12-31'
			
		union all
		
		-- Edgar fundamental data
		select 
			tik.ticker as ticker  -- JOIN TICKER FOR THIS
			,fy as fiscal_year
			,concat('Q', case when qtr = 'Y' then '4' else qtr end) as fiscal_period
			,'edgar' as src
			,ddate as report_date
			,filed as publish_date
			,shares_cso
			,shares_ecso
			,cash_equiv_st_invest
			,total_cur_assets
			,intang_asset
			,total_noncur_assets
			,total_assets
			,st_debt
			,total_cur_liab
			,lt_debt
			,total_noncur_liab
			,total_liab
			,total_equity
			,net_income_qtly
		
		from
			edgar.edgar_fndmntl_all_tb fnd
			left join 
				(
				select 
				distinct on (ticker) t.*
				from reference.ticker_cik_sic_ind t
				order by ticker, delist_date asc
				) tik 
			on fnd.cik = tik.cik 
	)
  
	,universe as 
	(	
		select * from reference.yearly_universe_fn
		(
		nonfin_cutoff => nonfin_cutoff_
		,fin_cutoff => fin_cutoff_
		,valid_year_param => valid_year_param_
		)
	)

select
	extract(year from universe.end_year) as valid_year
	,(date_trunc('quarter', publish_date) + interval '4 month - 1 day')::date as date_available
	,universe.sector
	,universe.fin_nonfin
	,fndmntl.*
from 
	fndmntl
	inner join universe
	on fndmntl.ticker = universe.symbol
	and fndmntl.publish_date between universe.start_date and universe.end_date
	and fndmntl.publish_date between universe.start_year and universe.end_year
	order by 5,2 
;
end; $$