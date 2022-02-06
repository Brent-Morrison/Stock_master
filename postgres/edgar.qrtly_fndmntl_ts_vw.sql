/******************************************************************************
* 
* edgar.qrtly_fndmntl_ts_vw
* 
* DESCRIPTION: 
* Join simfin and edgar data to export to R / python.
* Will be used to seed "access_layer.fundamental_universe" table.
* 
* ERRORS:
* 
* TO DO:
* - assess if the industry reference in "ind_ref" CTE should reference the simfin industry
*   (same as for alpha_vantage.daily_price_ts_view)
* - does this need to include a valid year indicator so that when results are returned
*   for the burn in period, these are flagged appropriately (ie., for exclusion from analysis)
*   (same as for edgar.qrtly_fndmntl_ts_vw)
* 
******************************************************************************/
	
--Test
select * from edgar.qrtly_fndmntl_ts_vw where ticker in ('AAPL','BGNE','EBIX','TUSK') order by 5,2 

create or replace view edgar.qrtly_fndmntl_ts_vw as 

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
			left join reference.ticker_cik_sic_ind tik
			on fnd.cik = tik.cik 
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
	(	
		select 
			t.ticker 
			,t.sic
			,i.sector
			,i.fin_nonfin
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
		where 
			(i.fin_nonfin = 'financial' and f.combined_rank < 100) or
			(i.fin_nonfin != 'financial' and f.combined_rank < 900) 
		order by
			t.ticker 
			,f.valid_year
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
	on fndmntl.ticker = universe.ticker
	and fndmntl.publish_date between universe.start_date and universe.end_date
	and fndmntl.publish_date between universe.start_year and universe.end_year
	order by 4,9 
;
