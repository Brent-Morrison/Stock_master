/******************************************************************************
* 
* DESCRIPTION: smfn_fndmntl_vw
* Create view for simfin fundamental data.
* This view is to be unioned with egdar data produced 
* with the view "smfn_edg_fndmntl_qy" query below.
* 
* ERRORS:
* Simfin data incorrect for CRWS, publish date 2012-08-14 data agrees to SEC filing
* adsh = 0001437749-12-008459, yet the Simfin report date is 2011-07-31 while the SEC
* has this as 2012-07-01
* 
* CMA has identical publish dates for a number of report dates
* 
******************************************************************************/

select * from simfin.smfn_fndmntl_vw

drop view simfin.smfn_fndmntl_vw;

create or replace view simfin.smfn_fndmntl_vw as 

	select 
	bal.ticker 
	,bal.simfin_id 
	,bal.fiscal_year
	,bal.fiscal_period
	,bal.report_date
	-- Correct instances of publish date less than report date, or
	-- signifcantly greater than the report date
	,case 
		when bal.publish_date < bal.report_date then bal.report_date + 45
		when bal.report_date + 100 < bal.publish_date then bal.report_date + 45
		else bal.publish_date end 		as publish_date 
	,bal.shares_basic / 1e6 			as shares_basic
	,bal.cash_equiv_st_invest / 1e6 	as cash_equiv_st_invest
	,bal.total_cur_assets / 1e6			as total_cur_assets
	,0 									as intang_asset
	,bal.total_noncur_assets / 1e6 		as total_noncur_assets
	,bal.total_assets / 1e6 			as total_assets
	,bal.st_debt / 1e6 					as st_debt
	,bal.total_cur_liab / 1e6 			as total_cur_liab
	,bal.lt_debt / 1e6 					as lt_debt
	,bal.total_noncur_liab / 1e6 		as total_noncur_liab
	,bal.total_liab / 1e6 				as total_liab
	,bal.total_equity / 1e6 			as total_equity
	,inc.net_income / 1e6				as net_income
	from simfin.us_balance_qtly bal
	inner join simfin.us_income_qtly inc
	on bal.ticker = inc.ticker 
	and bal.simfin_id = inc.simfin_id
	and bal.fiscal_year = inc.fiscal_year
	and bal.fiscal_period = inc.fiscal_period
	
	union all 
	
	select 
	bal.ticker 
	,bal.simfin_id 
	,bal.fiscal_year
	,bal.fiscal_period
	,bal.report_date
	-- Correct instances of publish date less than report date, or
	-- signifcantly greater than the report date
	,case 
		when bal.publish_date < bal.report_date then bal.report_date + 45
		when bal.report_date + 100 < bal.publish_date then bal.report_date + 45
		else bal.publish_date end 		as publish_date 
	,bal.shares_basic / 1e6 			as shares_basic
	,bal.cash_equiv_st_invest / 1e6 	as cash_equiv_st_invest
	,0 									as total_cur_assets
	,0 									as intang_asset
	,0 									as total_noncur_assets
	,bal.total_assets / 1e6 			as total_assets
	,bal.st_debt / 1e6 					as st_debt
	,0 									as total_cur_liab
	,bal.lt_debt / 1e6 					as lt_debt
	,0 									as total_noncur_liab
	,bal.total_liab / 1e6 				as total_liab
	,bal.total_equity / 1e6 			as total_equity
	,inc.net_income / 1e6 				as net_income
	from simfin.us_balance_banks_qtly bal
	inner join simfin.us_income_banks_qtly inc
	on bal.ticker = inc.ticker 
	and bal.simfin_id = inc.simfin_id
	and bal.fiscal_year = inc.fiscal_year
	and bal.fiscal_period = inc.fiscal_period
	
	union all 
	
	select 
	bal.ticker 
	,bal.simfin_id 
	,bal.fiscal_year
	,bal.fiscal_period
	,bal.report_date
	-- Correct instances of publish date less than report date, or
	-- signifcantly greater than the report date
	,case 
		when bal.publish_date < bal.report_date then bal.report_date + 45
		when bal.report_date + 100 < bal.publish_date then bal.report_date + 45
		else bal.publish_date end 		as publish_date 
	,bal.shares_basic / 1e6 			as shares_basic
	,bal.cash_equiv_st_invest / 1e6 	as cash_equiv_st_invest
	,0 									as total_cur_assets
	,0 									as intang_asset
	,0 									as total_noncur_assets
	,bal.total_assets / 1e6 			as total_assets
	,bal.st_debt / 1e6 					as st_debt
	,0 									as total_cur_liab
	,bal.lt_debt / 1e6 					as lt_debt
	,0 									as total_noncur_liab
	,0 									as total_liab 
	,bal.total_equity / 1e6 			as total_equity
	,inc.op_income / 1e6 				as net_income
	from simfin.us_balance_ins_qtly bal
	inner join simfin.us_income_ins_qtly inc
	on bal.ticker = inc.ticker 
	and bal.simfin_id = inc.simfin_id
	and bal.fiscal_year = inc.fiscal_year
	and bal.fiscal_period = inc.fiscal_period
	


	
/******************************************************************************
* 
* DESCRIPTION: smfn_edg_fndmntl_qy
* Join simfin and edgar data to seed "access_layer.fundamental_universe" table
* up to 2020.
* This table is manipulated in the "cik_ticker_fndmntly_univ.xlsx" 
* excel file prior to being loaded.
* 
* 
******************************************************************************/

with sf_all as 
	(
		select * from simfin.smfn_fndmntl_vw
	)

,sf_ind_lk as 
	(
		select 	
		lookup_ref as industry_id
		,case 
			when lookup_val1 = 'Financial Services' then 'financial'
			else 'non_financial'
			end as fin_nonfin
		from edgar.lookup
		where lookup_table = 'simfin_industries'
	)

,sf_edgar as 
	(
		-- Simfin fundamantal data
		select
			sf_all.ticker
			,999999 as cik
			,coalesce(sf_ind_lk.fin_nonfin, 'non_financial') as fin_nonfin
			,concat(extract (year from sf_all.publish_date), 'q', extract (quarter from sf_all.publish_date)) as sec_qtr
			,total_assets
			,total_equity * -1 as total_equity
		from 
			sf_all
		left join
			simfin.us_companies usc
			on sf_all.simfin_id = usc.simfin_id 
		left join
			sf_ind_lk
			on usc.industry_id = sf_ind_lk.industry_id::int
		where 
			extract (quarter from sf_all.publish_date) = 3
		
		union all	
			
		-- Edgar fundamental data
		select 
			'XXXX' as ticker
			,cik
			,fin_nonfin 
			,sec_qtr 
			,total_assets
			,total_equity
		from 
			edgar.edgar_fndmntl_t1
		where
			sec_qtr like '%q3'
			and filed between '2018-12-31' and '2020-06-30'
	)

,t1 as (	
	select 
	sf_edgar.*
	,rank() over (partition by sec_qtr, fin_nonfin order by total_assets desc) 	as asset_rank
	,rank() over (partition by sec_qtr, fin_nonfin order by total_equity asc) 	as equity_rank
	from sf_edgar
	)

,t2 as (	
	select
	t1.*
	,asset_rank + equity_rank 													as sum_rank
	from t1
	)

,t3 as (
	select 
	t2.*
	,rank() over (partition by sec_qtr, fin_nonfin order by sum_rank asc) 		as combined_rank
	from t2
	)

select
ticker
,cik
,cast(substring(sec_qtr,1, 4) as int) + 1 as valid_year
,fin_nonfin 
,total_assets
,total_equity
,combined_rank
--,t3.*
from t3
where 	(	(combined_rank <= 1350 and fin_nonfin = 'non_financial'	)
		or 	(combined_rank <= 150  and fin_nonfin = 'financial')	)
order by 
valid_year 
,fin_nonfin 
,combined_rank
