/******************************************************************************
* 
* simfin.smfn_fndmntl_vw
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
	
