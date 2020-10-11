/******************************************************************************
* 
* DESCRIPTION: access_layer.model_input
* Query combining monthly price and fudamental attributes for stocks in universe
* 
* TO DO:
* Join price data (to be replaced by technical indicators from python)
* Calculate marketcap (check python data retains unadjusted price)
* 
* ERRORS: 
*
* 
******************************************************************************/

select 
uts.* 
,efat.filed 
,efat.sec_qtr 
,efat.shares_os 
,efat.total_assets 
,efat.net_income_qtly 
from reference.universe_time_series_vw uts
left join edgar.edgar_fndmntl_all_tb efat
on uts.cik = efat.cik 
and uts.month_end 
	between (make_date(left(efat.sec_qtr,4)::int, right(efat.sec_qtr,1)::int * 3, 1) + interval '2 month')::date - 1 
	and 	(make_date(left(efat.sec_qtr,4)::int, right(efat.sec_qtr,1)::int * 3, 1) + interval '5 month')::date - 1 
where uts.cik = 20286
order by 
uts.cik
,uts.month_end 




select 
efat.* 
from edgar.edgar_fndmntl_all_tb efat 
where cik = 20286
