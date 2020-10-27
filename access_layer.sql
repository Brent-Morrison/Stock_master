/******************************************************************************
* 
* access_layer.return_attributes
* 
* DESCRIPTION: 
* Create table to contain price return attributes
* 
* TO DO:
* 
* ERRORS: 
*
* 
******************************************************************************/
select * from access_layer.return_attributes

drop table if exists access_layer.return_attributes;

create table access_layer.return_attributes
	(
		symbol text
		, date_stamp date
		, close numeric
		, adjusted_close numeric
		, volume numeric
		, rtn_log_1m numeric
		, amihud_1m numeric
		, amihud_60d numeric
		, amihud_vol_60d numeric
		, vol_ari_20d numeric
		, vol_ari_60d numeric
		, vol_ari_120d numeric
		, skew_ari_120d numeric
		, kurt_ari_120d numeric
		, smax_20d numeric
		, cor_rtn_1d_mkt_120d numeric
		, beta_rtn_1d_mkt_120d numeric
		, rtn_ari_1m numeric
		, rtn_ari_3m numeric
		, rtn_ari_6m numeric
		, rtn_ari_12m numeric
		, fwd_rtn_1m numeric
		, rtn_ari_1m_dcl smallint
		, rtn_ari_3m_dcl smallint
		, rtn_ari_6m_dcl smallint
		, rtn_ari_12m_dcl smallint
		, amihud_1m_dcl smallint
		, amihud_60d_dcl smallint
		, amihud_vol_60d_dcl smallint
		, vol_ari_20d_dcl smallint
		, vol_ari_60d_dcl smallint
		, vol_ari_120d_dcl smallint
		, skew_ari_120d_dcl smallint
		, kurt_ari_120d_dcl smallint
		, smax_20d_dcl smallint
		, cor_rtn_1d_mkt_120d_dcl smallint
		, beta_rtn_1d_mkt_120d_dcl smallint
	);

alter table access_layer.return_attributes owner to postgres;





/******************************************************************************
* 
* access_layer.model_input
* 
* DESCRIPTION: 
* Query combining monthly price and fundamental attributes for stocks in universe
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
