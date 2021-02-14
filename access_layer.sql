/******************************************************************************
* 
* access_layer.fundamental_attributes
* 
* DESCRIPTION: 
* Create table to contain fundamental attributes
* 
* TO DO:
* 
* ERRORS: 
*
* 
******************************************************************************/
select * from access_layer.fundamental_attributes

drop table if exists access_layer.fundamental_attributes;

create table access_layer.fundamental_attributes
	(
		date_stamp date
		, sector smallint
		, ticker text
		, valid_year numeric
		, fin_nonfin text
		, fiscal_year smallint
		, fiscal_period text
		, src text
		, report_date date
		, publish_date date
		, shares_cso numeric
		, shares_ecso numeric
		, cash_equiv_st_invest numeric
		, total_cur_assets numeric
		, intang_asset numeric
		, total_noncur_assets numeric
		, total_assets numeric
		, st_debt numeric
		, total_cur_liab numeric
		, lt_debt numeric
		, total_noncur_liab numeric
		, total_liab numeric
		, total_equity numeric
		, net_income_qtly numeric
		, cash_ratio numeric
		, ttm_earnings numeric
		, ttm_earnings_max numeric
		, asset_growth numeric
		, roa numeric
		, roe numeric
		, leverage numeric
		, other_ca_ratio numeric
		, intang_ratio numeric
		, close numeric
		, adjusted_close numeric
		, volume numeric
		, split_adj_ind numeric
		, split_adj numeric
		, shares_os numeric
		, mkt_cap numeric
		, book_price numeric
		, ttm_earn_yld numeric
		, ttm_earn_yld_max numeric
		, log_pb numeric
		, pbroe_rsdl_ols numeric
		, pbroe_rsq_ols numeric
		, pbroe_rsdl_ts numeric
		, residual numeric
		, fnmdl_rsdl_ts numeric
		, pbroe_rsdl_ols_rnk smallint
		, pbroe_rsdl_ts_rnk smallint
		, book_price_rnk smallint
		, ttm_earn_yld_rnk smallint
		, fnmdl_rsdl_ts_rnk smallint
		, pbroe_rsdl_ols_z numeric
		, pbroe_rsdl_ts_z numeric
		, book_price_z numeric
		, ttm_earn_yld_z numeric
		, fnmdl_rsdl_ts_z numeric
		, agg_valuation numeric
	);

alter table access_layer.fundamental_attributes owner to postgres;





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
select count(*) from access_layer.return_attributes

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
		, sector smallint
		, suv numeric
		, ipc numeric
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
		, rtn_ari_1m_sctr_dcl smallint
		, rtn_ari_3m_sctr_dcl smallint
		, rtn_ari_6m_sctr_dcl smallint
		, rtn_ari_12m_sctr_dcl smallint
		, amihud_1m_sctr_dcl smallint
		, amihud_60d_sctr_dcl smallint
		, amihud_vol_60d_sctr_dcl smallint
		, vol_ari_20d_sctr_dcl smallint
		, vol_ari_60d_sctr_dcl smallint
		, vol_ari_120d_sctr_dcl smallint
		, skew_ari_120d_sctr_dcl smallint
		, kurt_ari_120d_sctr_dcl smallint
		, smax_20d_vdcl smallint
		, cor_rtn_1d_mkt_120d_sctr_dcl smallint
		, beta_rtn_1d_mkt_120d_sctr_dcl smallint
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
