
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
		, total_equity_cln numeric
		, asset_growth numeric
		, roa numeric
		, roe numeric
		, leverage numeric
		, other_ca_ratio numeric
		, sue numeric
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
select date_stamp, count(*) as records from access_layer.return_attributes group by 1 order by 1

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
		, sector smallint
		, industry smallint
		, suv numeric
		, ipc numeric
	);

alter table access_layer.return_attributes owner to postgres;


