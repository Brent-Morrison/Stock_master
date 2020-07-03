/******************************************************************************
* 
* Load SimFin data make tables and copy
* 
******************************************************************************/

--drop table if exists simfin.us_balance_ins_qtly;

create table simfin.us_cashflow_ins_qtly
	(
		ticker	text
		,simfin_id	integer
		,currency	text
		,fiscal_year	smallint
		,fiscal_period	text
		,report_date	date
		,publish_date	date
		,shares_basic	bigint
		,shares_diluted	bigint
		,net_income_start	numeric
		,depr_amor	numeric
		,non_cash_items	numeric
		,chg_fix_assets_int	numeric
		,net_chg_invest	numeric
		,net_cash_inv	numeric
		,dividends_paid	numeric
		,cash_repay_debt	numeric
		,cash_repurchase_equity	numeric
		,net_cash_fin	numeric
		,effect_fx_rates	numeric
		,net_chg_cash	numeric
	);

alter table simfin.us_cashflow_ins_qtly owner to postgres;

copy simfin.us_cashflow_ins_qtly 
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\us-cashflow-insurance-quarterly.csv' 
delimiter ';' csv header;


select count(distinct ticker) from simfin.us_income_ins_qtly

select * from simfin.us_cashflow_ins_qtly





/******************************************************************************
* 
* SIC, sector, industry table
* 
******************************************************************************/

select 
ct.*
,sf.*
,sb.cik 
,sb.name
,sb.sic
,lk_sf.sector
,lk_sf.industry
,lk_sic.sic_name
from edgar.company_tickers ct
left join simfin.us_companies sf
on ct.ticker = sf.ticker
left join 
	(
	select 
	distinct on (cik) cik
	,sic
	,name
	,period
	from edgar.sub
	order by cik, period desc
	) sb
on ct.cik_str = sb.cik 
left join 
	(
	select 
	lookup_ref as industry_id
	,lookup_val1 as sector
	,lookup_val2 as industry
	from edgar.lookup 
	where lookup_table = 'simfin_industries'
	) lk_sf
on lk_sf.industry_id::int = sf.industry_id
left join 
	(
	select 
	lookup_ref as sic
	,lookup_val1 as sic_name
	from edgar.lookup 
	where lookup_table = 'sic_mapping'
	) lk_sic
on lk_sic.sic::int = sb.sic
where sf.ticker is not null 




/******************************************************************************
* 
* Max share price date
* 
******************************************************************************/

select * from alpha_vantage.shareprices_daily where symbol = 'A'
select * from simfin.us_shareprices_daily where ticker = 'A'

select
row_number 
ticker
,max(date)
from simfin.us_shareprices_daily
group by 1