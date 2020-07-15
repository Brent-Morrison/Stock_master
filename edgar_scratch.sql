/******************************************************************************
* 
* Create ticker to cik view
* 
* DEFECTS
* - This will currently exclude C and GS
* 
******************************************************************************/

drop view edgar.edgar_cik_ticker_view;

create or replace view edgar.edgar_cik_ticker_view as 
		
select 
cik_str 
,ticker 
,title
,ticker_count
from 
	(
	select 
	ct.* 
	,length(ticker) as len
	,min(length(ticker)) over (partition by title) as min_len
	-- This rank causes an issue with C and GS
	,rank() over (partition by title order by ticker asc) as rnk
	,count(ticker) over (partition by cik_str) as ticker_count 
	from edgar.company_tickers ct
	--where title in ('SPHERIX INC','BERKSHIRE HATHAWAY INC','CITIGROUP INC','GOLDMAN SACHS GROUP INC')
	) t1
-- Assume longer tickers relate to non-primary share classes, eg. title = 'SPHERIX INC'
where len = min_len
-- In the event of multiple tickers of the same length,
-- take the first ranked, eg. for title = 'BERKSHIRE HATHAWAY INC',
-- select 'BRKA' over 'BRKB'
and rnk = 1
;

select * from edgar.edgar_cik_ticker_view




/******************************************************************************
* 
* Join simfin and edgar for fundamental view
* 
* TO DO
* - Add edgar data
* 
******************************************************************************/

select 
bal.ticker 
,bal.simfin_id 
,bal.publish_date 
,bal.shares_basic 
,bal.total_assets 
,bal.total_liab 
,bal.total_equity 
,inc.net_income 
from simfin.us_balance_qtly bal
inner join simfin.us_income_qtly inc
on bal.ticker = inc.ticker 
and bal.simfin_id = inc.simfin_id
and bal.publish_date = inc.publish_date

union all 

select 
bal.ticker 
,bal.simfin_id 
,bal.publish_date 
,bal.shares_basic 
,bal.total_assets 
,bal.total_liab 
,bal.total_equity 
,inc.net_income 
from simfin.us_balance_banks_qtly bal
inner join simfin.us_income_banks_qtly inc
on bal.ticker = inc.ticker 
and bal.simfin_id = inc.simfin_id
and bal.publish_date = inc.publish_date

union all 

select 
bal.ticker 
,bal.simfin_id 
,bal.publish_date 
,bal.shares_basic 
,bal.total_assets 
,bal.total_liab 
,bal.total_equity 
,inc.op_income as net_income 
from simfin.us_balance_ins_qtly bal
inner join simfin.us_income_ins_qtly inc
on bal.ticker = inc.ticker 
and bal.simfin_id = inc.simfin_id
and bal.publish_date = inc.publish_date



/******************************************************************************
* 
* Edgar fundamental data by rank
* 
* DEFECTS
* - ??
* 
******************************************************************************/

with t1 as (	
	select 
	edgar.edgar_fndmntl_t1.*
	,rank() over (partition by sec_qtr, fin_nonfin order by total_assets desc) 	as asset_rank
	,rank() over (partition by sec_qtr, fin_nonfin order by total_equity asc) 	as equity_rank
	from edgar.edgar_fndmntl_t1
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

select distinct stock
from t3
where 	(	(combined_rank <= 1750 and fin_nonfin = 'non_financial'	)
		or 	(combined_rank <= 250  and fin_nonfin = 'financial')	)



/******************************************************************************
* 
* Returns data
* 
* DEFINITIONS
* - d01, d30, m01, m03, y01
* - log, ari
* - rtn, vol
* 
* TO DO
* - smax
* 
******************************************************************************/

-- V1
		
with t1 as (
	select 
	symbol 
	,timestamp
	,adjusted_close 
	,volume
	,adjusted_close / lag(adjusted_close, 1) over w_d01 - 1 as d01_ari_rtn
	,ln(adjusted_close / lag(adjusted_close, 1) over w_d01) as d01_log_rtn
	,abs(adjusted_close / lag(adjusted_close, 1) over w_d01 - 1) / (volume * adjusted_close / 10e7) as amihud
	from alpha_vantage.shareprices_daily
	where symbol in ('WMT', 'JPM')
	and timestamp > '2020-01-01'
	window w_d01 as (partition by symbol order by timestamp)
	)

select 
t1.*
,stddev(d01_log_rtn) over w_d20 as d20_log_vol
,avg(amihud) over w_d20 as d20_amihud
,rank() over w_d20_rnk as d01_ari_rtn_rnk
from t1
window w_d20 as (partition by symbol order by timestamp rows between 19 preceding and current row)
,w_d20_rnk as (partition by symbol order by d01_ari_rtn rows between 19 preceding and current row)


-- V2
drop view alpha_vantage.returns_view;

create or replace view alpha_vantage.returns_view as 
	
	select 
	sd.symbol
	,sp5.gics_industry 
	,sd.timestamp
	,sd.close
	,sd.adjusted_close
	,sd.volume 
	from alpha_vantage.shareprices_daily sd
	inner join alpha_vantage.sp_500 sp5
	on sd.symbol = sp5.symbol 
	--where symbol in (select symbol from alpha_vantage.sp_500)
	and sd.timestamp > '2018-01-01'
	order by 1,3,2
	
select * from alpha_vantage.returns_view; 


---------------------------------------------

select * from alpha_vantage.sp_500_dlta where ticker_removed = 'JDSU' or ticker_added = 'JDSU'



---------------------------------------------

select 
ticker 
,min(date) as min_date
,max(date) as max_date
,count(*) 
from simfin.us_shareprices_daily
where ticker not like ('%old%')
group by 1
order by 1;

---------------------------------------------

select 
symbol
,min(timestamp) as min_date
,max(timestamp) as max_date
,count(*) 
from alpha_vantage.shareprices_daily 
group by 1
order by symbol

---------------------------------------------

select * from alpha_vantage.shareprices_daily where symbol = 'FTDR'

---------------------------------------------

select 
distinct ticker 
from simfin.us_shareprices_daily
where ticker not like ('%old%')
and ticker not in 
	(
	select distinct symbol 
	from alpha_vantage.shareprices_daily
	)
order by 1;



		
--------------------------------------------------		

select * from edgar.sub where adsh in ('0001764925-19-000174','0000831001-17-000038')

select * from 
	(
	select 
	ct.* 
	,count(ticker) over (partition by cik_str) as ticker_count 
	from edgar.company_tickers ct
	) t1
where ticker_count > 1

select count(*) from edgar.company_tickers

select * from edgar.edgar_cik_ticker_view where ticker = 'AEP'

select * 
--from edgar.company_tickers
from edgar.sub 
where name in (
'ABV CONSULTING, INC.'
,'ALLY FINANCIAL INC.'
,'AMARIN CORP PLCUK'
,'AMERICAN RENAISSANCE CAPITAL, INC.'
,'ANTERO MIDSTREAM PARTNERS LP'
,'APELLIS PHARMACEUTICALS, INC.'
,'BAKER HUGHES A GE CO LLC'
,'BAKER HUGHES INC'
,'BANCORPSOUTH INC'
,'BENEFICIAL BANCORP INC.'
,'BROADCOM LTD'
,'CALGON CARBON CORP'
,'CAPITOL FEDERAL FINANCIAL INC'
,'CAPITOL FEDERAL FINANCIAL, INC.'
,'CHEE CORP.'
,'CHEMTURA CORP'
,'CIGNA CORP'
,'CITIGROUP INC'
,'DELEK US HOLDINGS, INC.'
,'DOW CHEMICAL CO /DE/'
,'EATON VANCE CORP'
,'EMPIRE STATE REALTY OP, L.P.'
,'ENERGY TRANSFER PARTNERS, L.P.'
,'ENERGY TRANSFER, LP'
,'EVERBANK FINANCIAL CORP'
,'FEDERAL HOME LOAN MORTGAGE CORP'
,'GANNETT CO., INC.'
,'GOLDMAN SACHS GROUP INC'
,'INTERFACE INC'
,'INVESCO DB COMMODITY INDEX TRACKING FUND'
,'INVESTORS BANCORP, INC.'
,'ISHARES S&P GSCI COMMODITY-INDEXED TRUST'
,'KALMIN CORP.'
,'KASKAD CORP.'
,'KEARNY FINANCIAL CORP.'
,'KNIGHT TRANSPORTATION INC'
,'KURA ONCOLOGY, INC.'
,'L3 TECHNOLOGIES, INC.'
,'LA QUINTA HOLDINGS INC.'
,'LEGACY RESERVES LP'
,'LEGACYTEXAS FINANCIAL GROUP, INC.'
,'MERCER INTERNATIONAL INC.'
,'MERIDIAN BANCORP, INC.'
,'NORTHWEST NATURAL GAS CO'
,'ORITANI FINANCIAL CORP'
,'OVINTIV INC.'
,'PIPER JAFFRAY COMPANIES'
,'POWERSHARES DB COMMODITY INDEX TRACKING FUND'
,'PROSHARES TRUST II'
,'RESTAURANT BRANDS INTERNATIONAL LIMITED PARTNERSHIP'
,'RIVIERA RESOURCES, INC.'
,'SCANA CORP'
,'SOHU COM INC'
,'SOLARWINDS CORP'
,'SOLDINO GROUP CORP'
,'SOUTHWEST GAS CORP'
,'SPECTRA ENERGY CORP.'
,'SPECTRUM BRANDS HOLDINGS, INC.'
,'STERIS PLC'
,'STONEMOR PARTNERS LP'
,'TCF FINANCIAL CORP'
,'TIME WARNER INC.'
,'TREX CO INC'
,'TWENTY-FIRST CENTURY FOX, INC.'
,'US ECOLOGY, INC.'
,'VALSPAR CORP'
,'VEREIT, INC.'
,'VISTRA ENERGY CORP.'
,'WALT DISNEY CO/'
,'WESTERN GAS PARTNERS LP'
,'WESTROCK CO'
,'XEROX CORP'
)

select left('vistra-20181231.xml', position('-' in 'vistra-20181231.xml')-1)
select position('-' in 'abvn-20180630.xml')




	
with ed1 as (	
	select 
	edgar.edgar_fndmntl_t1.*
	,rank() over (partition by sec_qtr, fin_nonfin order by total_assets desc) 	as asset_rank
	,rank() over (partition by sec_qtr, fin_nonfin order by total_equity asc) 	as equity_rank
	from edgar.edgar_fndmntl_t1
	)

,ed2 as (	
	select
	ed1.*
	,asset_rank + equity_rank 													as sum_rank
	from ed1
	)

,ed3 as (
	select 
	ed2.*
	,rank() over (partition by sec_qtr, fin_nonfin order by sum_rank asc) 		as combined_rank
	from ed2
	)

,ed4 as (
	select 
	distinct coalesce(ct.ticker, left(instance, position('-' in instance)-1)) as ed_ticker
	--,ed3.*
	from ed3
	left join edgar.edgar_cik_ticker_view ct
	on ed3.cik = ct.cik_str
	where 	(	(combined_rank <= 1400 and fin_nonfin = 'non_financial'	)
			or 	(combined_rank <= 100  and fin_nonfin = 'financial')	)
	)
	
,av as(
	select 
	distinct symbol as av_ticker
	from alpha_vantage.shareprices_daily
	)
	
select
av.av_ticker
,ed4.ed_ticker
,sp5.symbol as sp5_ticker
from ed4
full outer join av
on ed4.ed_ticker = av.av_ticker
full outer join alpha_vantage.sp_500 sp5
on ed4.ed_ticker = sp5.symbol
;



--drop table if exists alpha_vantage.sp_500_dlta;
create table alpha_vantage.sp_500_dlta
	(
		date date
		,ticker_added	text
		,name_added	text
		,ticker_removed text
		,name_removed text
		,reason text
		,capture_date date
	);

alter table alpha_vantage.sp_500_dlta owner to postgres;

/*drop table if exists alpha_vantage.sp_500;
create table alpha_vantage.sp_500
	(
		symbol	text
		,name	text
		,gics_sector text
		,gics_industry text
		,date_added date
		,cik int
		,capture_date date
	);

alter table alpha_vantage.sp_500 owner to postgres;
*/
select distinct symbol from alpha_vantage.shareprices_daily
select