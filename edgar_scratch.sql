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

---------------------------------------------

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
* Create ticker to cik view
* 
******************************************************************************/

drop view edgar.edgar_cik_ticker_view;

create or replace view edgar.edgar_cik_ticker_view as 
		
select 
cik_str 
,ticker 
,title 
from 
	(
	select 
	ct.* 
	,length(ticker) as len
	,min(length(ticker)) over (partition by title) as min_len
	-- This rank causes an issue with C and GS
	,rank() over (partition by title order by ticker asc) as rnk
	from edgar.company_tickers ct
	where title in ('SPHERIX INC','BERKSHIRE HATHAWAY INC','CITIGROUP INC','GOLDMAN SACHS GROUP INC')
	) t1
-- Assume longer tickers relate to non-primary share classes, eg. title = 'SPHERIX INC'
where len = min_len
-- In the event of multiple tickers of the same length,
-- take the first ranked, eg. for title = 'BERKSHIRE HATHAWAY INC',
-- select 'BRKA' over 'BRKB'
and rnk = 1
;


select * from edgar.sub where adsh in ('0001764925-19-000174','0000831001-17-000038')


select * from edgar.edgar_cik_ticker_view

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
	where 	(	(combined_rank <= 900 and fin_nonfin = 'non_financial'	)
			or 	(combined_rank <= 100  and fin_nonfin = 'financial')	)
	)
	
,av as(
	select 
	distinct symbol as av_ticker
	from alpha_vantage.shareprices_daily
	)
	
select
av_ticker
,ed_ticker
,ticker as sp_ticker
from ed4
full outer join av
on ed4.ed_ticker = av.av_ticker
full outer join alpha_vantage.sp_1000 av1
on ed4.ed_ticker = av1.ticker
;




select * from alpha_vantage.sp_500
select * from alpha_vantage.sp_500_dlta


drop table if exists alpha_vantage.sp_500;
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


drop table if exists alpha_vantage.sp_500_dlta;
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

