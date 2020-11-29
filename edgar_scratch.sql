/******************************************************************************
* 
* USEFUL QUERIES
* 
* 
******************************************************************************/

select * from alpha_vantage.shareprices_daily where symbol = 'AIG' and "timestamp" between '2019-01-01' and '2019-01-31'

select * from alpha_vantage.ticker_excl

delete from alpha_vantage.ticker_excl where status = 'failed_no_data'






/******************************************************************************
* 
* Cascade dependencies
* https://stackoverflow.com/questions/37976832/how-to-list-tables-affected-by-cascading-delete
* 
******************************************************************************/

with recursive chain as (
    select classid, objid, objsubid, conrelid
    from pg_depend d
    join pg_constraint c on c.oid = objid
    where refobjid = 'reference.ticker_cik_sic_ind'::regclass and deptype = 'n'
union all
    select d.classid, d.objid, d.objsubid, c.conrelid
    from pg_depend d
    join pg_constraint c on c.oid = objid
    join chain on d.refobjid = chain.conrelid and d.deptype = 'n'
    )
select pg_describe_object(classid, objid, objsubid), pg_get_constraintdef(objid)
from chain;

--------

select pg_describe_object(classid, objid, objsubid)
from pg_depend 
where refobjid = 'reference.ticker_cik_sic_ind'::regclass and deptype = 'n';





/******************************************************************************
* 
* Range of price data by ticker
* 
* TO DO
* - Add edgar data
* 
******************************************************************************/

select 
ticker 
,min(date) as min_date
,max(date) as max_date
,count(*) 
from simfin.us_shareprices_daily
where ticker = 'AAT'
group by 1
order by 1;

---------------------------------------------

select 
symbol
,min(timestamp) as min_date
,max(timestamp) as max_date
,count(*) as records
from alpha_vantage.shareprices_daily 
where symbol = 'AAT'
group by symbol
order by symbol

---------------------------------------------

select max(timestamp) from alpha_vantage.shareprices_daily where symbol = 'GSPC'

---------------------------------------------

-- Tickers in simfin but not in alpha vantage
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

-- Edgar CIK strings with multiple tickers
select * from 
	(
	select 
	ct.* 
	,count(ticker) over (partition by cik_str) as ticker_count 
	from edgar.company_tickers ct
	) t1
where ticker_count > 1



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
full outer join alpha_vantage. sp5
on ed4.ed_ticker = sp5.symbol
;

--------------------------------------------------	

-- various
select * from alpha_vantage.sp_500_dlta

show data_directory;

select * from alpha_vantage.shareprices_daily where symbol is null -- order by "timestamp" desc

select * from alpha_vantage.shareprices_daily where symbol = 'TEVA' order by "timestamp" desc 

--delete from alpha_vantage.shareprices_daily where symbol = 'SHP' and "timestamp" = '2020-06-18'

select length(symbol) from alpha_vantage.shareprices_daily where symbol like '%ETP%' and "timestamp" = '2000-12-29'

select max("timestamp") from alpha_vantage.shareprices_daily where symbol = 'ADM'

select * from alpha_vantage.tickers_to_update 
where last_date_in_db is not null
order by last_date_in_db desc, symbol asc

select * from alpha_vantage.returns_view

select * from edgar.company_tickers where cik_str = 1161154

--------------------------------------------------	

-- sic to simfin industry
with sp_1500 as 
	(
	select 
	distinct on (ticker) ticker
	,"name"
	,gics_sector 
	,gics_industry
	,src
	from 
		(
			select 
			symbol as ticker 
			,"name"
			,gics_sector 
			,gics_industry 
			,'sp_500' as src
			from alpha_vantage.sp_500
			
			union all
			
			select 
			ticker 
			,company as "name"
			,gics_sector 
			,gics_industry 
			,'sp_1000' as src
			from alpha_vantage.sp_1000
		) t1
	order by ticker, src desc
	)

,sf_sic as (
	select 
	simfin_ind
	,max_sic
	,min_sic
	,gics_sector
	,gics_industry
	,sum(tickers) as tickers
	from 
		( 
			select 
			coalesce(ctm.ticker,sf.ticker,sp_1500.ticker)
			,ctm.stock
			,ctm.cik
			,sf.industry_id as simfin_ind
			,gics_sector
			,gics_industry
			,max(ctm.sic) as max_sic
			,min(ctm.sic) as min_sic
			,count(ctm.ticker) as tickers
			from edgar.cik_ticker_master ctm
			left join simfin.us_companies sf
			on ctm.ticker = sf.ticker 
			left join sp_1500
			on ctm.ticker = sp_1500.ticker 
			group by 1,2,3,4,5,6
		) t1
	group by 1,2,3,4,5
	--having simfin_ind is not null
	)

, sf_ref as 
	(
		select 
		lookup_ref::int as sf_ind
		,lookup_val1 as sf_sector
		,lookup_val2 as sf_industry
		from edgar.lookup 
		where lookup_table = 'simfin_industries'
	),

sic_ref as 
	(
		select
		lookup_ref::int as sic
		,lookup_val2 as sic_level_1
		,lookup_val3 as sic_level_2
		,lookup_val4 as sic_level_3
		from edgar.lookup 
		where lookup_table = 'sic_mapping'
	)
select
distinct on (sf_sic.max_sic) sf_sic.max_sic
,sf_sic.simfin_ind
,sf_sic.min_sic
,sf_sic.tickers
,max_sic - min_sic as diff_chk
,sf_sector
,sf_industry
,gics_sector
,gics_industry
,sic_level_1
,sic_level_2
,sic_level_3
from sf_sic
left join sf_ref
on sf_sic.simfin_ind = sf_ref.sf_ind 
left join sic_ref
on sf_sic.max_sic = sic_ref.sic 
order by 
sf_sic.max_sic
,tickers desc



--------------------------------------------------	

select 
nm.* 
,case 
	when value > 1e12 then value / 1e6
	when value > 1e9 then value / 1e3
	when value < 1e6 then value * 1e3
	else value
	end as shares_os
from edgar.num nm
where 1 = 1
and adsh in ('0001615774-19-006777', '0001666359-17-000033', '0001004980-17-000023', '0001004980-18-000005','0001650030-18-000007','0001666359-17-000033','0000320193-20-000010') 
and lower(tag) like ('%common%')
and lower(tag) not like all (array['%issued%','%authorized%','%incremental%'])
and uom = 'shares'
and coreg = 'NVS'
order by 1,4

select * from edgar.num where adsh = '0001411579-19-000063' and tag = 'WeightedAverageNumberOfSharesOutstandingBasic'
select * from edgar.sub where cik = '1702780'
select tag, count(distinct adsh) from edgar.num where uom = 'shares' group by 1

		select 
		adsh as t11_adsh
		,value/1000000 as l1_ecso
		from edgar.num where value = 52080077
		where tag like ('EntityCommonStockSharesOutstanding' 
		and coreg = 'NVS'
		and adsh = '0001411579-19-000063'

select * from alpha_vantage.shareprices_daily where symbol = 'AAPL' and "timestamp" between '2014-06-01' and '2014-06-30'
select * from alpha_vantage.shareprices_daily where symbol = 'FAST' and "timestamp" between '2019-05-01' and '2019-05-31'
select count(*) from alpha_vantage.shareprices_daily where symbol = 'AAT' and "timestamp" between '2016-01-01' and '2019-01-31'

select * from reference.fundamental_universe where cik = 1500217
select * from reference.ticker_cik_sic_ind where cik = 1500217
select * from edgar.edgar_fndmntl_all_tb where cik = 1500217

select * from alpha_vantage.tickers_to_update where symbol not in (select ticker from alpha_vantage.ticker_excl)
delete from alpha_vantage.ticker_excl where status = 'failed_no_data'
select distinct sec_qtr from edgar.num
SELECT pg_database_size('postgres')
SELECT pg_size_pretty(pg_database_size('postgres'))
select pg_size_pretty(pg_table_size('stock_master.edgar.num'))