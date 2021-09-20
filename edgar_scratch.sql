/***************************************************************************************************************************
* 
* USEFUL QUERIES
* 
***************************************************************************************************************************/
-- List largest objects
select relname, relpages
from pg_class
order by relpages desc;

select pg_size_pretty(pg_database_size('geekdb'))

select * from alpha_vantage.active_delisted

select * from edgar.sub where adsh in ('0000006955-21-000003','0000006955-21-000012')
select sec_qtr , count(*) as n from edgar.num group by 1

select * from alpha_vantage.shareprices_daily where symbol = 'GPOR' and "timestamp" between '2020-12-31' and '2021-06-30' order by "timestamp"

select symbol, max("timestamp") as max_date from alpha_vantage.shareprices_daily group by 1 order by 2 desc

select date_stamp, count(*) as n from access_layer.return_attributes group by 1 order by 1 desc

select * from access_layer.return_attributes where date_stamp between '2018-01-31' and '2020-12-31' order by 1, 2;

select * from alpha_vantage.ticker_excl where status = 'failed_no_data' and last_date_in_db = '2021-04-30'
delete from alpha_vantage.ticker_excl where status = 'failed_no_data' and last_date_in_db = '2021-04-30'  --ticker in ('ZBRA','ZG','ZION','ZNGA','ZTS')  --last_date_in_db = '2020-11-02'

-- Tickers to update
select * from alpha_vantage.tickers_to_update 
where symbol not in (select ticker from alpha_vantage.ticker_excl)
and last_date_in_db <= '2021-05-31'

select * from alpha_vantage.ticker_excl

select stmt, tag, count(*) from edgar.pre where tag like('%epreciat%') group by 1,2

-- Last S&P 500 date
select max(timestamp) from alpha_vantage.shareprices_daily where symbol = 'GSPC'

select * from edgar.qrtly_fndmntl_ts_vw where date_available >= '2020-12-31'

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
where 1 = 1
--and ticker = 'AAPL'
group by 1
order by 1;

----------------------------------------------------------------------------------------------------------------------------


select 
symbol
,min(timestamp) as min_date
,max(timestamp) as max_date
,count(*) as records
from alpha_vantage.shareprices_daily 
where 1 = 1
--and symbol = 'AAPL'
group by symbol
order by symbol
----------------------------------------------------------------------------------------------------------------------------

select 
symbol
,capture_date 
,min(timestamp) as min_date
,max(timestamp) as max_date
,count(*) as records
from alpha_vantage.shareprices_daily 
where 1 = 1
and capture_date = '2021-06-05'
--and symbol = 'AAPL'
group by 1,2
order by 1,2


----------------------------------------------------------------------------------------------------------------------------

select max(timestamp) from alpha_vantage.shareprices_daily where symbol = 'GSPC'

----------------------------------------------------------------------------------------------------------------------------

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

----------------------------------------------------------------------------------------------------------------------------		

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

----------------------------------------------------------------------------------------------------------------------------

-- various
select * from alpha_vantage.sp_500_dlta

show data_directory;

select * from alpha_vantage.shareprices_daily where symbol is null -- order by "timestamp" desc

select * from alpha_vantage.shareprices_daily where symbol = 'AAPL' order by "timestamp" desc 

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

select * from edgar.num where adsh in ('0001326801-20-000013') and uom = 'shares' --tag like '%Entity%', adsh in ('0001326801-20-000013','0001326801-20-000076','0001564590-20-020502','0001564590-19-039139')
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

select * from reference.fundamental_universe where cik = 1144980
select * from reference.ticker_cik_sic_ind where cik = 1500217
select * from edgar.edgar_fndmntl_all_tb where (shares_cso > 0 and shares_cso < 10) and (shares_ecso > 0 and shares_ecso < 10)

select * from alpha_vantage.tickers_to_update where symbol not in (select ticker from alpha_vantage.ticker_excl)
delete from alpha_vantage.ticker_excl where status = 'failed_no_data'
select distinct sec_qtr from edgar.num

-- Determine database size
select pg_database_size('postgres')
select pg_size_pretty(pg_database_size('postgres'))
select pg_size_pretty(pg_table_size('stock_master.edgar.num'))



select * from (
with ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case 
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
--,universe as 
--	(	
		select 
			t.ticker 
			,t.sic
			,i.sector
			,i.fin_nonfin
			,t.ipo_date as start_date
			,t.delist_date as end_date
			,case 
				when lag(f.valid_year) over (partition by t.ticker order by f.valid_year) is null then make_date(f.valid_year-2,1,1) 
				else make_date(f.valid_year,1,1) 
				end as start_year
			,make_date(f.valid_year,12,31) as end_year
		from 
			reference.fundamental_universe f
			left join reference.ticker_cik_sic_ind t
			on f.cik = t.cik
			left join ind_ref i
			on t.ticker = i.ticker
		where 
			(i.fin_nonfin = 'financial' and f.combined_rank < 100) or
			(i.fin_nonfin != 'financial' and f.combined_rank < 900) 
		order by
			t.ticker 
			,f.valid_year
) t1 
where f.valid_year = 2020

select * from reference.ticker_cik_sic_ind

select * from reference.fundamental_universe where valid_year = 2020 and ((fin_nonfin = 'financial' and combined_rank < 100) or (fin_nonfin != 'financial' and combined_rank < 900))

select * from reference.lookup where lookup_table = 'simfin_industries'

select * from reference.lookup where lookup_ref = '102001'

update reference.lookup set lookup_val4 = '3' where lookup_ref = '102001'

select extract(year from date_stamp) as year, symbol, count(*) from access_layer.return_attributes group by 1,2
select date_stamp, count(*) from access_layer.return_attributes group by 1
select * from alpha_vantage.daily_price_ts_vw where date_stamp >= '2019-01-01' and date_stamp <= '2019-12-31' and symbol = 'A'
select max(date_stamp) as max_date from access_layer.return_attributes where fwd_rtn_1m is null group by 1
select date_stamp, count(*) from access_layer.return_attributes group by 1

select * from alpha_vantage.earnings where symbol = 'ARW' and date_stamp = '2020-11-23'
select symbol, count(*) as records from alpha_vantage.earnings group by 1 order by 1

select * from access_layer.return_attributes 

SELECT distinct tag FROM edgar.pre where stmt = 'IS'
select distinct sec_qtr from edgar.pre

CREATE TABLE test.test_table
	(
		symbol text
		,sector int
		,date_stamp date
		,"close" numeric
		,adjusted_close numeric
		,volume numeric
		,sp500 numeric
	)
;
	
select * from test.test_table

select 
vw.* 
,fn.*
--,(date_trunc('month', vw.date_stamp) + interval '1 month - 1 day')::date as date_stamp
from alpha_vantage.daily_price_ts_vw vw 
inner join 
	(
	select *
	from reference.universe_time_series_fn(nonfin_cutoff => 900, fin_cutoff => 100, valid_year_param => 2019)
	where symbol in ('BGNE','EBIX','KOSN','TUSK')
	) fn
on fn.date_stamp = (date_trunc('month', vw.date_stamp) + interval '1 month - 1 day')::date
and fn.symbol = vw.symbol



-- VALUATION MODEL DATA
select 
date_stamp
,sector
,ticker 
,mkt_cap
,roe
,total_assets 
,total_equity 
from access_layer.fundamental_attributes
where 1 = 1 
--and date_stamp in ('2020-06-30','2020-12-31') 
and ticker = 'AAPL'
order by 1,2,3,4,5

select date_available, count(*) from edgar.qrtly_fndmntl_ts_vw group by 1 order by 1 --where ticker ='AAPL'

select date_stamp, count(*) from access_layer.fundamental_attributes group by 1 order by 1
select date_stamp, count(*) from access_layer.return_attributes group by 1 order by 1
select * from alpha_vantage.monthly_price_ts_vw where date_stamp >= '2018-09-01' and date_stamp <= '2020-12-31' group by 1 order by 1
select date_stamp, count(*) from alpha_vantage.daily_price_ts_vw group by 1 order by 1


-- REMOVE DUPLICATES
delete 
from access_layer.return_attributes t1
using access_layer.return_attributes t2
where t1.ctid < t2.ctid
and t1.date_stamp = t2.date_stamp
and t1.symbol = t2.symbol

select 
	ctid
	,row_number() over (partition by symbol, date_stamp order by date_stamp) as row_num
	,ra.*
	from access_layer.return_attributes ra
) t1
where t1.row_num > 1 


-- DOLLAR VOLUME UNIVERSE
select
t2.*
,rank() over (partition by year_date order by dollar_volume desc) as dollar_volume_rank
from (
	select 
	symbol
	,year_date
	,count(year_mon) as month_active_count
	,sum(dollar_volume) as dollar_volume
	from (
		select 
			symbol
			,extract(year from date_stamp) as year_date
			,extract(year from date_stamp)||'_'||extract(month from date_stamp) as year_mon
			,round(sum(volume * "close")/1e6, 2) as dollar_volume
		from 
			(
			select 
			"timestamp" as date_stamp
			,symbol
			,"close"
			,volume
			from 
				(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
					select 
					sd.* 
					,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
					from alpha_vantage.shareprices_daily sd 
					where "timestamp" <= '2011-12-31' 
					and symbol != 'GSPC'
				) t1
			) dpts 
		group by 1,2,3
	) t1
	group by 1,2
	having count(year_mon) = 12
) t2
order by 2,5






















select * from alpha_vantage.price_ts_fn(valid_year_param => 2020, nonfin_cutoff => 10, fin_cutoff => 10)

create or replace function alpha_vantage.price_ts_fn

(
	fin_cutoff 			int 	default 100
	,nonfin_cutoff 		int 	default 900
	,valid_year_param	int		default 2021
) 

returns table 
(
	symbol_ 			text
	,sector_ 			text
	,date_stamp_		date
	,"close_"			numeric
	,adjusted_close_	numeric
	,volume_			numeric
	,sp500_				numeric
	,valid_year_ind_ 	text
) as $$

with prices as 
	(
		select 
		"timestamp"
		,symbol
		,close
		,adjusted_close
		,volume
		from 
			(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
				select 
				sd.* 
				,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
				from alpha_vantage.shareprices_daily sd 
			) t1
		where row_num = 1
		and symbol != 'GSPC'
	)

,sp_500 as 
	(
		select 
		"timestamp",
	    adjusted_close as sp500
	    from alpha_vantage.shareprices_daily spd
	    inner join 
			(
				select 
				max("timestamp") as last_trade_date
				from alpha_vantage.shareprices_daily
				where symbol = 'GSPC'
				group by date_trunc('month', "timestamp") 	
			) ltd
	    on spd."timestamp" = ltd.last_trade_date
		where symbol = 'GSPC'
    )

,ind_ref as 
	(
		select
		ind.ticker 
		,lk.lookup_val4 as sector
		,case -- see TO DO
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
	)	
	
,universe as 
	(	
		select 
			t.ticker 
			,t.sic
			,i.sector
			,f.valid_year 
			,t.ipo_date as start_date
			,t.delist_date as end_date
			,case 
				when lag(f.valid_year) over (partition by t.ticker order by f.valid_year) is null then make_date(f.valid_year-2,1,1) 
				else make_date(f.valid_year,1,1) 
				end as start_year
			,make_date(f.valid_year,12,31) as end_year
		from 
			reference.fundamental_universe f
			left join reference.ticker_cik_sic_ind t
			on f.cik = t.cik
			left join ind_ref i
			on t.ticker = i.ticker
		where ( 
			(i.fin_nonfin  = 'financial' and f.combined_rank <= $1) or 
			(i.fin_nonfin != 'financial' and f.combined_rank <= $2)
			)
			and f.valid_year = $3
	)
	
select
	prices.symbol
	,universe.sector
	,prices."timestamp" as date_stamp 
	,prices."close"
	,prices.adjusted_close
	,prices.volume
	,sp_500.sp500
	,case when extract(year from prices."timestamp") = universe.valid_year then 'valid' else 'invalid' end as valid_year_ind
from 
	prices
	inner join universe
	on prices.symbol = universe.ticker
	and prices."timestamp" between universe.start_date and universe.end_date
	and prices."timestamp" between universe.start_year and universe.end_year
	inner join sp_500
	on prices."timestamp" = sp_500."timestamp" 
order by 1,3 
;

$$ language sql immutable
;
