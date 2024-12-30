
/***************************************************************************************************************************
* 
* USEFUL QUERIES (postgresql db interrogation)
* 
***************************************************************************************************************************/

-- List largest objects
select relname, relpages
from pg_class
order by relpages desc;


-- Total disk space used by a database
select pg_size_pretty(pg_database_size('stock_master'))
select pg_size_pretty(pg_table_size('stock_master.edgar.num'))


-- Free space
vacuum full verbose edgar.num;


-- Show data file location on disk
show data_directory;


-- Current system settings for the AUTOVACUUM daemon
select * from pg_settings where name like 'autovacuum%'


-- Cascade dependencies - https://stackoverflow.com/questions/37976832/how-to-list-tables-affected-by-cascading-delete
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


-- List tables affected by cascading delete (https://stackoverflow.com/questions/37976832/how-to-list-tables-affected-by-cascading-delete)
select pg_describe_object(classid, objid, objsubid)
from pg_depend 
where refobjid = 'reference.ticker_cik_sic_ind'::regclass and deptype = 'n';


-- Retrieving distinct schema and view names for views that depend on the table 'reference.fundamental_universe'
select distinct n.nspname as schema_name, c.relname as view_name
from pg_depend d
join pg_rewrite w on w.oid = d.objid
join pg_class c on c.oid = w.ev_class
join pg_namespace n on n.oid = c.relnamespace
where d.refclassid = 'pg_class'::regclass 
and d.classid = 'pg_rewrite'::regclass
and d.refobjid = 'reference.fundamental_universe'::regclass
and c.oid <> 'reference.fundamental_universe'::regclass

-- List tables used by a view ()
select 
u.view_schema as schema_name,
u.view_name,
u.table_schema as referenced_table_schema,
u.table_name as referenced_table_name,
v.view_definition
from information_schema.view_table_usage u
join information_schema.views v 
on u.view_schema = v.table_schema
and u.view_name = v.table_name
where u.table_schema not in ('information_schema', 'pg_catalog')
order by 1,2;


/***************************************************************************************************************************
* 
* USEFUL QUERIES (stock_master)
* 
***************************************************************************************************************************/

-- Stock price data
select * from access_layer.shareprices_daily where symbol = 'ZTS'


-- AV data status
select 
t1.*
,(date_trunc('month', max_date) + interval '1 month' - interval '1 day')::date - max_date as days_from_month_end
from 
	(
		select 
		symbol
		,max(timestamp) as max_date
		--,count(*) as records
		from alpha_vantage.shareprices_daily 
		where 1 = 1
		and symbol != 'GSPC'
		group by symbol
		--order by max_date desc, symbol
	) t1


-- Simfin data status
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


-- ticker_excl status
select * from alpha_vantage.ticker_excl where status = 'nil_records_no_update' and last_date_in_db = '2021-11-30'

delete from alpha_vantage.ticker_excl 
where 1 = 1
and ticker in ('MAA','MAC','MAN')
and status = 'nil_records_no_update' 
and last_date_in_db = '2021-11-30'


-- Tickers to update per Python function "update_av_data()"
select * from alpha_vantage.tickers_to_update
where symbol not in (select ticker from alpha_vantage.ticker_excl) and last_date_in_db < '2021-11-30'


-- Last S&P 500 date
select max(timestamp) from alpha_vantage.shareprices_daily where symbol = 'GSPC'
alpha_vantage
-- Active_delisted
select * from alpha_vantage.active_delisted

-- Return attributes status
select date_stamp, count(*) as n from access_layer.return_attributes where date_stamp > current_date - interval '20 years' group by 1 order by 1 desc

-- S&P 500 data status
select max(timestamp) from alpha_vantage.shareprices_daily where symbol = 'GSPC'

-- Query in R "price_attribute" function
select * from access_layer.daily_price_ts_fn(valid_year_param_ => 2023, nonfin_cutoff_ => 900, fin_cutoff_ => 100) where symbol_ = 'IIVI'
-- old version
select * from alpha_vantage.daily_price_ts_fn(valid_year_param => 2023, nonfin_cutoff => 900, fin_cutoff => 100)


select * from edgar.num

select * from (
select 'num' as table, sec_qtr, count(*) as n from edgar.num group by 1,2
--union all 
select 'pre' as table, sec_qtr, count(*) as n from edgar.pre group by 1,2
union all 
select 'sub' as table, sec_qtr, count(*) as n from edgar.sub group by 1,2
union all 
select 'tag' as table, sec_qtr, count(*) as n from edgar.tag group by 1,2
) t1 order by 2 desc, 1 asc


select * from edgar.num_stage where adsh in ('0000320193--22-000108') --'0000320193--22-000108'
select sec_qtr , count(*) as n from edgar.num group by 1

select * from alpha_vantage.shareprices_daily where symbol in ('BLUE') and "timestamp" between '2021-10-31' and '2021-11-30' order by symbol, "timestamp"

select * from alpha_vantage.shareprices_daily where split_coefficient >= 1.5 and "timestamp" between '2021-10-31' and '2021-11-30' order by "timestamp" -- YUM, IBM, ANET

select symbol, max("timestamp") as max_date from alpha_vantage.shareprices_daily group by 1 order by 2 desc


select * from access_layer.return_attributes where date_stamp between '2018-01-31' and '2020-12-31' order by 1, 2;

select upper(split_part(instance, '-', 1)) as ticker, t.* from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 1350, fin_cutoff => 150, qrtr => '%q3', bad_data_fltr => false) t

select * from test.shareprices_daily_test where "timestamp" = '2021-10-29' and symbol = 'YUM' and adjusted_close = 137.98
select * from alpha_vantage.shareprices_daily where symbol = 'XOM' order by "timestamp"

alter table test.shareprices_daily_test add column data_source varchar(5) null default 'AVE';
select * from test.shareprices_daily_test where dividend_amount != 1 --symbol = 'DLTR' order by "timestamp" desc
select * from test.shareprices_daily_test where split_coefficient != 1 order by "timestamp" desc
select * from alpha_vantage.shareprices_daily where symbol = 'ANAT' order by "timestamp" desc

select * from test.shareprices_daily_test where capture_date = '2022-01-29' --and symbol = 'DBI' order by 1
select count(*) from test.shareprices_daily_test where capture_date = '2022-01-27' --"timestamp" = '2022-01-14' --27'
delete from test.shareprices_daily_test where capture_date = '2022-01-28'
select max("timestamp") from test.shareprices_daily_test
update test.tickers_to_update set last_date_in_db = '2021-12-31'::date
select * from access_layer.tickers_to_update_fn(valid_year_param => 2021, nonfin_cutoff => 950, fin_cutoff => 150)
select * from alpha_vantage.earnings where symbol = 'AAPL' order by date_stamp desc

select 
symbol
,"timestamp"
,close
,adjusted_close 
,volume 
,dividend_amount 
,split_coefficient 
,capture_date 
from 
	(	-- Capture most recent version of price data (i.e., split & dividend adjusted)
		select 
		sd.* 
		,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
		from alpha_vantage.shareprices_daily sd 
	) t1
where row_num = 1
and symbol in ('AAPL','DLTR','XOM')
order by 1,2 desc


--truncate test.shareprices_daily_test

select * from test.tickers_to_update



/***************************************************************************************************************************
* 
* USEFUL QUERIES (test data)
* 
***************************************************************************************************************************/

-- Test load AV csv
create table test.shareprices_daily_test as select * from alpha_vantage.shareprices_daily limit 10;
delete from test.shareprices_daily_test;
delete from test.shareprices_daily_test where symbol in ('A','AA','AAL') and "timestamp" > '2021-11-30';

create table test.shareprices_daily_test_idx as table test.shareprices_daily_test with no data;
create index shareprices_daily_idx on test.shareprices_daily_test_idx using btree (symbol, "timestamp")
delete from test.shareprices_daily_test_idx;

select * from information_schema.columns where table_name = 'shareprices_daily_test'


select * from reference.fundamental_universe order by 2,1



/***************************************************************************************************************************
* 
* DUPLICATE CHECKS
* 
***************************************************************************************************************************/

select * from reference.fundamental_universe

-- Remove duplicates from 'active_delisted'
select * from 
(
	select
	ad.*
	,count(*) over (partition by symbol, exchange, status) as n
	from alpha_vantage.active_delisted ad
) t1
where n > 1

delete 
from alpha_vantage.active_delisted t1
using alpha_vantage.active_delisted t2
where t1.ctid < t2.ctid
and t1.symbol = t2.symbol
and t1.exchange = t2.exchange
and t1.status = t2.status



-- Remove duplicates from 'company_tickers'
select count(*) from edgar.company_tickers

select * from 
(
	select 
	ct.* 
	,count(*) over (partition by cik_str, ticker, title) as n
	from edgar.company_tickers ct
) t1
where n > 1

delete 
from edgar.company_tickers t1
using edgar.company_tickers t2
where t1.ctid < t2.ctid
and t1.cik_str = t2.cik_str
and t1.ticker = t2.ticker
and t1.title = t2.title



-- Remove duplicates from 'fundamental_attributes'
select * from 
(
	select * 
	,count(*) over (partition by ticker, date_stamp) as n
	from access_layer.fundamental_attributes
) t1 where n > 1

delete 
from access_layer.fundamental_attributes t1
using access_layer.fundamental_attributes t2
where t1.ctid < t2.ctid
and t1.ticker = t2.ticker
and t1.date_stamp = t2.date_stamp







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

select * from edgar.company_tickers where cik_str in (1646972, 1161154, 1037676) or ticker in ('ARCH','ACI') or title like '%arch%'
select capture_date, count(*) as n from edgar.company_tickers group by 1 order by 1
select capture_date, count(*) as n from alpha_vantage.active_delisted group by 1 order by 1 --where symbol in ('ACI','ARCH','ETP','ARRHW')
select * from alpha_vantage.active_delisted where delist_date > '2023-02-06' and delist_date != '9998-12-31'
select * from alpha_vantage.active_delisted where capture_date = '2023-02-06' order by 1, 3, 6
select symbol, exchange, status, count(*) as n from alpha_vantage.active_delisted where capture_date = '2023-02-06' group by 1,2,3
select * from  edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 1350, fin_cutoff => 150 ,qrtr => '2022%q3', bad_data_fltr => false) where cik in (1037676,1161154,1646972)

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

select * from edgar.num where adsh in ('0001615774-19-006777') and uom = 'shares' --tag like '%Entity%', adsh in ('0001326801-20-000013','0001326801-20-000076','0001564590-20-020502','0001564590-19-039139')
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
select * from edgar.edgar_fndmntl_all_tb where stock like '%APPLE INC%' --(shares_cso > 0 and shares_cso < 10) and (shares_ecso > 0 and shares_ecso < 10)
select ticker, cik, sic, count(*) as n from reference.ticker_cik_sic_ind where delist_date between now()::date and '9998-12-31'::date group by 1,2,3 having count(*) > 1
select distinct on (ticker) ticker, cik, sic from reference.ticker_cik_sic_ind order by ticker, delist_date
select * from edgar.qrtly_fndmntl_ts_vw where ticker = 'AAPL'
select * from edgar.qrtly_fndmntl_ts_vw where date_available >= '2021-12-01' and date_available <= '2022-03-31' and ticker = 'AAPL'

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

select * from reference.lookup where lookup_table = 'tag_mapping'

select distinct lookup_table from reference.lookup

update reference.lookup set lookup_val4 = '3' where lookup_ref = '102001'

select extract(year from date_stamp) as year, symbol, count(*) from access_layer.return_attributes group by 1,2
select date_stamp, count(*) from access_layer.return_attributes group by 1
select * from alpha_vantage.daily_price_ts_vw where date_stamp >= '2019-01-01' and date_stamp <= '2019-12-31' and symbol = 'A'
select max(date_stamp) as max_date from access_layer.return_attributes where fwd_rtn_1m is null group by 1
select date_stamp, count(*) from access_layer.return_attributes group by 1

select * from alpha_vantage.earnings where symbol = 'AAPL'
select symbol, count(*) as records, max(date_stamp) from alpha_vantage.earnings group by 1 order by 1

select * from access_layer.return_attributes WHERE symbol = 'AAPL'
select max(date_stamp) from access_layer.return_attributes
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

select ticker, count(*) as n from access_layer.fundamental_attributes where date_stamp = '2022-05-31' group by 1 order by 1 
select * from access_layer.fundamental_attributes where date_stamp = '2022-05-31' order by ticker, total_cur_assets 
select date_stamp, count(*) from access_layer.return_attributes group by 1 order by 1
select * from alpha_vantage.monthly_price_ts_vw where date_stamp >= '2018-09-01' and date_stamp <= '2020-12-31' group by 1 order by 1
select date_stamp, count(*) from alpha_vantage.daily_price_ts_vw group by 1 order by 1




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





-- COMPLETENESS CHECK
select * from (
	select 
	univ.symbol
	,univ.date_stamp
	--,coalesce(stocks.symbol, lag(stocks.symbol, 1) over (order by gspc.date_stamp)) as symbol
	,case when stocks.date_stamp isnull then 1 else 0 end as miss_ind 
	from
		(
			select stock_univ.symbol_ as symbol
			,gspc.date_stamp
			from access_layer.tickers_to_update_fn(valid_year_param => 2021, nonfin_cutoff => 950, fin_cutoff => 150) stock_univ
			cross join 
			(
				select date_stamp
				from access_layer.shareprices_daily  
				where symbol = 'GSPC' 
				and date_stamp > '2021-12-31'
			) gspc
		) univ
	left join 
		(
			select symbol, date_stamp 
			from access_layer.shareprices_daily 
			where date_stamp > '2021-12-31' 
		) stocks 
	on univ.date_stamp = stocks.date_stamp 
	and univ.symbol = stocks.symbol
) t1 
where miss_ind = 1
--where symbol = 'WSBCP'
order by 1, 2 desc	




select * from edgar.qrtly_fndmntl_ts_fn(valid_year_param_ => 2022, nonfin_cutoff_ => 925, fin_cutoff_ => 125) where ticker_ in ('ABG','AYI')

select * from access_layer.tickers_to_update_fn(valid_year_param => 2024, nonfin_cutoff => 950, fin_cutoff => 150)

create extension fuzzystrmatch;
select levenshtein('Albertsons Companies, Inc.', 'ARCH COAL INC')
select levenshtein('ALBERTSONS COMPANIES, INC.', 'ARCH COAL INC')
select regexp_replace(
		regexp_replace(upper('Social Capital Hedosophia Holdings Corp. VI'), '[[:<:]](COMPANY|COMPANIES|CORP|CO|CLASS A|CLASS B|INC|LTD|PLC)[[:>:]]', '', 'g'),
		'[^\w]+','');

select regexp_replace(
  'And more food or drinks at the international airport Ltd',
  '[[:<:]](and|or|Ltd|international)[[:>:]]',
  ' ',
  'gi'
);


-- ticker_cik_sic_ind
select 
new_.* 
from 
(
	select
	ct.ticker 
	,fn.cik
	,ct.title as name
	,fn.sic
	,coalesce(sf.industry_id, 999999) as simfin_industry_id  
	,ad.exchange 
	,ad.ipo_date 
	,ad.delist_date 
	--,ad.status 
	--,count(*) asn
	from 
		(
			select distinct on (cik) fn.* 
			--select distinct cik, sic, filed
			from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 1350, fin_cutoff => 150 ,qrtr => '%q3', bad_data_fltr => false) fn
			where sec_qtr = '2023q3'
			order by cik, ddate
		) fn  -- multiple quarterly results submitted in the one quarter
	left join 
		(
			select 
			distinct on (cik_str, ticker_letter) ct.*
			from
				(
					select 
					ct.* 
					,left(ticker, 1) as ticker_letter
					,length(ticker) as ticker_len
					from edgar.company_tickers ct
				) ct
			order by cik_str, ticker_letter, ticker_len asc
		) ct
	on fn.cik = ct.cik_str
	left join --  SEE MULTIPLE ABMD
		(
		select distinct on (symbol) ad.*
		from alpha_vantage.active_delisted ad
		order by symbol, capture_date desc
		) ad 
	
	on ct.ticker = ad.symbol
	left join 
	(
		select 
		distinct on (ticker) usc.* 
		from simfin.us_companies usc 
		order by ticker, capture_date desc
	) sf
	on ct.ticker = sf.ticker
	--where fn.filed between ad.ipo_date and ad.delist_date 
	--and ad.status = 'Active'
) new_	
 	
left join 	
	
(	
	select 
	ticker
	,cik
	,delist_date 
	from reference.ticker_cik_sic_ind
) old_	
	
on new_.ticker = old_.ticker	
and new_.cik = old_.cik	
and new_.delist_date = old_.delist_date	
where old_.ticker is null	
order by 1,2	



--fundamental_universe
select 
distinct on (cik) f.*
from
(
	select	
	cik	
	,fy + 1  as valid_year	
	,fin_nonfin	
	,total_assets	
	,total_equity	
	,combined_rank	
	from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 1350, fin_cutoff => 150 ,qrtr => '%q3', bad_data_fltr => false)
	where sec_qtr = '2023q3'	
) f
order by cik, valid_year asc


--------
select * from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 1350, fin_cutoff => 150 ,qrtr => '%q3', bad_data_fltr => false)
order by cik, sec_qtr, ddate

select * from alpha_vantage.active_delisted where symbol in ('ABMD','ACI','AIG','ALF','ALQ','ALZ','ARCH','AVF','CFX','CFXA','ENOV','ETP','FB','META','WEBR') order by 1
select * from simfin.us_companies where ticker in ('ABMD','ACI','AIG','ALF','ALQ','ALZ','ARCH','AVF','CFX','CFXA','ENOV','ETP','FB','META','WEBR') order by 1
select * from edgar.company_tickers where ticker in ('ABMD','ACI','AIG','ALF','ALQ','ALZ','ARCH','AVF','CFX','CFXA','ENOV','ETP','FB','META','WEBR') or cik_str in (3153,5272,1420800) order by 1
select * from reference.ticker_cik_sic_ind where ticker in ('ATH','ACI','ARCH','DELL','FB','META','ETP')
select * from access_layer.tickers_to_update_fn(valid_year_param => 2024, nonfin_cutoff => 950, fin_cutoff => 150) where symbol_ in ('ATH','ACI','ARCH','CXP','DELL','FB','META','ETP')

delete
from simfin.us_companies t1
using simfin.us_companies t2
where t1.ctid < t2.ctid
and t1.simfin_id = t2.simfin_id
and t1.industry_id = t2.industry_id
and t1.ticker = t2.ticker
and t1.capture_date = t2.capture_date


		select 
			t.ticker 
			,t.sic
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
		where 	
			f.valid_year = 2022
			
select * from access_layer.tickers_to_update_fn(valid_year_param => 2023, nonfin_cutoff => 950, fin_cutoff => 150)
select * from access_layer.tickers_to_update_fn()
select * from access_layer.return_attributes where date_stamp > current_date - interval '5 years' and symbol = 'AAPL' order by 2, 1
select date_stamp, report_date, publish_date, total_equity from access_layer.fundamental_attributes where date_stamp > current_date - interval '5 years' and ticker = 'AAPL' order by 1  --date_stamp, total_equity


select 
r.date_stamp
,r.rtn_ari_1m 
,f.report_date
,f.publish_date
,f.total_equity
,f.mkt_cap 
from access_layer.return_attributes r 
left join access_layer.fundamental_attributes f
on r.date_stamp = f.date_stamp 
and r.symbol = f.ticker 
where r.date_stamp between '2017-01-31'::date and '2023-12-31'::date 
and r.symbol = 'AAPL' order by 1


select * from reference.sp500_cons_temp;
create table reference.sp500_cons_temp
(
	date_stamp 		date
	,added 			varchar(6)
	,added_name 	varchar(50)
	,removed 		varchar(6)
	,removed_name 	varchar(50)
	,capture_date 	date
);

create table reference.sp500_cons_mdate
(
	max_date 		date
);




--=======================================================================================================================
-- S&P500 UDPATE PROCEDURES
--=======================================================================================================================


--truncate table reference.sp500_cons

select * from reference.sp500_cons order by min_date desc;

--select * from reference.sp500_cons_temp;
--delete from reference.sp500_cons_temp where date_stamp is not null = '1997-06-17'::date;

with max_date_tbl as (
	select max(max_date) as max_date 
	from (
		select 
		greatest(
			min_date, 
			case when max_date = '9998-12-31'::date then '1980-12-31'::date else max_date end
			) as max_date 
		from reference.sp500_cons
		) t1
	)
update reference.sp500_cons_mdate
set max_date = max_date_tbl.max_date from max_date_tbl;

--insert into reference.sp500_cons_mdate values ('9998-12-31'::date)

select * from reference.sp500_cons_mdate;

-- Insert re additions (HAS TO BE DONE FIRST.  IF THIS IS DONE SECOND NIL RESULTS ARE RETURNED - THE MAX_DATE HAS CHANGED)
with insert_tbl as (
	select
	added 				as ticker
	,date_stamp 		as min_date
	,'9998-12-31'::date as max_date
	,capture_date
	from reference.sp500_cons_temp 
	where date_stamp > (select max_date from reference.sp500_cons_mdate)
	and added is not null
	)

	--select * from insert_tbl
insert into reference.sp500_cons select * from insert_tbl;


-- Update re removals
with update_tbl as (
	select
	t.removed as ticker
	,c.min_date
	,t.date_stamp as max_date
	,t.capture_date
	from reference.sp500_cons_temp t
	left join reference.sp500_cons c
	on t.removed = c.ticker 
	where t.date_stamp > (select max_date from reference.sp500_cons_mdate)
	and t.removed is not null
	and c.max_date = '9998-12-31'::date
	)

--select * from update_tbl
update reference.sp500_cons c
set max_date = u.max_date,
	capture_date = u.capture_date
from update_tbl u
where c.ticker = u.ticker;

select * from reference.sp500_cons where ticker = 'FRC';

-- Constituents for specific date
select * from  reference.sp500_cons 
where '1996-01-31'::date between min_date and max_date
order by min_date desc

-- Assess date range of price data collected for stocks ever in the S&P500
select
sp.*
,spd.min_price_date
,spd.max_price_date
,spd.n
from reference.sp500_cons sp
left join
(
	select 
	symbol
	,min(date_stamp) as min_price_date 
	,max(date_stamp) as max_price_date 
	,count(*) as n
	from access_layer.shareprices_daily
	group by symbol
) spd
on sp.ticker = spd.symbol 
where '1996-01-31'::date between min_date and max_date
order by min_date DESC





/***************************************************************************************************************************
* 
* Academic stock data
* 
***************************************************************************************************************************/

-- Retrieve object description
select obj_description('reference.eapvml'::regclass, 'pg_class');
select obj_description('reference.osap'::regclass, 'pg_class');
select obj_description('reference.permno_ticker_iw'::regclass, 'pg_class');

create index eapvml_permno_idx on reference.eapvml (permno)
create index osap_permno_idx on reference.osap (permno)

/*-------------------------------------------------------------------------------------------------
##### EAPVML table #####
C:\Users\brent\Documents\TRADING_Current\EmpiricalAssetPricingViaMachineLearning.xlsx
https://www.crsp.org/products/documentation/stkquery-stock-data-access (AAPL = 14593 / MSFT = 10107 / AMBC = 12491)

https://www.sec.gov/Archives/edgar/data/320193/000032019318000145/a10-k20189292018.htm
https://www.sec.gov/Archives/edgar/data/789019/000156459020034944/msft-10k_20200630.htm
    mvel1 /sep 	  sp / mar	   sales /
   790,050,073 	0.29015122	 229,234 
 1,073,390,566 	0.24721570	 265,359 (sept 2018 / mar 2019 / ye sep 2018), therefore lag 
 
ISSUES
BM is prepared on different basis to other annual ratios such as SP
 
https://dachxiu.chicagobooth.edu/download/datashare.zip (eapvml)

---------------------------------------------------------------------------------------------------*/

-- Get academic data attributes
drop table if exists eo;

create temporary table eo as 
(
select t1.* from 
	(
		select e.permno, e.date_stamp, e.mvel1, e.sp, e.ep, e.cfp, e.salecash, e.lev, o.leverage, e.bm, o.bm as bm_o, e.gma, o.gp, e.saleinv, e.salerec
		,row_number() over (partition by e.date_stamp order by e.mvel1 desc) as mkt_cap_rank
		from reference.eapvml e
		left join reference.osap o
		on  e.date_stamp = o.date_stamp 
		and e.permno     = o.permno
		where 1 = 1  
		and e.date_stamp > '1979-12-31'::date
		and e.mvel1 is not null
		--and e.permno in (14593, 10107) -- AAPL / MSFT
	) t1
where mkt_cap_rank <= 1500
);

select * from eo where permno = 14593;

with t1 as 
(	-- create fiscal month with partition over random attribute (using "sp", could have used "ep" for example) 
	-- that is returned on a lagged annual basis
	select 
	permno
	,date_stamp
	,mvel1
	,mkt_cap_rank
	,sp
	,ep
	,cfp
	,lev
	,leverage
	,bm
	,bm_o
	,gma
	,gp
	-- capture division by 0 error
	,case when salecash = 0 then 999 else salecash end as salecash
	,case when saleinv  = 0 then 999 else saleinv  end as saleinv
	,case when salerec  = 0 then 999 else salerec  end as salerec
	,row_number() over (partition by permno, sp order by date_stamp) as fiscal_month1
	from eo 
	--where permno in (15222, 28302, 14593) -- 15222 is 0 salecash and NULL saleinv
	order by date_stamp
)

,t2 as 
(	-- adjust fiscal month so that last month in fiscal year is 12
	select t1.*
	,lag(fiscal_month1, 7) over (partition by permno order by date_stamp) as fiscal_month
	from t1
)

,t3 as 
(	-- infer values from ratios and prior year end market cap.
	-- note differing values returned by "bm" across eapvml and osap
	select 
	t2.*
	,round(mvel1/1000) as mkt_cap
	,round(lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000) as mvel_ye
	,round(sp  * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000)) as sales_i 
	,round(ep  * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000)) as net_income_annl_i
	,round(cfp * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000)) as oper_cflow_i 
	,round(sp  * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000) / salecash) as cash_i 
	,round(sp  * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000) / saleinv) as inventory_i 
	,round(sp  * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000) / salerec) as receivables_i 
	,round(lev * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000)) as total_liab_i 
	,round(bm  * (lag(mvel1, (case when fiscal_month > 5 then fiscal_month else fiscal_month + 12 end)::int) over (partition by permno order by date_stamp) / 1000)) as total_equity_i 
	,round(exp(bm_o) * (mvel1 / 1000)) as total_equity_io
	from t2
	order by permno, date_stamp
)

select 
--t3.*
permno, date_stamp, mkt_cap, mkt_cap_rank
,min((date_stamp + interval '6 month')::date) over (partition by mvel_ye, permno) 		as report_date
,lag(sales_i, -2) over (partition by permno order by date_stamp) 						as sales_i
,lag(round(sales_i - ((total_liab_i + total_equity_io) * gp)), -2) over (partition by permno order by date_stamp) as cogs_i  -- incorrect
,lag(net_income_annl_i, -2) over (partition by permno order by date_stamp) 				as net_income_annl_i
,lag(oper_cflow_i, -2) over (partition by permno order by date_stamp) 					as oper_cflow_i
,lag(round(leverage * mkt_cap), -2) over (partition by permno order by date_stamp)		as total_liab_i 		-- from query below
,lag(total_equity_io, -2) over (partition by permno order by date_stamp) 				as total_equity_i
,lag(total_liab_i + total_equity_io, -2) over (partition by permno order by date_stamp)	as total_assets_i 
,case when salecash = 999 then null else 
	lag(cash_i, -2) over (partition by permno order by date_stamp) end 					as cash_i				-- introduces division by zero
,case when saleinv = 999 then null else
	lag(inventory_i, -2) over (partition by permno order by date_stamp) end				as inventory_i			-- introduces division by zero
,case when salerec = 999 then null else
	lag(receivables_i, -2) over (partition by permno order by date_stamp) end			as receivables_i		-- introduces division by zero
from t3
order by permno, date_stamp


-------------------------


select distinct on (date_stamp) date_stamp, permno from reference.osap order by date_stamp
select permno, date_stamp, sp from reference.eapvml where permno in (14593, 10107)  AAPL  


-------------------------


-- Compare inferred balances from the academic data to that collected from the SEC
select 
o.date_stamp 
,case when extract(month from o.date_stamp) = 3 then 1 else 0 end as mon
--,e.sic2
,e.mvel1 /*
,sp.adjusted_close
,sp.adjusted_close / lag(sp.adjusted_close, 1) over (order by o.date_stamp) -1 as rtn_ari_1m
,e.mom1m
,e.mom12m
,o.mom12m
,lag(sp.adjusted_close, 1) over (order by o.date_stamp) / lag(sp.adjusted_close, 12) over (order by o.date_stamp) -1 as rtn_ari_12m
,o.bm
,o.bmdec */
,fa.total_equity
,lag(round(exp(o.bm) * (e.mvel1 / 1000)), -2) over (order by o.date_stamp) as total_equity_i
,o.leverage  -- different to below
,e.lev		 -- different to above
,fa.total_liab
,lag(round(o.leverage * (e.mvel1 / 1000)), -2) over (order by o.date_stamp) as total_liab_i
,lag(round(o.cfp * (e.mvel1 / 1000)), -2) over (order by o.date_stamp) as oper_cflow_i
,fa.ttm_earnings
,lag(round(o.ep * (e.mvel1 / 1000)), -2) over (order by o.date_stamp) as earnings_i
,lag(round(e.ep * (e.mvel1 / 1000)), -2) over (order by o.date_stamp) as earnings_ie
,e.sp as sales_price
,lag(round(e.sp * (e.mvel1 / 1000)), -2) over (order by o.date_stamp) as sales_i
--,e.dolvol
--,o.dolvol
from reference.eapvml e
full outer join reference.osap o
on  e.date_stamp = o.date_stamp 
and e.permno     = o.permno 
left join 
(	-- Monthly close
	select 
	spd.symbol 
	,(date_trunc('month', spd.date_stamp) + interval '1 month - 1 day')::date as date_stamp
	,spd."close"
	,spd.adjusted_close 
	from access_layer.shareprices_daily spd
	inner join 
	(	-- Last trade date for each month
		select 
		max(date_stamp) as last_trade_date
		from access_layer.shareprices_daily
		where symbol = 'GSPC'
		group by date_trunc('month', date_stamp) 	
	) ltd
	on spd.date_stamp = ltd.last_trade_date
	where symbol = 'AAPL'
	--order by date_stamp 
) sp
on e.date_stamp = sp.date_stamp
left join 
(	-- fundamental data from SEC
	select
	date_stamp 
	,fiscal_year 
	,fiscal_period 
	,report_date 
	,publish_date 
	,-total_equity as total_equity
	,-total_liab as total_liab
	,ttm_earnings 
	from access_layer.fundamental_attributes 
	where ticker = 'AAPL'
) fa
on e.date_stamp = fa.date_stamp
where e.permno = 14593 or o.permno = 14593

---------------------------------------------------------------------------------------------------
	
-- Find the top n stocks in the academic data by mkt cap, join price data and 
-- expose missing price data via nulls in "adjusted_close"
select date_stamp, ticker, permno, mkt_cap, mkt_cap_rank, adjusted_close from 
(
	select 
	e.date_stamp
	,e.permno 
	,pt.ticker 
	,round(e.mvel1 / 1000) as mkt_cap
	,spd.adjusted_close 
	,row_number() over (partition by e.date_stamp order by e.mvel1 desc) as mkt_cap_rank
	from reference.eapvml e
	left join reference.permno_ticker_iw pt 
	on e.permno = pt.permno 
	left join access_layer.shareprices_daily spd 
	on pt.ticker = spd.symbol 
	and e.date_stamp = spd.date_stamp 
	where e.date_stamp between '2020-09-30'::date and '2021-09-30'::date
	and pt.ticker != 'no_data'
	and pt.max_date = '2021-12-31'::date
) t1
where mkt_cap_rank <= 750

select date_stamp, ticker, mom1m, retvol from 
(
	select 
	e.date_stamp
	,e.permno 
	,pt.ticker 
	,e.mom1m
	,e.retvol
	,row_number() over (partition by e.date_stamp order by e.mvel1 desc) as mkt_cap_rank
	from reference.eapvml e
	left join reference.permno_ticker_iw pt 
	on e.permno = pt.permno 
	where e.date_stamp between '2021-03-31'::date and '2021-09-30'::date
	and pt.ticker != 'no_data'
	and pt.max_date = '2021-12-31'::date
) t1
where mkt_cap_rank <= 500

select count(distinct date_stamp) from reference.datashare
select count(distinct permno) from reference.signed_predictors_dl_wide
select count(distinct date_stamp) from reference.signed_predictors_dl_wide


select * from reference.permno_ticker_iw where ticker in ('SD','CHV','CVX')
select obj_description('reference.permno_ticker_iw'::regclass, 'pg_class')


select version()

select
column_name 
,data_type
,numeric_precision 
,numeric_precision_radix 
,numeric_scale 
from information_schema.columns
where table_schema = 'reference'
and table_name   = 'datashare'



------------
-- drop indexs and tables
alter table edgar.num drop constraint num_pkey;
alter table edgar.sub drop constraint sub_pkey;
drop table edgar.num_bad;
drop table edgar.sub_bad;

-- Confirm test db is identical to production
select 'sub' as table, sec_qtr, count(*) as n from edgar.sub group by 1,2 order by 2 desc, 1 asc

with t1 as 
(
select distinct sec_qtr from edgar.sub except 
select distinct sec_qtr from edgar.edgar_fndmntl_all_tb
)
select * from edgar.edgar_fndmntl_all_vw 
where sec_qtr = '2023q2'


select 
length(ticker)
,ticker
,

		select 
		length(tcsi.ticker), fu.*, tcsi.* 
		from reference.fundamental_universe fu
		inner join reference.ticker_cik_sic_ind tcsi 
		on fu.cik = tcsi.cik 
		where fu.valid_year = 2024  -- Parameter
		and ( 
			(fu.fin_nonfin  = 'financial' and fu.combined_rank <= 150) or 
			(fu.fin_nonfin != 'financial' and fu.combined_rank <= 950)
			)
		and tcsi.delist_date = '9998-12-31'