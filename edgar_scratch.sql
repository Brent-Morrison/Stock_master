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

select * from edgar.company_tickers

select * from edgar.edgar_cik_ticker_view



/******************************************************************************
* 
* Create ticker list based on "instance record" in the sub table
* 
* DEFECTS
* - incomplete
* 
******************************************************************************/

select 
sb.*
,count(cik) over (partition by cik, sub_tkr_1) as ticker_1_count
,ct.ct_tickers
,case 
	when sb.sub_tkr_1 = any(ct.ct_tickers) 
	and  sb.sub_tkr_2 = any(ct.ct_tickers) then 1 
	else 0 
	end as filter1
,case 
	when sub_tkr_2 !~ '[[:digit:]]' then 1
	else 0
	end as filter2
from 
	(
		select 
	    cik
	    ,upper(substring(instance, '[A-Za-z]{1,5}')) as sub_tkr_1
	    ,length(substring(instance, '[A-Za-z]{1,5}')) as sub_tkr_1_len
	    ,upper(left(instance, position('-' in instance)-1)) as sub_tkr_2
	    ,length(left(instance, position('-' in instance)-1)) as sub_tkr_2_len
		from edgar.sub
		where form in ('10-K', '10-Q')
		and afs = '1-LAF'
		and length(left(instance, position('-' in instance)-1)) < 6
		and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FY'
		and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FORM'
		group by 1,2,3,4,5
		--order by 1
	) sb

left join 
	(	-- company tickers data from sec website
		-- 
	    select 
	    cik_str
	    ,array_agg(ticker) as ct_tickers
	    from edgar.company_tickers
	    group by cik_str
    ) ct
on sb.cik = ct.cik_str



/******************************************************************************
* 
* Join simfin and edgar for fundamental view
* 
* TO DO
* - Add edgar data
* 
******************************************************************************/

with t1 as (
	
	select 
	bal.ticker 
	,bal.simfin_id 
	,bal.fiscal_year
	,bal.fiscal_period
	,bal.report_date
	,bal.publish_date 
	,bal.shares_basic / 1e6 			as shares_basic
	,bal.cash_equiv_st_invest / 1e6 	as cash_equiv_st_invest
	,bal.total_cur_assets / 1e6			as total_cur_assets
	,0 									as intang_asset
	,bal.total_noncur_assets / 1e6 		as total_noncur_assets
	,bal.total_assets / 1e6 			as total_assets
	,bal.st_debt / 1e6 					as st_debt
	,bal.total_cur_liab / 1e6 			as total_cur_liab
	,bal.lt_debt / 1e6 					as lt_debt
	,bal.total_noncur_liab / 1e6 		as total_noncur_liab
	,bal.total_liab / 1e6 				as total_liab
	,bal.total_equity / 1e6 			as total_equity
	,inc.net_income / 1e6				as net_income
	from simfin.us_balance_qtly bal
	inner join simfin.us_income_qtly inc
	on bal.ticker = inc.ticker 
	and bal.simfin_id = inc.simfin_id
	and bal.publish_date = inc.publish_date
	
	union all 
	
	select 
	bal.ticker 
	,bal.simfin_id 
	,bal.fiscal_year
	,bal.fiscal_period
	,bal.report_date
	,bal.publish_date 
	,bal.shares_basic / 1e6 			as shares_basic
	,bal.cash_equiv_st_invest / 1e6 	as cash_equiv_st_invest
	,0 									as total_cur_assets
	,0 									as intang_asset
	,0 									as total_noncur_assets
	,bal.total_assets / 1e6 			as total_assets
	,bal.st_debt / 1e6 					as st_debt
	,0 									as total_cur_liab
	,bal.lt_debt / 1e6 					as lt_debt
	,0 									as total_noncur_liab
	,bal.total_liab / 1e6 				as total_liab
	,bal.total_equity / 1e6 			as total_equity
	,inc.net_income / 1e6 				as net_income
	from simfin.us_balance_banks_qtly bal
	inner join simfin.us_income_banks_qtly inc
	on bal.ticker = inc.ticker 
	and bal.simfin_id = inc.simfin_id
	and bal.publish_date = inc.publish_date
	
	union all 
	
	select 
	bal.ticker 
	,bal.simfin_id 
	,bal.fiscal_year
	,bal.fiscal_period
	,bal.report_date
	,bal.publish_date 
	,bal.shares_basic / 1e6 			as shares_basic
	,bal.cash_equiv_st_invest / 1e6 	as cash_equiv_st_invest
	,0 									as total_cur_assets
	,0 									as intang_asset
	,0 									as total_noncur_assets
	,bal.total_assets / 1e6 			as total_assets
	,bal.st_debt / 1e6 					as st_debt
	,0 									as total_cur_liab
	,bal.lt_debt / 1e6 					as lt_debt
	,0 									as total_noncur_liab
	,0 									as total_liab 
	,bal.total_equity / 1e6 			as total_equity
	,inc.op_income / 1e6 				as net_income
	from simfin.us_balance_ins_qtly bal
	inner join simfin.us_income_ins_qtly inc
	on bal.ticker = inc.ticker 
	and bal.simfin_id = inc.simfin_id
	and bal.publish_date = inc.publish_date
	)

select
extract (year from t1.publish_date) as yr
,extract (quarter from t1.publish_date) as qtr
,concat(extract (year from t1.publish_date), 'q', extract (quarter from t1.publish_date)) as sec_qtr
,t1.* 
from t1


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
where ticker not like ('%old%')
group by 1
order by 1;

---------------------------------------------

select 
symbol
,min(timestamp) as min_date
,max(timestamp) as max_date
,count(*) as records
from alpha_vantage.shareprices_daily 
group by 1
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

select * from alpha_vantage.shareprices_daily where symbol = 'CSCO' order by "timestamp" desc 

delete from alpha_vantage.shareprices_daily where symbol = 'ATW' and "timestamp" = '2020-06-18'

select length(symbol) from alpha_vantage.shareprices_daily where symbol like '%ADM%' and "timestamp" = '2000-12-29'

select max("timestamp") from alpha_vantage.shareprices_daily where symbol = 'ADM'

select * from alpha_vantage.tickers_to_update 
where last_date_in_db is not null
order by last_date_in_db desc, symbol asc

select * from alpha_vantage.returns_view

select * from edgar.company_tickers where cik_str = 70858

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

-- ticker_excl

drop table if exists alpha_vantage.ticker_excl cascade;
			
create table alpha_vantage.ticker_excl			
	(		
		ticker	text
		,last_date_in_db date
		,price	numeric
		,last_av_date date
	);		
			
alter table alpha_vantage.ticker_excl owner to postgres;	

alter table alpha_vantage.ticker_excl add column status text;

update alpha_vantage.ticker_excl set status = 'data_up_to_date' where status is null;

select * from alpha_vantage.ticker_excl order by ticker;

select * from pg_catalog.pg_locks 

-- ATW isa new ticker??
-- CBG is now CBRE, need to account for changed tickers


--------------------------------------------------	

-- actual data industry map
with tickers as (
	select 
	distinct ticker 
	from (
		select ticker from simfin.us_companies
		union all
		select symbol as ticker from alpha_vantage.sp_500
		union all
		select ticker from alpha_vantage.sp_1000
		union all
		select ticker from zacks.zacks_gt_750
		union all
		select ticker from edgar.cik_ticker_master
		) t1
	)

,sf_ref as (
	select ticker, industry_id as sf_ind_id from simfin.us_companies
	)

,sf_ind as (
	select * 
	from edgar.lookup 
	where lookup_table = 'simfin_industries'
	)

,sp_ind as (
	-- dupes in here
	select symbol as ticker, gics_sector, gics_industry from alpha_vantage.sp_500
	union all
	select ticker, gics_sector, gics_industry from alpha_vantage.sp_1000
	)

,zacks_ind as (
	select ticker, zacks_sector, zacks_industry
	from zacks.zacks_gt_750
	)

,edgar_ref as (
	select distinct ticker, sic from edgar.cik_ticker_master
	)
	
,edgar_ind as (
	select * 
	from edgar.lookup 
	where lookup_table = 'sic_mapping'
	)

,hrchy as (
	select 
	tickers.ticker
	,sf_ind.lookup_val1 as sf_sector
	,sf_ind.lookup_val3 as sf_industry
	,sp_ind.gics_sector
	,replace(sp_ind.gics_industry,'&', 'and') as gics_industry
	,zacks_ind.zacks_sector
	,zacks_ind.zacks_industry
	,edgar_ref.sic
	,edgar_ind.lookup_val3 as edgar_sector
	,edgar_ind.lookup_val4 as edgar_industry
	from tickers
	left join sf_ref
	on tickers.ticker = sf_ref.ticker
	left join sf_ind
	on sf_ref.sf_ind_id = sf_ind.lookup_ref::int
	left join sp_ind
	on tickers.ticker = sp_ind.ticker
	left join zacks_ind
	on tickers.ticker = zacks_ind.ticker
	left join edgar_ref
	on tickers.ticker = edgar_ref.ticker
	left join edgar_ind
	on edgar_ref.sic = edgar_ind.lookup_ref::int
	)
	
select 
gics_sector
,gics_industry
,sf_sector
,sf_industry
,zacks_sector
,zacks_industry
,edgar_industry
,count(ticker) as records
from hrchy
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7

-- edgar_raw1
select 
ct.cik_str
,ct.ticker
,ct.title as name 
from edgar.company_tickers ct
inner join (select distinct cik from edgar.sub where afs = '1-LAF') sb
on ct.cik_str = sb.cik

-- edgar_raw2
with t1 as (
	select 
	distinct 
	cik
	,upper(substring(instance, '[A-Za-z]{1,5}')) as ticker
	from edgar.sub
	where form in ('10-K', '10-Q')
	and afs = '1-LAF'
	and length(substring(instance, '[A-Za-z]{1,5}')) > 1
	and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FY'
	and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FORM'
	except 
	select 
	cik_str
	,ticker
	from edgar.company_tickers
	)

,t2 as (
	select 
	distinct on (cik) cik
	,upper(substring(instance, '[A-Za-z]{1,5}')) as ticker
	,translate(name, '/,.,,', '') as name 
	from edgar.sub
	where form in ('10-K', '10-Q')
	and afs = '1-LAF'
	order by cik, period 
	)
		
select 
t1.cik
,t1.ticker
,t2.name
from t1
left join t2
on t1.cik = t2.cik

select * from edgar.sub where cik in (23082,1688568) --(1001039, 1744489, 1363829)
select * from edgar.company_tickers where cik_str in (1001039, 1744489)  -- ORIGINAL NOT IN 'company_tickers'