/******************************************************************************
* 
* reference.lookup_csv
* 
* DESCRIPTION: 
* Create table for variance lookups.
* https://stackoverflow.com/questions/14083311/permission-denied-when-trying-to-import-a-csv-file-from-pgadmin
* 
* TO DO:
* add IndefiniteLivedLicenseAgreements & OtherIntangibleAssetsNet to 'intang' per 0000732717-17-000021
* add LongTermDebtAndCapitalLeaseObligations re 0000732717-17-000021
* 
******************************************************************************/

select * from reference.lookup;

drop table if exists reference.lookup_csv cascade;

create table reference.lookup_csv
	(
		lookup_table	text	
		,lookup_ref 	text
		,lookup_val1	text
		,lookup_val2	text
		,lookup_val3	text
		,lookup_val4	text
		,lookup_val5	text
		,lookup_val6	text
		,lookup_val7	text
	);

alter table reference.lookup_csv owner to postgres;

copy reference.lookup_csv 
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\reference\edgar_lookups.csv' 
delimiter ',' csv header;


-- Derive depreciation and amortisation for addition to lookups ---------------

truncate table reference.lookup;
--drop table if exists reference.lookup cascade;

insert into reference.lookup
--create table reference.lookup as 
	select * from reference.lookup_csv
	
	union all 
	
	select
	'tag_mapping' as lookup_table
	,tag 
	,'1' as lookup_val1
	,'1' as lookup_val2
	,'3' as lookup_val3
	,'na' as lookup_val4
	,'na' as lookup_val5
	,'depr_amort' as lookup_val6
	,'na' as lookup_val7
	from 
		(
			select distinct stmt, tag 
			from edgar.pre 
			where stmt = 'CF' 
			and (lower(tag) like '%depletion%' or lower(tag) like '%depreciation%' or lower(tag) like '%amort%')
			and (lower(tag) like '%intang%' or lower(tag) like '%goodwill%')
			order by stmt, tag
		) as lookup_ref
;





/******************************************************************************
* 
* reference.ticker_cik_sic_ind
* 
* DESCRIPTION: 
* Create table for ticker/cik reference.
* The data for this table is from the "cik_ticker_fndmntl_univ.xlsx" file, 
* which in turn takes data returned from the query below.
*  
******************************************************************************/


-- Create table 
create table reference.ticker_cik_sic_ind			
	(
		ticker				text
		,cik				integer
		,name				text
		,sic				integer
		,simfin_industry_id	integer
		,exchange			text
		,ipo_date			date
		,delist_date		date
		,capture_date		date
	);		
			
alter table reference.ticker_cik_sic_ind owner to postgres;

alter table reference.ticker_cik_sic_ind add primary key (ticker, cik, capture_date);


-- Select new data to insert. To be copied to excel for manual over-ride
-- TO DO: needs to include "capture_date"
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


select * from reference.ticker_cik_sic_ind where delist_date = '1900-01-01';


-- Truncate table prior to full re-load
truncate reference.ticker_cik_sic_ind;
			

-- Table insert from csv
copy reference.ticker_cik_sic_ind		
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\reference\ticker_cik_sic_ind.csv' 			
delimiter ',' csv header;


-- Adhoc updates
update reference.ticker_cik_sic_ind
set delist_date = '2020-08-01'
where cik = 82811
and ticker = 'RBC'



/******************************************************************************
* 
* DESCRIPTION: reference.fundamental_universe
* Create table for fundamental universe.
* The data for this table is from the "cik_ticker_fndmntl_univ.xlsx" file, 
* which in turn takes data returned from the "edgar.edgar_fndmntl_fltr_fn" function
* 
* 
* ERRORS: 
* Underlying source contains dupes "cik_ticker_fndmntl_univ.xlsx"
* 
******************************************************************************/




-- Query for selection of current year universe
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

			
create table reference.fundamental_universe			
	(
		cik				integer
		,valid_year		integer
		,fin_nonfin		text
		,total_assets	numeric
		,total_equity	numeric
		,combined_rank	integer
	);		
			
alter table reference.fundamental_universe owner to postgres;
alter table reference.fundamental_universe add primary key (cik, valid_year);


-- Truncate table prior to full re-load
truncate reference.fundamental_universe


-- Table insert from csv
copy reference.fundamental_universe		
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\reference\fundamental_universe.csv' 			
delimiter ',' csv header;


select * from reference.fundamental_universe



/******************************************************************************
* 
* reference.universe_time_series_vw
* 
* DESCRIPTION: 
* Create view returning monthly series for stocks in universe
* 
* Universe defined on size (assets & equity) and availability date
* from 
* 
* ERRORS: 
*
* 
******************************************************************************/

select * from reference.universe_time_series_vw where ticker = 'A' 

create or replace view reference.universe_time_series_vw as 

with months as 
	(
		select month_start::date - 1 as month_end
		from generate_series('2000-01-01'::date, now()::date, '1 month'::interval) as month_start
	)

,fund_univ as 
	(
		select * from reference.fundamental_universe
	)

,tickers as 
	(
		select * from reference.ticker_cik_sic_ind
	)

select 
	fund_univ.cik
	,tickers.ticker
	,months.month_end
	,ipo_date
	,delist_date
	,case 
		when lag(fund_univ.valid_year) 
			over (partition by tickers.ticker order by fund_univ.valid_year) is null 
			then make_date(fund_univ.valid_year-2,1,1) 
		else make_date(fund_univ.valid_year,1,1) 
		end as start_year
	,make_date(fund_univ.valid_year,12,31) as end_year
from 
	fund_univ 
	cross join months
	left join tickers
	on fund_univ.cik = tickers.cik
where 
	(	   
			(combined_rank <= 900 and fin_nonfin = 'non_financial'	)
	 	or 	(combined_rank <= 100 and fin_nonfin = 'financial'      )	
	 )
	and months.month_end between tickers.ipo_date and tickers.delist_date
	and months.month_end between start_date and end_date
	and months.month_end between start_year and end_year
order by 
	ticker, month_end
;






-- SCRATCH UNIVERSE CTE
-- Test function
select * 
from reference.universe_time_series_fn(nonfin_cutoff => 900, fin_cutoff => 100, valid_year_param => 2019) 
where symbol in ('AGFC','ALXN','BGNE','EBIX','KOSN','TUSK')

select 
extract(year from date_stamp) as yr 
,valid_year_ind
,count(distinct symbol) 
from reference.universe_time_series_fn(nonfin_cutoff => 900, fin_cutoff => 100, valid_year_param => 2020) 
where symbol in ('BGNE','EBIX','KOSN','TUSK')
group by 1,2


-- Function
create or replace function reference.universe_time_series_fn

	(
		fin_cutoff 			int 	default 100
		,nonfin_cutoff 		int 	default 900
		,valid_year_param	int 	default 2020
	) 
	
	returns table 
	(
		symbol 			text
		,sector			text
		,fin_nonfin		text
		,valid_year_ind text
		,date_stamp		date
	)


	language plpgsql
	
	as $$
	
	begin
		
	return query
	
		with prices as 
			(
				select month_start::date - 1 as month_end
				from generate_series('2000-01-01'::date, now()::date, '1 month'::interval) as month_start
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
					t.ticker as symbol
					,t.sic
					,i.sector
					,i.fin_nonfin
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
				where 
					(
						(i.fin_nonfin = 'financial' and f.combined_rank < fin_cutoff) or
						(i.fin_nonfin != 'financial' and f.combined_rank < nonfin_cutoff) 
					)
					and f.valid_year = valid_year_param
				order by
					t.ticker 
					,f.valid_year
			)
			
		select
			universe.symbol
			,universe.sector
			,universe.fin_nonfin
			,case when extract(year from prices.month_end) = universe.valid_year then 'valid' else 'invalid' end as valid_year_ind
			,prices.month_end as date_stamp 
		from 
			universe
			left join prices
		on prices.month_end >= universe.start_date 
			and prices.month_end <= universe.end_date
			and prices.month_end >= universe.start_year 
			and prices.month_end <= universe.end_year
		order by 1,5
	
	;
	end; $$
	
	
	
-- Test view
select 
extract(year from date_stamp) as yr 
,valid_year_ind
,count(distinct symbol) 
from reference.universe_time_series_vw 
where extract(year from date_stamp) >= 2019
group by 1,2
and symbol in ('BGNE','EBIX','KOSN','TUSK')


-- View
create or replace view reference.universe_time_series_vw as 

with prices as 
	(
		select month_start::date - 1 as month_end
		from generate_series('2000-01-01'::date, now()::date, '1 month'::interval) as month_start
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
			t.ticker as symbol
			,t.sic
			,i.sector
			,i.fin_nonfin
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
		where 
			(
				(i.fin_nonfin = 'financial' and f.combined_rank < 100) or
				(i.fin_nonfin != 'financial' and f.combined_rank < 900) 
			)
			and f.valid_year = 2020
		order by
			t.ticker 
			,f.valid_year
	)
	
select
	universe.symbol
	,universe.sector
	,universe.fin_nonfin
	,case when extract(year from prices.month_end) = universe.valid_year then 'valid' else 'invalid' end as valid_year_ind
	,prices.month_end as date_stamp 
from 
	prices
	cross join universe
where
	prices.month_end between universe.start_date and universe.end_date
	and prices.month_end between universe.start_year and universe.end_year
	order by 1,5
;
