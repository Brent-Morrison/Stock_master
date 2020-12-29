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
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\edgar_lookups.csv' 
delimiter ',' csv header;


-- Derive depreciation and amortisation for addition to lookups

drop table if exists reference.lookup cascade;

create table reference.lookup as 
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
* which in turn takes data returned from the "?????" query
* 
* 
******************************************************************************/

select * from reference.ticker_cik_sic_ind

truncate table reference.ticker_cik_sic_ind;

--drop table if exists reference.ticker_cik_sic_ind cascade;
			
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
	);		
			
alter table reference.ticker_cik_sic_ind owner to postgres;
alter table reference.ticker_cik_sic_ind add primary key (ticker, cik);

copy reference.ticker_cik_sic_ind		
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\ticker_cik_sic_ind.csv' 			
delimiter ',' csv header;




/******************************************************************************
* 
* DESCRIPTION: reference.fundamental_universe
* Create table for fundamental universe.
* The data for this table is from the "cik_ticker_fndmntl_univ.xlsx" file, 
* which in turn takes data returned from the "smfn_edg_fndmntl_qy" query
* 
* 
* ERRORS: 
* Underlying source contains dupes "cik_ticker_fndmntl_univ.xlsx"
* 
******************************************************************************/

select * from reference.fundamental_universe

drop table if exists reference.fundamental_universe cascade;
			
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

copy reference.fundamental_universe		
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\fundamental_universe.csv' 			
delimiter ',' csv header;




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
		from generate_series('2008-03-01'::date, now()::date, '1 month'::interval) as month_start
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
	--,ipo_date
	--,delist_date
from 
	fund_univ 
	cross join months
	left join tickers
	on fund_univ.cik = tickers.cik
where 
	extract(year from months.month_end) = fund_univ.valid_year
	and (	(combined_rank <= 900 and fin_nonfin = 'non_financial'	)
		 or (combined_rank <= 100 and fin_nonfin = 'financial')	)
	and months.month_end between ipo_date and delist_date
order by 
	cik, month_end
;
