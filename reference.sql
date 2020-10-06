/******************************************************************************
* 
* DESCRIPTION: reference.ticker_cik_sic_ind
* Create table for ticker/cik reference.
* The data for this table is from the "cik_ticker_fndmntl_univ.xlsx" file, 
* which in turn takes data returned from the "?????" query
* 
* 
******************************************************************************/

select * from reference.ticker_cik_sic_ind

drop table if exists reference.ticker_cik_sic_ind cascade;
			
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
* DESCRIPTION: reference.universe_time_series_vw
* Create view for monthly series for stocks in universe
* 
* ERRORS: 
*
* 
******************************************************************************/

select * from reference.universe_time_series_vw

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
		or 	(combined_rank <= 100  and fin_nonfin = 'financial')	)
	and months.month_end between ipo_date and delist_date
order by 
	cik, month_end
;
