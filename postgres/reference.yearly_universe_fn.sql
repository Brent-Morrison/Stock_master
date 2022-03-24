/******************************************************************************
* 
* reference.yearly_universe_fn
* 
* DESCRIPTION: 
* Select stocks for universe inclusion based on size and yer valid
* 
* ERRORS:
* - None
* 
* TO DO:
* - NA
* 
******************************************************************************/

-- Test function
select * from reference.yearly_universe_fn(nonfin_cutoff => 900, fin_cutoff => 150, valid_year_param => 2021) where symbol in ('AAPL','AGFC','ALXN','BGNE','EBIX','KOSN','TUSK')

drop function reference.yearly_universe_fn;

-- Function
create or replace function reference.yearly_universe_fn

(
	fin_cutoff 			int 	default 100
	,nonfin_cutoff 		int 	default 900
	,valid_year_param	int 	default 2021
) 

returns table 
(
	symbol 				text
	,sector				text
	,industry			text
	,fin_nonfin			text
	,valid_year			int
	,start_date		 	date
	,end_date		 	date
	,start_year		 	date
	,end_year		 	date
)


language plpgsql

as $$

begin
	
return query

with ind_ref as 
	(
		select distinct
		ind.ticker 
		,lk.lookup_val4 as sector
		,lk.lookup_val5 as industry
		,case -- see TO DO
			when ind.sic::int between 6000 and 6500 then 'financial' 
			else 'non_financial' end as fin_nonfin
		from reference.ticker_cik_sic_ind ind
		left join reference.lookup lk
		on ind.simfin_industry_id = lk.lookup_ref::int
		and lk.lookup_table = 'simfin_industries' 
		where lk.lookup_val4 != '13' -- ignore records with default industry
	)	
	
,universe as 
	(	
		select 
			t.ticker as symbol
			,t.sic
			,i.fin_nonfin
			,i.sector
			,i.industry
			,f.valid_year 
			,t.ipo_date as _start_date
			,t.delist_date as _end_date
			,case 
				when lag(f.valid_year) over (partition by t.ticker order by f.valid_year) is null then make_date(f.valid_year-2,1,1) 
				else make_date(f.valid_year,1,1) 
				end as _start_year
			,make_date(f.valid_year,12,31) as _end_year
		from 
			reference.fundamental_universe f
			left join 
				(
					select 
					distinct on (ticker) t.*
					from reference.ticker_cik_sic_ind t
					order by ticker, delist_date asc
				) t  
			on f.cik = t.cik
			left join ind_ref i
			on t.ticker = i.ticker
		where 
			( 
				(i.fin_nonfin  = 'financial' and f.combined_rank <= fin_cutoff) or 
				(i.fin_nonfin != 'financial' and f.combined_rank <= nonfin_cutoff)
			)
			and f.valid_year = valid_year_param
	)
	
select
	universe.symbol
	,universe.sector
	,universe.industry
	,universe.fin_nonfin
	,valid_year_param 
	,universe._start_date
	,universe._end_date
	,universe._start_year
	,universe._end_year
from 
	universe
order by 1,5

;
end; $$
	
