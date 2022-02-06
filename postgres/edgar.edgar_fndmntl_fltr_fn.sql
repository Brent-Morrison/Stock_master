/******************************************************************************
* 
* edgar.edgar_fndmntl_fltr_fn
* 
* DESCRIPTION: 
* Rank and filter for top n stocks by assets and equity
* The 'bad_data_filter' argument when set to true will return stocks with any of 
* nil cash, equity, net income or shares outstanding
* 
* ERRORS
* none
* 
******************************************************************************/

-- Query for "fundamental universe"
select
cik
,fy + 1  as valid_year
,fin_nonfin
,total_assets
,total_equity
,combined_rank
from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 1350, fin_cutoff => 150 ,qrtr => '%q3', bad_data_fltr => false)
where sec_qtr = '2020q3'

select * from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 20, fin_cutoff =>20, qrtr => null)
select * from edgar.edgar_fndmntl_fltr_fn(300, 2000, null, false) where cik = 1702780 order by cik, ddate

-- Function
create or replace function edgar.edgar_fndmntl_fltr_fn
	(
  		fin_cutoff 		int 	default 100
  		,nonfin_cutoff 	int 	default 900
  		,qrtr			text	default null
  		,bad_data_fltr	bool	default false
	) 
	
	returns table 
	(
		stock					text
	    ,cik					int
	    ,sic					smallint
		,ddate					date
		,adsh					char (20)
		,instance				text
		,fy						smallint
		,qtr					text
		,filed					date
		,sec_qtr 				char (6)
		,fin_nonfin				text
		,start_date				date
		,cash_equiv_st_invest	numeric
		,total_cur_assets		numeric
		,intang_asset			numeric
		,total_noncur_assets	numeric
		,total_assets			numeric
		,st_debt				numeric
		,total_cur_liab			numeric
		,lt_debt				numeric
		,total_noncur_liab		numeric
		,total_liab				numeric
		,total_equity			numeric
		,net_income_qtly		numeric
		,shares_cso				numeric
		,shares_ecso			numeric
		,asset_rank				bigint
		,equity_rank			bigint
		,sum_rank				bigint
		,shares_os				numeric
		,combined_rank			bigint
	)

	language plpgsql
	
	as $$
	
	begin
		return query
		
		with t1 as (	
			select 
			t0.*
			,rank() over (partition by t0.sec_qtr, t0.fin_nonfin order by t0.total_assets desc) 	as asset_rank
			,rank() over (partition by t0.sec_qtr, t0.fin_nonfin order by t0.total_equity asc) 		as equity_rank
			from edgar.edgar_fndmntl_all_tb t0
			)
		
		,t2 as (	
			select
			t1.*
			,t1.asset_rank + t1.equity_rank 													as sum_rank
			from t1
			)
		
		,t3 as (
			select 
			t2.*
			,t2.shares_cso + t2.shares_ecso														as shares_os
			,rank() over (partition by t2.sec_qtr, t2.fin_nonfin order by t2.sum_rank asc) 		as combined_rank
			from t2
			)
		
		select 
			t3.*
		from 
			t3
		where 	
			(	(t3.combined_rank <= nonfin_cutoff	and t3.fin_nonfin = 'non_financial'	) 
			or 	(t3.combined_rank <= fin_cutoff  	and t3.fin_nonfin = 'financial')	)
			
			-- filter quarter
			and (qrtr is null or t3.sec_qtr like qrtr)
		
			-- filter missing data 
			and (bad_data_fltr = false
				or	(
							(t3.cash_equiv_st_invest 	= 0 and bad_data_fltr = true)
						or 	(t3.total_equity 			= 0 and bad_data_fltr = true)
						or  (t3.net_income_qtly			= 0 and bad_data_fltr = true)
						or	(t3.shares_os 				= 0 and bad_data_fltr = true)
						or	(t3.shares_os 			is null	and bad_data_fltr = true)
					) 
				)
	;
	end; $$