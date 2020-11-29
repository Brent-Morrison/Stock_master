
/******************************************************************************
* 
* edgar.edgar_fndmntl_all_vw
* 
* DESCRIPTION: 
* Create view to extract fundamantal data from the edgar tables
* 
* DATA FORMAT: 
* https://www.sec.gov/edgar/searchedgar/accessing-edgar-data.htm
* 
* ISSUES:
* Change "start_date" to reference end of quarter implied by "sec_qtr" attribute
* 
* 0001459417-20-000003 - '2U, INC.' 
* - Cash is doubled due to mapping of both tags detailed below.
* - 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents' AND 'CashAndCashEquivalentsAtCarryingValue'
* - No income returned
* 
* 0000732717-17-000021 - AT&T 2017 for no total liabilities, 
* - equity not returned since flagged as 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'
* 
* 0000092230-17-000021 - BB&T 
* - no current assets
* 
* 0000063908-20-000022 - MCDONALDS CORP, -ve equity not returned
* 
* 0001418819-19-000019 - IRIDIUM re cash balances
* 
* 0000798354-18-000009 - FISERV INC re cash balances (tag = 'CashAndCashEquivalentsAtCarryingValueIncludingDiscontinuedOperation')
* 
* 0000354950-17-000005 - HOME DEPOT INC, no profit 2017-01-31
* 
* 0000074145-17-000011 - OKLAHOMA GAS & ELECTRIC CO, no cash
* 
* 0001764925-19-000174 - SLACK TECHNOLOGIES, INC., no shares OS
* 
* 0001615774-19-006777 - GARMIN LTD, shares o/s in millions, select * from edgar.num where adsh = '0001615774-19-006777'
* 0001666359-17-000033 - ARROW ELECTRONICS INC, shares o/s quoted in 1000's instead of whole number per all other filings
* 
* Only certain instances of ARCH CAPITAL being returned - select distinct adsh from edgar.num where adsh like '%947484%', this condition not satisfied "and sb.afs = '1-LAF'",
* quarterly filings are labelled afs=2-ACC
* 
* Exclude REAL ESTATE INVESTMENT TRUSTS 6798??
* 
******************************************************************************/

create or replace view edgar.edgar_fndmntl_all_vw as 

with t1 as 
	(
		select
		sb.name as stock
		,nm.ddate
		,nm.adsh
		,sb.instance
		,sb.cik
		,sb.sic
		,nm.sec_qtr
		,sb.fy
		,substring(sb.fp,2,1) as qtr
		,nm.qtrs
		,sb.filed
		,nm.tag
		,lk_t.lookup_val3 as level
		,lk_t.lookup_val4 as L1
		,lk_t.lookup_val5 as L2
		,lk_t.lookup_val6 as L3
		,nm.value/1000000 * lk_t.lookup_val1::int as amount
		,case 
			when lk_s.lookup_val2 = 'Office of Finance' then 'financial' 
			else 'non_financial' end as fin_nonfin
		from edgar.num nm
		inner join edgar.lookup lk_t
		on nm.tag = lk_t.lookup_ref
		and lk_t.lookup_table = 'tag_mapping'
		left join edgar.sub sb
		on nm.adsh = sb.adsh
		left join edgar.lookup lk_s
		on sb.sic = lk_s.lookup_ref::int
		and lk_s.lookup_table = 'sic_mapping' 
		where 1 = 1
		-- Filter forms 10-K/A, 10-Q/A, these being restated filings
		-- This should be done with sb.prevrpt however this was attribute removed pre insert 
		and sb.form in ('10-K', '10-Q') 
		-- coreg filter to avoid duplicates
		and nm.coreg = 'NVS' 
		-- Filer status filter return only larger companies
		-- refer to notes in edgar_structure.xlxs and 'https://www.sec.gov/corpfin/secg-accelerated-filer-and-large-accelerated-filer-definitions'
		and sb.afs = '1-LAF'
		-- FILTERS FOR INVESTIGATION
		--and nm.adsh = '0000947484-17-000024'
		--and sb.name in ('APPLE INC','ARROW ELECTRONICS INC','PG&E CORP')
		--or sb.cik in (1090872,1121788)
	)

,t11 as 
	(
		select 
		adsh as t11_adsh
		,value/1000000 as l1_ecso
		from edgar.num
		where tag = 'EntityCommonStockSharesOutstanding' 
		and coreg = 'NVS'
	)
	
,t2 as 
	(
		select 
		stock
		,cik
		,sic
		,ddate
		,adsh
		,instance
		,fy
		,qtr
		,qtrs
		,filed 
		,sec_qtr
		,fin_nonfin
		,sum(case when level = '1' and L1 = 'a' 		then amount else 0 end) 	as L1_a
		,sum(case when level = '1' and L1 = 'l' 		then amount else 0 end) 	as L1_l
		,sum(case when level = '1' and L1 = 'le' 		then amount else 0 end) 	as L1_le
		,sum(case when level = '1' and L1 = 'cso' 		then amount else 0 end) 	as L1_cso
--		,sum(case when level = '1' and L1 = 'ecso' 		then amount else 0 end) 	as L1_ecso do not use, introduces latter date throwing partition filter in t4
		,min(case when level = '1' and L1 = 'p' 		then amount else 0 end) 	as L1_p_cr
		,max(case when level = '1' and L1 = 'p' 		then amount else 0 end) 	as L1_p_dr
		,max(case when level = '2' and L2 = 'ca' 		then amount else 0 end) 	as L2_ca
		,sum(case when level = '2' and L2 = 'nca' 		then amount else 0 end) 	as L2_nca
		,sum(case when level = '2' and L2 = 'cl' 		then amount else 0 end) 	as L2_cl
		,sum(case when level = '2' and L2 = 'ncl' 		then amount else 0 end) 	as L2_ncl
		,min(case when level = '2' and L2 = 'eq' 		then amount else 0 end) 	as L2_eq_cr
		,max(case when level = '2' and L2 = 'eq' 		then amount else 0 end) 	as L2_eq_dr
		,max(case when level = '3' and L3 = 'cash' 		then amount else 0 end) 	as L3_cash
		,sum(case when level = '3' and L3 = 'st_debt' 	then amount else 0 end) 	as L3_std
		,min(case when level = '3' and L3 = 'lt_debt' 	then amount else 0 end) 	as L3_ltd
		,sum(case when level = '3' and L3 = 'intang' 	then amount else 0 end) 	as L3_intang
		,sum(case when level = '3' and L3 = 'depr_amort'then amount else 0 end) 	as L3_dep_amt
		from t1
		where 1 = 1 
		group by 1,2,3,4,5,6,7,8,9,10,11,12
	)

,t3 as 
	(
		select 
		t2.*
		,rank() over (partition by adsh order by ddate desc) 						as rnk
		,L1_a + L1_le 																as L1_bs_chk
		,L1_a - L2_ca - L2_nca 														as L2_a_chk
		,L1_l - L2_cl - L2_ncl 
			- (case when L2_eq_cr < 0 then L2_eq_cr else L2_eq_dr end)				as L2_l_chk
		,l2_ca + l2_nca + l2_cl + l2_ncl 
			+ (case when L2_eq_cr < 0 then L2_eq_cr else L2_eq_dr end) 				as L2_bs_chk
		,case when L2_eq_cr < 0 then L2_eq_cr else L2_eq_dr end 					as L2_eq
		,case when L1_p_cr < 0 then L1_p_cr else L1_p_dr end 						as L1_p
		from t2
	)
	
,t4 as 
	(	
		select 
		t3.*
		,case when L1_bs_chk = 0 then L1_a else 0 end 								as total_assets
		,case 
			when L1_bs_chk = 0 and L1_l != 0 then L1_l 
			when L2_cl != 0 and L2_ncl != 0 then L2_cl + L2_ncl
			when L2_cl != 0 and L2_ncl = 0 and l2_eq != 0 then l1_le - l2_eq
			else 0 end 																as total_liab
		,case 
			when L1_bs_chk = 0 and L1_l != 0 then -(L1_a + L1_l)
			when L2_cl != 0 and L2_ncl != 0 then -(L1_a + L2_cl + L2_ncl)
			when L2_cl != 0 and L2_ncl = 0 and l2_eq != 0 then l2_eq
			else 0 end 																as total_equity
		,case when L1_bs_chk = 0 then L1_le else 0 end 								as total_liab_equity
		,L1_cso																		as shares_cso
--		,L1_ecso																	as shares_ecso
		,case 
			when qtrs = 0 then 'pit'
			when qtrs::text = qtr or (qtrs::text = '4' and qtr = 'Y') then 'ytd_pl'
			else 'na'
			end 																	as bal_type
		from t3
		where rnk = 1
		and case 
			when qtrs = 0 then 'pit'
			when qtrs::text = qtr or (qtrs::text = '4' and qtr = 'Y') then 'ytd_pl'
			else 'na'
			end != 'na'
	)

,t5 as 
	(	
		select 
		t4.*
		,case 
			when L2_a_chk = 0 then L2_ca 
			when L2_ca <= total_assets and L2_ca != 0 then L2_ca
			when L2_ca = 0 and L2_nca != 0 then total_assets - L2_nca
			else total_assets 
			end 																	as total_cur_assets
		,case 
			when L2_a_chk = 0 then L2_nca 
			when L2_nca <= total_assets and L2_nca != 0 then L2_nca
			when L2_nca = 0 and L2_ca != 0 then total_assets - L2_ca
			else 0
			end 																	as total_noncur_assets
		,case 
			when L2_l_chk = 0 then L2_cl 
			when L2_cl >= total_liab and L2_cl != 0 then L2_cl
			when L2_cl = 0 and L2_ncl != 0 then total_assets - L2_ncl
			else total_liab 
			end 																	as total_cur_liab
		,case 
			when L2_l_chk = 0 then L2_ncl 
			when L2_ncl >= total_liab and L2_ncl != 0 then L2_ncl
			when L2_ncl = 0 and L2_cl != 0 then total_liab - L2_cl
			else 0
			end 																	as total_noncur_liab	
		,L1_p - case when bal_type = 'ytd_pl' and qtrs > 1 
						then lag(L1_p) over (partition by cik, bal_type order by ddate) 
						else 0
						end 														as net_income_qtly
		from t4
	)

,t6 as 
	(	
		select
		t5.*
		,case 
			when L3_cash <= total_cur_assets and L3_cash > 0 then L3_cash
			else 0 
			end 																	as cash_equiv_st_invest
		,case 
			when L3_std >= total_cur_liab and L3_std < 0 then L3_std
			else 0 
			end 																	as st_debt
		,case 
			when L3_ltd >= total_noncur_liab and L3_ltd < 0 then L3_ltd
			else 0 
			end 																	as lt_debt
		,case 
			when L3_intang <= total_assets and L3_intang > 0 then L3_intang
			else 0 
			end 																	as intang_asset
		from t5
	)

,t7 as 
	(
		select 
		stock
		,cik
		,sic
		,ddate
		,t6.adsh
		,instance
		,fy
		,qtr
		,filed
		,sec_qtr 
		,fin_nonfin
		,(date_trunc('month',filed) + interval '3 month - 1 day')::date 			as start_date
		,sum(cash_equiv_st_invest) 													as cash_equiv_st_invest
		,sum(total_cur_assets) 														as total_cur_assets
		,sum(intang_asset) 															as intang_asset
		,sum(total_noncur_assets) 													as total_noncur_assets
		,sum(total_assets) 															as total_assets
		,sum(st_debt) 																as st_debt
		,sum(total_cur_liab) 														as total_cur_liab
		,sum(lt_debt) 																as lt_debt
		,sum(total_noncur_liab) 													as total_noncur_liab
		,sum(total_liab) 															as total_liab
		,sum(total_equity) 															as total_equity
		,sum(net_income_qtly) 														as net_income_qtly
		,round(sum(shares_cso),3)													as shares_cso
--		,sum(shares_ecso)															as shares_ecso
		from t6
		group by 1,2,3,4,5,6,7,8,9,10,11,12
	)

select 
t7.*
,round(t11.l1_ecso,3)																as shares_ecso
--,case when shares_os_cso = 0 then t11.l1_ecso else shares_os_cso end				as shares_os
--,greatest(shares_os_cso, t11.l1_ecso )											as shares_os
from t7
left join t11
on t7.adsh = t11.t11_adsh
;




/******************************************************************************
* 
* Create table to insert view results
* 
******************************************************************************/

drop table if exists edgar.edgar_fndmntl_all_tb;

create table edgar.edgar_fndmntl_all_tb
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
	);

alter table edgar.edgar_fndmntl_all_tb owner to postgres;





/******************************************************************************
* 
* Insert into table
* 
******************************************************************************/

insert into edgar.edgar_fndmntl_all_tb 
	select * 
	from edgar.edgar_fndmntl_all_vw
	where sec_qtr in ('2020q2','2020q3')
;

select * from edgar.edgar_fndmntl_all_tb where cik in (815556,1459417,771497);



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

-- Test	
select * from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 5, fin_cutoff => 5, qrtr => '%q1')
select * from edgar.edgar_fndmntl_fltr_fn(nonfin_cutoff => 5, fin_cutoff => 5, qrtr => null)
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


	
	

/******************************************************************************
* 
* edgar.qrtly_fndmntl_ts_vw
* 
* DESCRIPTION: 
* Join simfin and edgar data to export to R / python.
* Will be used to seed "access_layer.fundamental_universe" table.
* 
* ERRORS:
* 
* TO DO:
* 
******************************************************************************/
	
--Test
select * from edgar.qrtly_fndmntl_ts_vw where ticker = 'ATUS'
	
create or replace view edgar.qrtly_fndmntl_ts_vw as 

with fndmntl as 
	(
		-- Simfin fundamental data
		select 
			ticker
			,fiscal_year
			,fiscal_period
			,'simfin' as src
			,report_date
			,publish_date
			,shares_basic as shares_cso
			,0 as shares_ecso
			,cash_equiv_st_invest
			,total_cur_assets
			,intang_asset
			,total_noncur_assets
			,total_assets
			,-1 * st_debt as st_debt
			,-1 * total_cur_liab as total_cur_liab
			,-1 * lt_debt as lt_debt
			,-1 * total_noncur_liab as total_noncur_liab
			,-1 * total_liab as total_liab
			,-1 * total_equity as total_equity
			,-1 * net_income as net_income_qtly
		
		from 
			simfin.smfn_fndmntl_vw
		where
			publish_date < '2016-12-31'
			
		union all
		
		-- Edgar fundamental data
		select 
			tik.ticker as ticker  -- JOIN TICKER FOR THIS
			,fy as fiscal_year
			,concat('Q', case when qtr = 'Y' then '4' else qtr end) as fiscal_period
			,'edgar' as src
			,ddate as report_date
			,filed as publish_date
			,shares_cso
			,shares_ecso
			,cash_equiv_st_invest
			,total_cur_assets
			,intang_asset
			,total_noncur_assets
			,total_assets
			,st_debt
			,total_cur_liab
			,lt_debt
			,total_noncur_liab
			,total_liab
			,total_equity
			,net_income_qtly
		
		from
			edgar.edgar_fndmntl_all_tb fnd
			left join reference.ticker_cik_sic_ind tik
			on fnd.cik = tik.cik 
	)
	
,ind_ref as 
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
	
,universe as 
	(	
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
	)
	
select
	(date_trunc('quarter', publish_date) + interval '4 month - 1 day')::date as date_available
	,universe.sector
	,universe.fin_nonfin
	,fndmntl.*
from 
	fndmntl
	inner join universe
	on fndmntl.ticker = universe.ticker
	and fndmntl.publish_date between universe.start_date and universe.end_date
	and fndmntl.publish_date between universe.start_year and universe.end_year
	order by 4,9 
;
