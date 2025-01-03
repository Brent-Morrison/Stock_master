
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
		--and nm.adsh in ('0001326801-20-000076','0001564590-20-020502','0001564590-19-039139') 
		--and sb.name in ('FORTUNE BRANDS HOME & SECURITY, INC.','FACEBOOK INC','APPLE INC','ARROW ELECTRONICS INC','PG&E CORP')
		--or sb.cik in (1090872,1121788)
	)

,t11 as 
	(	-- The mappings in edgar.lookup capture shares o/s as 'CommonStockSharesOutstanding' 
		-- this is not always populated.  Grab 'EntityCommonStockSharesOutstanding'
		select 
		adsh as t11_adsh
		,avg(value/1000000) as l1_ecso
		from edgar.num
		where tag = 'EntityCommonStockSharesOutstanding' 
		and coreg = 'NVS'
		group by 1
	)
	
,t12 as 
	(	-- The mappings in edgar.lookup capture shares o/s as 'CommonStockSharesOutstanding' 
		-- and that per t11 above are not always populated.  Grab 'WeightedAverageNumberOfSharesOutstandingBasic'
		select 
		adsh as t12_adsh
		,ddate
		,avg(value/1000000) as l1_wcso
		from edgar.num
		where tag = 'WeightedAverageNumberOfSharesOutstandingBasic'	
		and qtrs in (1,4) -- for non-year ends the quarterly average is disclosed, for year ends only the yearly average (test case FB)
		and coreg = 'NVS'
		group by 1,2
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
--		,sum(case when level = '1' and L1 = 'ecso' 		then amount else 0 end) 	as L1_ecso -- do not use, introduces newer date throwing partition filter in t4
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
		from t6
		group by 1,2,3,4,5,6,7,8,9,10,11,12
	)

select 
t7.*
--,round(t11.l1_ecso,3)																as shares_ecso
,round(coalesce(t11.l1_ecso, t12.l1_wcso),3)										as shares_ecso
from t7
left join t11
on t7.adsh = t11.t11_adsh
left join t12
on t7.adsh = t12.t12_adsh
and t7.ddate = t12.ddate
;




/******************************************************************************
* 
* Create table to insert view results
* 
******************************************************************************/

drop table if exists edgar.edgar_fndmntl_all_tb;
truncate edgar.edgar_fndmntl_all_tb;

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

-- Check status
select sec_qtr, count(*) as n from edgar.edgar_fndmntl_all_tb group by 1 order by 1 desc

	with t1 as (select distinct sec_qtr from edgar.sub except select distinct sec_qtr from edgar.edgar_fndmntl_all_tb)
	select * from edgar.edgar_fndmntl_all_vw where sec_qtr in (select sec_qtr from t1)

-- Insert (TO DO - do this via Airflow by embedding in )
insert into edgar.edgar_fndmntl_all_tb 
	with t1 as (
		select distinct sec_qtr from edgar.sub except 
		select distinct sec_qtr from edgar.edgar_fndmntl_all_tb
		)
	select * from edgar.edgar_fndmntl_all_vw 
	where sec_qtr in (select sec_qtr from t1)
;

select * from edgar.edgar_fndmntl_all_tb where cik in (815556,1459417,771497,1326801) order by 2,7,8;

select distinct sec_qtr from edgar.sub except select distinct sec_qtr from edgar.edgar_fndmntl_all_tb


select max(date_stamp) as date_stamp from access_layer.fundamental_attributes