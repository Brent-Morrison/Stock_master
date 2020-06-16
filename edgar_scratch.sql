
/*******************************************************************************************
 * 
 * DATA FORMAT: https://www.sec.gov/edgar/searchedgar/accessing-edgar-data.htm
 * 
 * ISSUES
 * 
 * 0001459417-20-000003 - '2U, INC.' cash is doubled due to two tags as below.
 * - 'CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents' AND 'CashAndCashEquivalentsAtCarryingValue'
 * 
 * Check AT&T 2017 for no total liabilities, 
 * equity not returned since flagged as 'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'
 * adsh = '0000732717-17-000021'
 * 
 * Check BB&T for no current assets adsh = '0000092230-17-000021'
 * 
 ********************************************************************************************/

with t1 as (
	-- new
	select
	sb.name as stock
	,nm.ddate
	,nm.adsh
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
	,case when lk_s.lookup_val2 = 'Office of Finance' then 'financial' else 'non_financial' end as fin_nonfin
	-- This can be moved down the chain
	,fc.value as equity_cutoff
	,sum(case when nm.tag = 'StockholdersEquity' then nm.value else 0 end) over (partition by nm.adsh, nm.ddate, nm.sec_qtr) as equity_actual
	-- This can be moved down the chain
	from edgar.num nm
	inner join edgar.lookup lk_t
	on nm.tag = lk_t.lookup_ref
	and lk_t.lookup_table = 'tag_mapping'
	left join edgar.sub sb
	on nm.adsh = sb.adsh
	left join edgar.lookup lk_s
	on sb.sic = lk_s.lookup_ref::int
	and lk_s.lookup_table = 'sic_mapping' 
	-- For size cut-off, change join to prior months cut-offs
	left join 
		(
			select 
			tag 
			,sec_qtr
			,fin_nonfin
			,rnk
			,value 
			from edgar.fndmtl_cutoffs 
			where tag = 'StockholdersEquity' 
			and (fin_nonfin = 'financial' and rnk = 200
			or fin_nonfin = 'non_financial' and rnk = 1800)
		) fc
	on fc.fin_nonfin = case when lk_s.lookup_val2 = 'Office of Finance' then 'financial' else 'non_financial' end
	and fc.sec_qtr = nm.sec_qtr 
	where 1 = 1
	-- Filter forms 10-K/A, 10-Q/A, these being restated filings
	-- This should be done with sb.prevrpt however this was attribute removed pre insert 
	and sb.form in ('10-K', '10-Q')
	-- coreg filter to avoid duplicates
	and nm.coreg = 'NVS'
	--and sb.name in ('') 						-- FILTER FOR INVESTIGATION
	--and nm.adsh = '0001193125-17-050292' 		-- FILTER FOR INVESTIGATION
	),

t2 as (
	select 
	stock
	,ddate
	,adsh
	,fy
	,qtr
	,qtrs
	,filed 
	,sum(case when level = '1' and L1 = 'a' then amount else 0 end) 			as L1_a
	,sum(case when level = '1' and L1 = 'l' then amount else 0 end) 			as L1_l
	,sum(case when level = '1' and L1 = 'le' then amount else 0 end) 			as L1_le
	,sum(case when level = '1' and L1 = 'p' then amount else 0 end) 			as L1_p
	,sum(case when level = '2' and L2 = 'ca' then amount else 0 end) 			as L2_ca
	,sum(case when level = '2' and L2 = 'nca' then amount else 0 end) 			as L2_nca
	,sum(case when level = '2' and L2 = 'cl' then amount else 0 end) 			as L2_cl
	,sum(case when level = '2' and L2 = 'ncl' then amount else 0 end) 			as L2_ncl
	,min(case when level = '2' and L2 = 'eq' then amount else 0 end) 			as L2_eq
	,sum(case when level = '3' and L3 = 'cash' then amount else 0 end) 			as L3_cash
	,sum(case when level = '3' and L3 = 'st_debt' then amount else 0 end) 		as L3_std
	,sum(case when level = '3' and L3 = 'lt_debt' then amount else 0 end) 		as L3_ltd
	,sum(case when level = '3' and L3 = 'intang' then amount else 0 end) 		as L3_intang
	,sum(case when level = '3' and L3 = 'depr_amort' then amount else 0 end) 	as L3_dep_amt
	from t1
	where 1 = 1 --equity_actual > equity_cutoff
	group by 1,2,3,4,5,6,7
	),

t3 as (
	select 
	t2.*
	,rank() over (partition by adsh order by ddate desc) as rnk
	,L1_a + L1_le 								as L1_bs_chk
	,L1_a - L2_ca - L2_nca 						as L2_a_chk
	,L1_l - L2_cl - L2_ncl - L2_eq 				as L2_l_chk
	,l2_ca + l2_nca + l2_cl + l2_ncl + l2_eq 	as L2_bs_chk
	from t2
	),
	
t4 as (	
	select 
	t3.*
	,case when L1_bs_chk = 0 then L1_a else 0 end as total_assets
	,case 
		when L1_bs_chk = 0 and L1_l != 0 then L1_l 
		when L2_cl != 0 and L2_ncl != 0 then L2_cl + L2_ncl
		when L2_cl != 0 and L2_ncl = 0 and l2_eq != 0 then l1_le - l2_eq
		else 0 end as total_liab
	,case 
		when L1_bs_chk = 0 and L1_l != 0 then -(L1_a + L1_l)
		when L2_cl != 0 and L2_ncl != 0 then -(L1_a + L2_cl + L2_ncl)
		when L2_cl != 0 and L2_ncl = 0 and l2_eq != 0 then l2_eq
		else 0 end as total_equity
	,case when L1_bs_chk = 0 then L1_le else 0 end as total_liab_equity
	,case 
		when qtrs = 0 then 'pit'
		when qtrs::text = qtr or (qtrs::text = '4' and qtr = 'Y') then 'ytd_pl'
		else 'na'
		end as bal_type
	from t3
	where rnk = 1
	and case 
		when qtrs = 0 then 'pit'
		when qtrs::text = qtr or (qtrs::text = '4' and qtr = 'Y') then 'ytd_pl'
		else 'na'
		end != 'na'
	),

t5 as (	
	select 
	t4.*
	,case 
		when L2_a_chk = 0 then L2_ca 
		when L2_ca <= total_assets and L2_ca != 0 then L2_ca
		when L2_ca = 0 and L2_nca != 0 then total_assets - L2_nca
		else total_assets 
		end as total_cur_assets
	,case 
		when L2_a_chk = 0 then L2_nca 
		when L2_nca <= total_assets and L2_nca != 0 then L2_nca
		when L2_nca = 0 and L2_ca != 0 then total_assets - L2_ca
		else 0
		end as total_noncur_assets
	,case 
		when L2_l_chk = 0 then L2_cl 
		when L2_cl >= total_liab and L2_cl != 0 then L2_cl
		when L2_cl = 0 and L2_ncl != 0 then total_assets - L2_ncl
		else total_liab 
		end as total_cur_liab
	,case 
		when L2_l_chk = 0 then L2_ncl 
		when L2_ncl >= total_liab and L2_ncl != 0 then L2_ncl
		when L2_ncl = 0 and L2_cl != 0 then total_liab - L2_cl
		else 0
		end as total_noncur_liab	
	,L1_p - case when bal_type = 'ytd_pl' and qtrs > 1 
					then lag(L1_p) over (partition by stock, bal_type order by ddate) 
					else 0 
					end as net_income_qtly
	from t4
	),

t6 as (	
	select
	t5.*
	,case 
		when L3_cash <= total_cur_assets and L3_cash > 0 then L3_cash
		else 0 
		end as cash_equiv_st_invest
	,case 
		when L3_std >= total_cur_liab and L3_std < 0 then L3_std
		else 0 
		end as st_debt
	,case 
		when L3_ltd >= total_noncur_liab and L3_ltd < 0 then L3_ltd
		else 0 
		end as lt_debt
	,case 
		when L3_intang <= total_assets and L3_intang > 0 then L3_intang
		else 0 
		end as intang_asset
	from t5
	)

select 
stock
,ddate
,adsh
,fy
,qtr
,filed
,(date_trunc('month',filed) + interval '3 month - 1 day')::date as start_date
,sum(cash_equiv_st_invest) as cash_equiv_st_invest
,sum(total_cur_assets) as total_cur_assets
,sum(intang_asset) as intang_asset
,sum(total_noncur_assets) as total_noncur_assets
,sum(total_assets) as total_assets
,sum(st_debt) as st_debt
,sum(total_cur_liab) as total_cur_liab
,sum(lt_debt) as lt_debt
,sum(total_noncur_liab) as total_noncur_liab
,sum(total_liab) as total_liab
,sum(total_equity) as total_equity
,sum(net_income_qtly) as net_income_qtly
from t6
group by 1,2,3,4,5,6
--having sum(cash_equiv_st_invest) = 0   -- FILTER FOR INVESTIGATION
;





