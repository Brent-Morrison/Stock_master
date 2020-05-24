--=============================================================================
-- Upload check
--=============================================================================

select count(tag) as records, 'tag' as tbl_source from edgar.tag where sec_qtr = '2017q1'
union all
select count(tag) as records, 'tag_bad' as tbl_source from edgar.tag_bad where sec_qtr = '2017q1'
union all
select count(adsh) as records, 'sub' as tbl_source from edgar.sub where sec_qtr = '2017q1'
union all
select count(adsh) as records, 'sub_bad' as tbl_source from edgar.sub_bad where sec_qtr = '2017q1'
union all
select count(tag) as records, 'num' as tbl_source from edgar.num where sec_qtr = '2017q1'
union all
select count(tag) as records, 'num_bad' as tbl_source from edgar.num_bad where sec_qtr = '2017q1'
;



--=============================================================================
-- Data format
-- https://www.sec.gov/edgar/searchedgar/accessing-edgar-data.htm
--=============================================================================

with t1 as (
	select
	sb.name as stock
	,nm.ddate
	,nm.adsh
	,sb.fy
	,substring(sb.fp,2,1) as qtr
	,nm.qtrs
	,sb.filed
	,nm.tag
	--,tg.iord
	--,tg.crdr
	,lk.lookup_val3 as level
	,lk.lookup_val4 as L1
	,lk.lookup_val5 as L2
	,lk.lookup_val6 as L3
	,nm.value/1000000 * lk.lookup_val1::int as amount
	,(date_trunc('month',sb.filed) + interval '3 month - 1 day')::date as start_date
	from edgar.num nm
	inner join edgar.lookup lk
	on nm.tag = lk.lookup_ref
	and lk.lookup_table = 'tag_mapping'
	left join edgar.sub sb
	on nm.adsh = sb.adsh
	where 1 = 1
	-- Filter forms 10-K/A, 10-Q/A being restated filings
	-- This should be done with sb.prevrpt however this attribute removed pre insert 
	and sb.form in ('10-K', '10-Q')
	and sb.name in ('AUTOMATIC DATA PROCESSING INC', 'ORACLE CORP', 
		'ACUITY BRANDS INC', 'ACURA PHARMACEUTICALS, INC',
		'CUSTOMERS BANCORP, INC.','CHEWY, INC.') 
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
	,sum(case when level = '1' and L1 = 'a' then amount else 0 end) as L1_a
	,sum(case when level = '1' and L1 = 'l' then amount else 0 end) as L1_l
	,sum(case when level = '1' and L1 = 'le' then amount else 0 end) as L1_le
	,sum(case when level = '1' and L1 = 'p' then amount else 0 end) as L1_p
	,sum(case when level = '2' and L2 = 'ca' then amount else 0 end) as L2_ca
	,sum(case when level = '2' and L2 = 'nca' then amount else 0 end) as L2_nca
	,sum(case when level = '2' and L2 = 'cl' then amount else 0 end) as L2_cl
	,sum(case when level = '2' and L2 = 'ncl' then amount else 0 end) as L2_ncl
	,sum(case when level = '2' and L2 = 'eq' then amount else 0 end) as L2_eq
	,sum(case when level = '3' and L3 = 'cash' then amount else 0 end) as L3_cash
	,sum(case when level = '3' and L3 = 'st_debt' then amount else 0 end) as L3_std
	,sum(case when level = '3' and L3 = 'lt_debt' then amount else 0 end) as L3_ltd
	,sum(case when level = '3' and L3 = 'intang' then amount else 0 end) as L3_intang
	,sum(case when level = '3' and L3 = 'depr_amort' then amount else 0 end) as L3_dep_amt
	from t1
	group by 1,2,3,4,5,6,7
	),

t3 as (
	select 
	t2.*
	,rank() over (partition by adsh order by ddate desc) as rnk
	,L1_a + L1_le as L1_bs_chk
	,L1_a - L2_ca - L2_nca as L2_a_chk
	,L1_l - L2_cl - L2_ncl - L2_eq as L2_l_chk
	,l2_ca + l2_nca + l2_cl + l2_ncl + l2_eq as L2_bs_chk
	from t2
	),
	
t4 as (	
	select 
	t3.*
	,case when L1_bs_chk = 0 then L1_a else 0 end as total_assets
	,case 
		when L1_bs_chk = 0 and L1_l !=0 then L1_l 
		when L2_cl != 0 or L2_ncl != 0 then L2_cl + L2_ncl
		else 0 end as total_liab
	,case 
		when L1_bs_chk = 0 and L1_l !=0 then -(L1_a + L1_l)
		when L2_cl != 0 or L2_ncl != 0 then -(L1_a + L2_cl + L2_ncl)
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
,sum(total_assets) as total_assets
,sum(total_liab) as total_liab
,sum(total_equity) as total_equity
,sum(net_income_qtly) as net_income_qtly
from t6
group by 1,2,3,4,5,6
;








select distinct sic from edgar.sub

select * from edgar.sub where adsh = '0001766502-19-000007'--coreg = 'KentuckyTrailer' --adsh = '0001558370-20-001148'

select count(*) from edgar.num 
where 