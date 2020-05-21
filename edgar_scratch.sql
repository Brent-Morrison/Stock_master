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
	,tg.iord
	,tg.crdr
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
	and sb.name in ('AUTOMATIC DATA PROCESSING INC', 'ORACLE CORP', 'ACUITY BRANDS INC', 'ACURA PHARMACEUTICALS, INC') --'AUTOMATIC DATA PROCESSING INC', 'ORACLE CORP', 'ACUITY BRANDS INC', 'ACURA PHARMACEUTICALS, INC'
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
	,sum(case when level = '1' and L1 = 'p' then amount else 0 end) as L1_p
	,sum(case when level = '2' and L2 = 'ca' then amount else 0 end) as L2_ca
	,sum(case when level = '2' and L2 = 'nca' then amount else 0 end) as L2_nca
	,sum(case when level = '2' and L2 = 'cl' then amount else 0 end) as L2_cl
	,sum(case when level = '2' and L2 = 'ncl' then amount else 0 end) as L2_ncl
	,sum(case when level = '2' and L2 = 'eq' then amount else 0 end) as L2_eq
	,sum(case when level = '3' and L3 = 'depr_amort' then amount else 0 end) as L3_dep_amt
	from t1
	group by 1,2,3,4,5,6,7
	),

t3 as (
	select 
	t2.*
	,rank() over (partition by adsh order by ddate desc) as rnk
	,L1_a + L1_l as L1_bs_chk
	,L1_a - L2_ca - L2_nca as L2_a_chk
	,L1_l - L2_cl - L2_ncl - L2_eq as L2_l_chk
	from t2
	),
	
t4 as (	
	select 
	t3.*
	,case when L1_bs_chk = 0 then L1_a else 0 end as total_assets
	,case when L1_bs_chk = 0 then L1_l else 0 end as total_liab_equity
	,L2_ca as total_cur_assets
	,case when L2_a_chk = 0 and L1_bs_chk = 0 then L2_nca else L1_a - L2_ca end as total_noncur_assets
	,L2_cl as total_cur_liab
	,case when L2_l_chk = 0 and L1_bs_chk = 0 then L2_ncl else L1_l - L2_cl - L2_eq end as total_noncur_liab
	,case when L1_bs_chk = 0 then L2_eq else 0 end as total_equity
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
	)

select 
t4.*
,L1_p - case when bal_type = 'ytd_pl' and qtrs > 1 
				then lag(L1_p) over (partition by stock, bal_type order by ddate) 
				else 0 
				end as net_income_qtly
from t4
;
