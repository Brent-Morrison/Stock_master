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
	,lk.lookup_val3 as level
	,lk.lookup_val4 as level_1
	,lk.lookup_val5 as level_2
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
	,sum(case when level = '1' and level_1 = 'a' then amount else 0 end) as level_1_a
	,sum(case when level = '1' and level_1 = 'l' then amount else 0 end) as level_1_l
	,sum(case when level = '1' and level_1 = 'p' then amount else 0 end) as level_1_p
	,sum(case when level = '2' and level_2 = 'ca' then amount else 0 end) as level_2_ca
	,sum(case when level = '2' and level_2 = 'nca' then amount else 0 end) as level_2_nca
	,sum(case when level = '2' and level_2 = 'cl' then amount else 0 end) as level_2_cl
	,sum(case when level = '2' and level_2 = 'ncl' then amount else 0 end) as level_2_ncl
	,sum(case when level = '2' and level_2 = 'eq' then amount else 0 end) as level_2_eq
	,sum(case when level = '2' and level_2 = 'p' then amount else 0 end) as level_2_p
	from t1
	group by 1,2,3,4,5,6,7
	),

t3 as (
	select 
	rank() over (partition by adsh order by ddate desc) as rnk
	,t2.*
	,level_1_a + level_1_l as level_1_bs_chk
	,level_1_a - level_2_ca - level_2_nca as level_2_a_chk
	,level_1_l - level_2_cl - level_2_ncl - level_2_eq as level_2_l_chk
	-- infering of missing balances to go in here
	from t2
	),
	
t4 as (	
	select 
	case 
		when qtrs = 0 then 'pit'
		when qtrs::text = qtr or (qtrs::text = '4' and qtr = 'Y') then 'ytd_pl'
		else 'na'
		end as bal_type
	,t3.*
	from t3
	where rnk = 1
	and case 
		when qtrs = 0 then 'pit'
		when qtrs::text = qtr or (qtrs::text = '4' and qtr = 'Y') then 'ytd_pl'
		else 'na'
		end != 'na'
	)

select 
level_1_p - case when bal_type = 'ytd_pl' and qtrs > 1 
				then lag(level_1_p) over (partition by stock, bal_type order by ddate) 
				else 0 
				end as net_income_qtly
,t4.*
from t4
;






select nm.tag, lk.lookup_ref, nm.value 
from edgar.num nm
left join edgar.lookup lk
on nm.tag = lk.lookup_ref
where adsh = '0000019617-19-000232';  --0001564590-20-010833 / 0000008670-19-000013 / 0000019617-19-000232


select distinct tag from edgar.num where qtrs = 0 and lower(tag) like '%cash%';