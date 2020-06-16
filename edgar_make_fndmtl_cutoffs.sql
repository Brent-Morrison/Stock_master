select * from edgar.fndmtl_cutoffs;

/******************************************************************************

Create view containing size cut-offs based on total assets and equity
Financials have signicantly larger assets then non financials, therefore 
assess separately

******************************************************************************/

drop materialized view if exists edgar.fndmtl_cutoffs;

create materialized view edgar.fndmtl_cutoffs as 

	with t1 as (
		select 
		nm.*
		,sb.filed 
		,case when lk.lookup_val2 = 'Office of Finance' then 'financial' else 'non_financial' end as fin_nonfin
		,rank() over (partition by nm.adsh order by nm.ddate desc) as fltr
		from edgar.num nm
		left join edgar.sub sb
		on nm.adsh = sb.adsh
		left join edgar.lookup lk
		on sb.sic = lk.lookup_ref::int
		and lk.lookup_table = 'sic_mapping'
		where nm.tag in ('Assets','StockholdersEquity')
		-- coreg filter to avoid duplicates
		and nm.coreg = 'NVS'
		-- Filter forms 10-K/A, 10-Q/A being restated filings
		and sb.form in ('10-K', '10-Q')
		and nm.uom = 'USD'
		and nm.value is not null
		and nm.qtrs = 0
		),
		
	t2 as (
		select
		fin_nonfin
		,tag
		,sec_qtr 
		,adsh
		,filed
		,value
		,rank() over (partition by sec_qtr, tag, fin_nonfin order by value desc) as rnk
		from t1
		where 1 = 1
		and fltr = 1
		)
	
	select 
	t2.* 
	,max(filed) over() as max_file_date
	from t2 
	where rnk in (
		100,200,300,400,500,600,700,800,900,1000,1100,
		1200,1300,1400,1500,1600,1700,1800,1900,2000
		) 
;



/******************************************************************************

Create view containing size cut-offs based on total assets and equity
Financials have signicantly larger assets then non financials, therefore 
assess separately - TAKE TWO
check AT&T 2017 for no total liabilities, adsh = '0000732717-17-000021'

******************************************************************************/

	with t1 as (
		select 
		nm.adsh
		,nm.version
		,nm.ddate
		,nm.qtrs
		,nm.uom
		,nm.coreg
		,nm.sec_qtr
		,case when nm.tag = 'Assets' then nm.value else 0 end as assets
		,(case when nm.tag = 'Assets' then nm.value else 0 end)
			- (case when nm.tag in ('Liabilities','Totalliabilities') then nm.value else 0 end) as equity1
		,case when nm.tag in ('StockholdersEquity','MinorityInterest') then nm.value else 0 end as equity2
		,sb.filed 
		,case when lk.lookup_val2 = 'Office of Finance' then 'financial' else 'non_financial' end as fin_nonfin
		,rank() over (partition by nm.adsh order by nm.ddate desc) as fltr
		from edgar.num nm
		left join edgar.sub sb
		on nm.adsh = sb.adsh
		left join edgar.lookup lk
		on sb.sic = lk.lookup_ref::int
		and lk.lookup_table = 'sic_mapping'
		where nm.tag in ('Assets','StockholdersEquity','MinorityInterest', 'Liabilities', 'Totalliabilities')
		-- coreg filter to avoid duplicates
		and nm.coreg = 'NVS'
		-- Filter forms 10-K/A, 10-Q/A being restated filings
		and sb.form in ('10-K', '10-Q')
		and nm.uom = 'USD'
		and nm.value is not null
		and nm.qtrs = 0
		--and nm.adsh = '0000012927-17-000006' 		-- FILTER FOR INVESTIGATION
		),
		
	t2 as (
		select
		fin_nonfin
		,sec_qtr 
		,adsh
		,filed
		,sum(assets) as assets 
		,sum(case when equity1 < assets then equity1
				  else equity2 end) as equity
		from t1
		where 1 = 1
		and fltr = 1
		group by 1,2,3,4
		),
	
	t3 as (
		select 
		t2.*
		,rank() over (partition by sec_qtr, fin_nonfin order by assets desc) as asset_rank
		,rank() over (partition by sec_qtr, fin_nonfin order by equity desc) as equity_rank
		from t2
		),
		
	t4 as (
		select
		t3.* 
		,asset_rank + equity_rank as sum_rank
		from t3
		),
	
	t5 as (
		select
		t4.*
		,rank() over (partition by sec_qtr, fin_nonfin order by sum_rank asc) as combined_rank
		from t4
		)
	
	select
	t5.*
	,max(filed) over() as max_file_date
	from t5 
	where (combined_rank <= 1800 and fin_nonfin = 'non_financial')
	or (combined_rank <= 200 and fin_nonfin = 'financial')
;
