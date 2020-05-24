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
		where nm.tag in ('Assets','StockholdersEquity')--,'NetIncomeLoss')
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