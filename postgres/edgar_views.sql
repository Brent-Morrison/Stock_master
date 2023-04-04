/******************************************************************************
* 
* Create ticker & cik list based on "instance record" in the sub table
* 
* DEFECTS
* - none
* 
******************************************************************************/

select * from edgar.cik_ticker

create or replace view edgar.cik_ticker as 

select 
sb.cik
,sb.sub_tkr_1 as ticker
/*
,count(cik) over (partition by cik, sub_tkr_1) as ticker_1_count
,ct.ct_tickers
,case 
	when sb.sub_tkr_1 = any(ct.ct_tickers) 
	and  sb.sub_tkr_2 = any(ct.ct_tickers) then 1 
	else 0 
	end as filter1
,case 
	when sub_tkr_2 !~ '[[:digit:]]' then 1
	else 0
	end as filter2

*/
from 
	(	-- CIK and ticker (embedded in the "instance" field) from the edgar sub table
		select 
	    cik
	    ,upper(substring(instance, '[A-Za-z]{1,5}')) as sub_tkr_1
	    ,length(substring(instance, '[A-Za-z]{1,5}')) as sub_tkr_1_len
	    ,upper(left(instance, position('-' in instance)-1)) as sub_tkr_2
	    ,length(left(instance, position('-' in instance)-1)) as sub_tkr_2_len
		from edgar.sub
		where form in ('10-K', '10-Q')
		and afs = '1-LAF'
		and length(left(instance, position('-' in instance)-1)) < 6
		and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FY'
		and upper(substring(instance, '[A-Za-z]{1,5}')) != 'FORM'
		group by 1,2,3,4,5
		--order by 1
	) sb

full outer join 
	(	-- Company tickers data from sec website
		-- https://www.sec.gov/files/company_tickers.json
		-- Refer edgar_import.py
	    select 
	    cik_str
	    ,array_agg(ticker) as ct_tickers
	    from edgar.company_tickers
	    group by cik_str
    ) ct
on sb.cik = ct.cik_str

where 
	sub_tkr_2 !~ '[[:digit:]]'
	and sb.sub_tkr_1 = any(ct.ct_tickers) 
	and sb.sub_tkr_2 = any(ct.ct_tickers)
;
