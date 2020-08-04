/******************************************************************************
* 
* Create view
* 
* ERRORS
* none
* 
******************************************************************************/
drop view alpha_vantage.tickers_to_update;

create or replace view alpha_vantage.tickers_to_update as 

select 
ctm.ticker as symbol
,max(av.max_date) as last_date_in_db
,max(av.adjusted_close) as last_adj_close
from edgar.cik_ticker_master ctm
left join (
	select 
	distinct on (symbol) symbol
	,timestamp as max_date
	,adjusted_close
	from alpha_vantage.shareprices_daily
	order by symbol, timestamp desc
	) av
on ctm.ticker = av.symbol
group by 1
;

select * from alpha_vantage.tickers_to_update
where symbol in ('AAPL','AAT');