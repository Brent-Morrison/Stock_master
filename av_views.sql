/******************************************************************************
* 
* Create view for tickers to update price data
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
where ctm.ticker not in (select distinct ticker from alpha_vantage.ticker_excl) 
group by 1
;




/******************************************************************************
*  
* Create view to extract price data for attribute creation
*  
* ERRORS
* to be amended to take account of multiple records for same day
*  
******************************************************************************/

select * from alpha_vantage.returns_view where symbol = 'A'

create or replace view alpha_vantage.returns_view as 

select 
sd.symbol,
sp5.gics_sector,
sd."timestamp",
sd.close,
sd.adjusted_close,
sd.volume,
sp.sp500
from alpha_vantage.shareprices_daily sd
join alpha_vantage.sp_500 sp5 
on sd.symbol = sp5.symbol
left join (
	select 
	shareprices_daily."timestamp",
    shareprices_daily.adjusted_close as sp500
    from alpha_vantage.shareprices_daily
    where shareprices_daily.symbol = 'gspc'::text) sp 
on sd."timestamp" = sp."timestamp"
where sd."timestamp" > '2018-01-01'::date
order by 
sd.symbol, 
sd."timestamp", 
sp5.gics_sector
;


 
-- NEW 
-- Add sector to "edgar.cik_ticker_master" table
with fltr as (
	select 
	ticker
	,sic
	,min(valid_year) as min_yr
	,max(valid_year) as max_yr
	from edgar.cik_ticker_master
	group by 1,2
	)

,sd as (
	select * from (
		select 
		symbol
		,"timestamp"
		,close
		,adjusted_close
		,volume
		,row_number() over (partition by "timestamp", symbol order by capture_date desc) as row_num
		from alpha_vantage.shareprices_daily
		where symbol != 'GSPC'
		order by 
		symbol
		,"timestamp" asc
		) t1
	where row_num = 1 and symbol = 'A'
	)

,sp5 as (
	select 
	"timestamp"
	,adjusted_close as sp500
	from alpha_vantage.shareprices_daily
	where shareprices_daily.symbol = 'GSPC'
	and adjusted_close is not null
	and extract(year from "timestamp") >= (select min(min_yr) from fltr) - 1
	)


select 
sd.symbol
,fltr.sic as gics_sector
,sd."timestamp"
,sd.close
,sd.adjusted_close
,sd.volume
,sp5.sp500
from fltr
inner join sd
on fltr.ticker = sd.symbol
right join sp5
on sd."timestamp" = sp5."timestamp"
and extract(year from sd."timestamp") >= fltr.min_yr - 1
and extract(year from sd."timestamp") <= fltr.max_yr
order by 
sd.symbol
,sd."timestamp"
--,gics_sector
;  