select * from edgar.lookup;

--=============================================================================
-- Create tag hierarchy table
-- https://stackoverflow.com/questions/14083311/permission-denied-when-trying-to-import-a-csv-file-from-pgadmin
-- add IndefiniteLivedLicenseAgreements & OtherIntangibleAssetsNet to 'intang' per 0000732717-17-000021
-- add LongTermDebtAndCapitalLeaseObligations re 0000732717-17-000021
--=============================================================================

drop table if exists edgar.lookup_csv cascade;

create table edgar.lookup_csv
	(
		lookup_table	text	
		,lookup_ref 	text
		,lookup_val1	text
		,lookup_val2	text
		,lookup_val3	text
		,lookup_val4	text
		,lookup_val5	text
		,lookup_val6	text
		,lookup_val7	text
	);

alter table edgar.lookup_csv owner to postgres;

copy edgar.lookup_csv 
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\edgar_lookups.csv' 
delimiter ',' csv header;





--=============================================================================
-- Derive depreciation and amortisation
--=============================================================================

drop table if exists edgar.lookup cascade;

create table edgar.lookup as 
	select * from edgar.lookup_csv
	
	union all 
	
	select
	'tag_mapping' as lookup_table
	,tag 
	,'1' as lookup_val1
	,'1' as lookup_val2
	,'3' as lookup_val3
	,'na' as lookup_val4
	,'na' as lookup_val5
	,'depr_amort' as lookup_val6
	,'na' as lookup_val7
	from 
		(
			select distinct stmt, tag 
			from edgar.pre 
			where stmt = 'CF' 
			and (lower(tag) like '%depletion%' or lower(tag) like '%depreciation%' or lower(tag) like '%amort%')
			and (lower(tag) like '%intang%' or lower(tag) like '%goodwill%')
			order by stmt, tag
		) as lookup_ref
;

