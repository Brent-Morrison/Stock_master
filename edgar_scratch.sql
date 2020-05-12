
SELECT sec_qtr, count(*) FROM edgar.tag GROUP BY 1 ORDER by 1;

SELECT sec_qtr, count(*) FROM edgar.tag_stage GROUP BY 1 ORDER by 1;

SELECT * FROM edgar.tag_bad WHERE tag = 'ZA';

SELECT * FROM edgar.tag WHERE tag = 'ZA';

SELECT sec_qtr, count(*) FROM edgar.num WHERE tag = 'EntityCommonStockSharesOutstanding' GROUP BY 1; -- limit 50;

SELECT sum(records) FROM (
	SELECT count(tag) AS records, 'final' AS tbl_source FROM edgar.tag WHERE sec_qtr = '2017q3'
	UNION ALL
	SELECT count(tag) AS records, 'bad_data' AS tbl_source FROM edgar.tag_bad WHERE sec_qtr = '2017q3'
	) t1;

SELECT * FROM edgar.tag WHERE lower(tag) LIKE '%accountspayable%';

SELECT * FROM edgar.tag WHERE tag = 'AllowanceForDoubtfulAccountsReceivableCurrent';

SELECT
tag
,value/1000000
FROM edgar.num 
WHERE adsh = '0001564590-20-010833'
AND ddate = '2020-02-29'
AND qtrs IN (0,1)
AND tag IN (
	'CommonStockSharesOutstanding',
	'AssetsCurrent',
	'AssetsNoncurrent',
	'Goodwill',
	'LiabilitiesCurrent',
	'LiabilitiesNoncurrent',
	'StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'
	)
;