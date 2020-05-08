INSERT INTO edgar.sub_bad
	SELECT *
	FROM edgar.sub_stage AS s
	WHERE EXISTS (
		SELECT 'x'
		FROM edgar.sub AS f
		WHERE f.adsh = s.adsh
		)
;

INSERT INTO edgar.sub
	SELECT 
	COALESCE(adsh, 'NVS')
	,cik
	,name
	,sic
	,countryba
	,stprba
	,cityba
	,zipba
	,former
	,TO_DATE(changed::text, 'YYYYMMDD')
	,afs
	,CAST(wksi AS BOOLEAN)
	,fye
	,form
	,TO_DATE(period::text, 'YYYYMMDD')
	,fy
	,fp
	,TO_DATE(filed::text, 'YYYYMMDD')
	,CAST(prevrpt AS BOOLEAN)
	,detail
	,instance
	,nciks
	,aciks
	FROM edgar.sub_stage
ON CONFLICT (adsh) 
DO NOTHING
;



INSERT INTO edgar.tag_bad
	SELECT *
	FROM edgar.tag_stage AS s
	WHERE EXISTS (
		SELECT 'x'
		FROM edgar.tag AS f
		WHERE f.tag = s.tag
		AND f.version = s.version
		)
;

INSERT INTO edgar.tag
	SELECT 
	COALESCE(tag, 'NVS')
	,COALESCE(version, 'NVS')
	,CAST(custom AS BOOLEAN)
	,CAST(abstract AS BOOLEAN)
	,datatype
	,iord
	,crdr
	,tlabel
	,doc
	FROM edgar.tag_stage
ON CONFLICT (tag, version) 
DO NOTHING
;



INSERT INTO edgar.num_bad
	SELECT *
	FROM edgar.num_stage AS s
	WHERE EXISTS (
		SELECT 'x'
		FROM edgar.num AS f
		WHERE f.adsh = s.adsh
		AND f.tag = s.tag
		AND f.version = s.version
		AND f.ddate = to_date(s.ddate::text, 'YYYYMMDD')
		AND f.qtrs = s.qtrs
		AND f.uom = s.uom
		AND f.coreg = s.coreg
		)
;

INSERT INTO edgar.num
	SELECT 
	adsh
	,tag
	,version
	,to_date(ddate::text, 'YYYYMMDD')
	,qtrs
	,uom
	,COALESCE(coreg, 'NVS')
	,value
	,footnote
	FROM edgar.num_stage
ON CONFLICT (adsh, tag, version, ddate, qtrs, uom, coreg) 
DO NOTHING
;