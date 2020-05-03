
--=============================================================================
-- Create staging table for test
--=============================================================================

DROP TABLE IF EXISTS edgar.num_stage_test;

CREATE TABLE edgar.num_stage_test
	(
		adsh 			char (20)
		,tag 			text
		,version 		varchar (20)
		,ddate 			integer
		,qtrs 			smallint
		,uom 			varchar (20)
		,coreg 			text
		,value 			numeric
		,footnote 		text
	);

ALTER TABLE edgar.num_stage_test OWNER TO postgres;

-- Insert dummy data
INSERT INTO edgar.num_stage_test
SELECT * FROM edgar.num_stage LIMIT 5;

SELECT * FROM edgar.num_stage_test;


--=============================================================================
-- Create FINAL table for test (this has a primary key)
--=============================================================================

DROP TABLE IF EXISTS edgar.num_final_test;

CREATE TABLE edgar.num_final_test
	(
		adsh 			char (20)
		,tag 			text
		,version 		varchar (20)
		,ddate 			date
		,qtrs 			smallint
		,uom 			varchar (20)
		,coreg 			text
		,value 			numeric
		,footnote 		text
		,PRIMARY KEY(adsh, tag, version, ddate, qtrs, uom)
	);

ALTER TABLE edgar.num_final_test OWNER TO postgres;

-- Insert two records from the test staging table, these two records should not be replaced
INSERT INTO edgar.num_final_test
	SELECT 
	adsh
	,tag
	,version
	,to_date(ddate::text, 'YYYYMMDD')
	,qtrs
	,uom
	,coreg
	,value
	,footnote
	FROM edgar.num_stage_test LIMIT 2
;

SELECT * FROM edgar.num_final_test;


--=============================================================================
-- Create BAD DATA table for test, this is where we want duplicates to be
-- directed to
--=============================================================================

DROP TABLE IF EXISTS edgar.num_stage_test_bd;

CREATE TABLE edgar.num_stage_test_bd
	(
		adsh 			char (20)
		,tag 			text
		,version 		varchar (20)
		,ddate 			integer
		,qtrs 			smallint
		,uom 			varchar (20)
		,coreg 			text
		,value 			numeric
		,footnote 		text
	);

ALTER TABLE edgar.num_stage_test_bd OWNER to postgres;

SELECT * FROM edgar.num_stage_test_bd;

-- Insert duplicates in to BAD DATA table
INSERT INTO edgar.num_stage_test_bd
	SELECT *
	FROM edgar.num_stage_test AS s
	WHERE EXISTS (
		SELECT 'x'
		FROM edgar.num_final_test AS f
		WHERE f.adsh = s.adsh
		AND f.tag = s.tag
		AND f.version = s.version
		AND f.ddate = to_date(s.ddate::text, 'YYYYMMDD')
		AND f.qtrs = s.qtrs
		AND f.uom = s.uom
		)
;

SELECT * FROM edgar.num_stage_test_bd;



--=============================================================================
-- Insert new data to FINAL table, some of which is a duplicate
--=============================================================================

-- Insert all records from the test staging table, these two records should not be replaced
INSERT INTO edgar.num_final_test
	SELECT 
	adsh
	,tag
	,version
	,to_date(ddate::text, 'YYYYMMDD')
	,qtrs
	,uom
	,coreg
	,value
	,footnote
	FROM edgar.num_stage_test
ON CONFLICT (adsh, tag, version, ddate, qtrs, uom) 
DO NOTHING
;

SELECT * FROM edgar.num_final_test;

SELECT * FROM edgar.tag_stage;

SELECT * FROM edgar.num_bad;


