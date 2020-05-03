--=============================================================================
-- Sub table (staging)
--=============================================================================

DROP TABLE IF EXISTS edgar.sub_stage;

CREATE TABLE edgar.sub_stage
	(
		adsh 			char (20)
		,cik 			integer
		,name 			text
		,sic 			smallint
		,countryba 		char (2)
		,stprba 		char (2)
		,cityba 		text
		,zipba 			varchar (10)
		,former 		text
		,changed 		integer			-- cast to date
		,afs 			char (5)
		,wksi 			integer 		-- cast to boolean
		,fye 			integer
		,form 			varchar (10)
		,period 		integer  		-- cast to date
		,fy 			smallint
		,fp 			char (2)
		,filed 			integer 		-- cast to date
		,prevrpt 		integer 		-- cast to boolean
		,detail 		integer
		,instance 		varchar (32)
		,nciks 			smallint
		,aciks 			text
	);

ALTER TABLE edgar.sub_stage OWNER to postgres;




--=============================================================================
-- Tag table (staging)
--=============================================================================

DROP TABLE IF EXISTS edgar.tag_stage;

CREATE TABLE edgar.tag_stage
	(
		tag 			text
		,version 		varchar (20)
		,custom 		integer			-- cast to boolean
		,abstract 		integer			-- cast to boolean
		,datatype 		varchar (20)
		,iord 			char (1)
		,crdr 			char (1)
		,tlabel 		text
		,doc 			text
	);

ALTER TABLE edgar.tag_stage OWNER to postgres;




--=============================================================================
-- Num table (staging)
--=============================================================================

DROP TABLE IF EXISTS edgar.num_stage;

CREATE TABLE edgar.num_stage
	(
		adsh 			char (20)
		,tag 			text
		,version 		varchar (20)
		,ddate 			integer			-- cast to date
		,qtrs 			smallint
		,uom 			varchar (20)
		,coreg 			text,
		,value 			numeric,
		,footnote 		text
	);

ALTER TABLE edgar.num_stage OWNER to postgres;




--=============================================================================
-- Bad data tables
--=============================================================================

CREATE TABLE edgar.sub_bad (LIKE edgar.sub_stage INCLUDING ALL);
ALTER TABLE edgar.sub_bad OWNER to postgres;

CREATE TABLE edgar.tag_bad (LIKE edgar.tag_stage INCLUDING ALL);
ALTER TABLE edgar.tag_bad OWNER to postgres;

CREATE TABLE edgar.num_bad (LIKE edgar.num_stage INCLUDING ALL);
ALTER TABLE edgar.num_bad OWNER to postgres;




--=============================================================================
-- Load data directly from file
--=============================================================================

--COPY edgar.num_stage (adsh, tag, version, coreg, ddate, qtrs, uom, value, footnote) 
--FROM 'C:\Users\brent\Downloads\num.txt' DELIMITER E'\t' CSV HEADER;




--=============================================================================
-- Test
--=============================================================================

SELECT COUNT(*) FROM edgar.sub_stage;
SELECT COUNT(*) FROM edgar.tag_stage;
SELECT COUNT(*) FROM edgar.num_stage;

DELETE FROM edgar.sub_stage;
DELETE FROM edgar.tag_stage;
DELETE FROM edgar.num_stage;

--INSERT INTO edgar.num_stage VALUES ('TEST', 'TEST', 'TEST', 'TEST', 1000, 'TEST', 'TEST', 2000, 'some text');