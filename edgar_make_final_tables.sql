--=============================================================================
-- Sub table (final)
--=============================================================================

DROP TABLE IF EXISTS edgar.sub;

CREATE TABLE edgar.sub
	(
		adsh 			char (20)       PRIMARY KEY
		,cik 			integer
		,name 			text
		,sic 			smallint
		,countryba 		char (2)
		,stprba 		char (2)
		,cityba 		text
		,zipba 			varchar (10)
		,former 		text
		,changed 		date			-- cast to date
		,afs 			char (5)
		,wksi 			boolean 		-- cast to boolean
		,fye 			integer
		,form 			varchar (10)
		,period 		date      		-- cast to date
		,fy 			smallint
		,fp 			char (2)
		,filed 			date    		-- cast to date
		,prevrpt 		boolean 		-- cast to boolean
		,detail 		integer
		,instance 		varchar (32)
		,nciks 			smallint
		,aciks 			text
		,sec_qtr 		char (6)		NOT NULL
	);

ALTER TABLE edgar.sub OWNER to postgres;




--=============================================================================
-- Tag table (final)
--=============================================================================

DROP TABLE IF EXISTS edgar.tag;

CREATE TABLE edgar.tag
	(
		tag 			text
		,version 		varchar (20)
		,custom 		boolean			-- cast to boolean
		,abstract 		boolean			-- cast to boolean
		,datatype 		varchar (20)
		,iord 			char (1)
		,crdr 			char (1)
		,tlabel 		text
		,doc 			text
		,sec_qtr 		char (6)		NOT NULL
        ,PRIMARY KEY(tag, version)
	);

ALTER TABLE edgar.tag OWNER to postgres;




--=============================================================================
-- Num table (final)
--=============================================================================

DROP TABLE IF EXISTS edgar.num;

CREATE TABLE edgar.num
	(
		adsh 			char (20)
		,tag 			text
		,version 		varchar (20)
		,ddate 			date			-- cast to date
		,qtrs 			smallint
		,uom 			varchar (20)
		,coreg 			text
		,value 			numeric
		,footnote 		text
		,sec_qtr 		char (6)		NOT NULL
        ,PRIMARY KEY    (adsh, tag, version, ddate, qtrs, uom, coreg)
	);

ALTER TABLE edgar.num OWNER to postgres;
