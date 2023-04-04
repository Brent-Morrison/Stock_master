--=============================================================================
-- Sub table (staging)
--=============================================================================

drop table if exists edgar.sub_stage;

create table edgar.sub_stage
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
		,sec_qtr 		char (6)
	);

alter table edgar.sub_stage owner to postgres;




--=============================================================================
-- Tag table (staging)
--=============================================================================

drop table if exists edgar.tag_stage;

create table edgar.tag_stage
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
		,sec_qtr 		char (6)
	);

alter table edgar.tag_stage owner to postgres;




--=============================================================================
-- Num table (staging)
--=============================================================================

drop table if exists edgar.num_stage;

create table edgar.num_stage
	(
		adsh 			char (20)
		,tag 			text
		,version 		varchar (20)
		,ddate 			integer			-- cast to date
		,qtrs 			smallint
		,uom 			varchar (20)
		,coreg 			text
		,value 			numeric
		,footnote 		text
		,sec_qtr 		char (6)
	);

alter table edgar.num_stage owner to postgres;




--=============================================================================
-- Bad data tables
--=============================================================================

drop table if exists edgar.sub_bad;
create table edgar.sub_bad (like edgar.sub_stage including all);
alter table edgar.sub_bad owner to postgres;

drop table if exists edgar.tag_bad;
create table edgar.tag_bad (like edgar.tag_stage including all);
alter table edgar.tag_bad owner to postgres;

drop table if exists edgar.num_bad;
create table edgar.num_bad (like edgar.num_stage including all);
alter table edgar.num_bad owner to postgres;




--=============================================================================
-- Sub table (final)
--=============================================================================

drop table if exists edgar.sub;

create table edgar.sub
	(
		adsh 			char (20)       primary key
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
		,sec_qtr 		char (6)		not null
	);

alter table edgar.sub owner to postgres;




--=============================================================================
-- Tag table (final)
--=============================================================================

drop table if exists edgar.tag;

create table edgar.tag
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
		,sec_qtr 		char (6)		not null
        ,primary key(tag, version)
	);

alter table edgar.tag owner to postgres;




--=============================================================================
-- Num table (final)
--=============================================================================

drop table if exists edgar.num;

create table edgar.num
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
		,sec_qtr 		char (6)		not null
        ,primary key    (adsh, tag, version, ddate, qtrs, uom, coreg)
	);

alter table edgar.num owner to postgres;




--=============================================================================
-- Manual load of "pre" table
--=============================================================================

drop table if exists edgar.pre;

create table edgar.pre
	(
		adsh 			char (20)
		,report 		integer
		,line 			integer
		,stmt 			varchar (2)
		,inpth 			boolean 
		,rfile 			char (1)
		,tag 			text
		,version 		varchar (20)
		,plabel 		text
		,negating 		boolean 
	);

alter table edgar.pre owner to postgres;

copy edgar.pre 
from 'C:\Users\brent\Documents\VS_Code\postgres\postgres\pre2020q1.txt' 
delimiter e'\t' csv header;




--=============================================================================
-- SEC symbols table
--=============================================================================

drop table if exists edgar.company_tickers;

create table edgar.company_tickers
	(
		cik_str 		integer
		,ticker			varchar (10)
		,title 			text
	);

alter table edgar.company_tickers owner to postgres;

select * from edgar.company_tickers