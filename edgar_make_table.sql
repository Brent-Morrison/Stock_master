--=============================================================================
-- Sub table (staging)
--=============================================================================

DROP TABLE edgar.sub_stage;

CREATE TABLE edgar.sub_stage
(
	adsh char (20) NOT NULL,
	cik integer NOT NULL,
	name text NOT NULL,
	sic smallint,
	countryba char (2) NOT NULL,
	stprba char (2),
	cityba text NOT NULL,
	zipba varchar (10),
	former text,
	changed date,
	afs char (5),
	wksi boolean NOT NULL,
	fye integer NOT NULL,
	form varchar (10) NOT NULL,
	period date NOT NULL,
	fy smallint NOT NULL,
	fp char (2) NOT NULL,
	filed date NOT NULL,
	prevrpt boolean NOT NULL,
	detail boolean NOT NULL,
	instance varchar (32) NOT NULL,
	nciks smallint NOT NULL,
	aciks text
);

ALTER TABLE edgar.sub_stage OWNER to postgres;




--=============================================================================
-- Tag table (staging)
--=============================================================================

--DROP TABLE edgar.tag_stage;

CREATE TABLE edgar.tag_stage
(
	tag text NOT NULL,
	version varchar (20) NOT NULL,
	custom boolean NOT NULL,
	abstract boolean NOT NULL,
	datatype varchar (20),
	iord char (1),
	crdr char (1),
	tlabel text,
	foc text
);

ALTER TABLE edgar.tag_stage OWNER to postgres;




--=============================================================================
-- Num table (staging)
--=============================================================================

--DROP TABLE edgar.pre_stage;

CREATE TABLE edgar.pre_stage
(
	adsh char (20) NOT NULL,
	report integer NOT NULL,
	line integer NOT NULL,
	stmt varchar (2) NOT NULL,
	inpth boolean NOT NULL,
	rfile char (1) NOT NULL,
	tag text NOT NULL,
	version varchar (20) NOT NULL,
	plabel text NOT NULL,
	negating boolean
);

ALTER TABLE edgar.pre_stage OWNER to postgres;




--=============================================================================
-- Num table (staging)
--=============================================================================

DROP TABLE edgar.num_stage;

CREATE TABLE edgar.num_stage
(
	adsh char (20) NOT NULL,
	tag text NOT NULL,
	version varchar (20) NOT NULL,
	ddate date NOT NULL,
	qtrs smallint NOT NULL,
	uom varchar (20) NOT NULL,
	coreg text,
	value numeric,
	footnote text
);

ALTER TABLE edgar.num_stage OWNER to postgres;

--INSERT INTO edgar.num_stage VALUES ('TEST', 'TEST', 'TEST', 'TEST', 1000, 'TEST', 'TEST', 2000, 'some text');




--=============================================================================
-- Load data directly from file
--=============================================================================

--COPY edgar.num_stage (adsh, tag, version, coreg, ddate, qtrs, uom, value, footnote) 
--FROM 'C:\Users\brent\Downloads\num.txt' DELIMITER E'\t' CSV HEADER;

select * from edgar.sub_stage;