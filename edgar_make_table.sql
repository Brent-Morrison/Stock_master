--=============================================================================
-- Sub table (staging)
--=============================================================================

--DROP TABLE edgar.sub_stage;

CREATE TABLE edgar.sub_stage
(
	adsh char (20) NOT NULL,
	cik integer NOT NULL,
	name text NOT NULL,
	sic smallint,
	countryba char (2),
	stprba char (2),
	cityba text,
	zipba varchar (10),
	former text,
	changed integer,  				-- cast to date
	afs char (5),
	wksi integer NOT NULL,  		-- cast to boolean
	fye integer,
	form varchar (10) NOT NULL,
	period integer NOT NULL,  		-- cast to date
	fy smallint NOT NULL,
	fp char (2) NOT NULL,
	filed integer NOT NULL, 		-- cast to date
	prevrpt integer NOT NULL, 		-- cast to boolean
	detail integer NOT NULL,
	instance varchar (32) NOT NULL,
	nciks smallint NOT NULL,
	aciks text
);

ALTER TABLE edgar.sub_stage OWNER to postgres;




--=============================================================================
-- Tag table (staging)
--=============================================================================

DROP TABLE edgar.tag_stage;

CREATE TABLE edgar.tag_stage
(
	tag text NOT NULL,
	version varchar (20) NOT NULL,
	custom integer NOT NULL,		-- cast to boolean
	abstract integer NOT NULL,		-- cast to boolean
	datatype varchar (20),
	iord char (1),
	crdr char (1),
	tlabel text,
	doc text
);

ALTER TABLE edgar.tag_stage OWNER to postgres;




--=============================================================================
-- Num table (staging)
--=============================================================================

DROP TABLE edgar.num_stage;

CREATE TABLE edgar.num_stage
(
	adsh char (20) NOT NULL,
	tag text NOT NULL,
	version varchar (20) NOT NULL,
	ddate integer NOT NULL,			-- cast to date
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

SELECT * FROM edgar.sub_stage;
SELECT * FROM edgar.tag_stage;