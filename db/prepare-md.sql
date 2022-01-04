--
-- Transform MD tables
--

-- SET enable_progress_bar=true;

--
-- Define types
--

BEGIN TRANSACTION;

-- Fields ("field", "tag") should be ENUMs
CREATE TYPE tags AS ENUM ('PER','LOC','ORG', 'MISC');
CREATE TYPE fields AS ENUM ('title','headings','body');

-- MD field conversion table
-- Read field values from the field_mappings.parquet file
CREATE TABLE fielddict (
	id    TINYINT,
	field fields
);
INSERT INTO fielddict 
SELECT stats_min_value, path_in_schema
FROM parquet_metadata('msmarco_doc_00_field_mapping.parquet');

COMMIT;

--
-- Create the Document Data Dictionary
--

BEGIN TRANSACTION;

-- Data Dictionary
CREATE TABLE dict(
	cpart UTINYINT, 
	docid UINTEGER, 
	identifier VARCHAR,
	nent USMALLINT, 
	PRIMARY KEY(cpart, docid)
);

-- Create the document identifiers (from the Parquet File)
INSERT INTO dict
SELECT
  cpdocid[0]::UTINYINT AS cpart,
  cpdocid[1]::UINTEGER AS docid,
  identifier,
  count(*) AS nent
FROM
  (SELECT 
     string_split(replace(identifier,'msmarco_doc_',''),'_') as cpdocid,
     identifier
   FROM 'msmarco_doc_00.parquet' 
  )
GROUP BY cpart, docid, identifier
ORDER BY docid;

COMMIT;

--
-- Entity dictionary
--
BEGIN TRANSACTION;

CREATE TABLE edict(
	eid UINTEGER PRIMARY KEY, 
	e VARCHAR, 
	ef UINTEGER);

INSERT INTO edict
SELECT row_number() OVER (), text, ef
FROM
  (SELECT text, count(*) as ef
   FROM 'msmarco_doc_00.parquet'
   GROUP by text
   ORDER by ef DESC
  );

CREATE INDEX e_idx ON edict(e);

COMMIT;

--
-- The Document-Entity Table
--

BEGIN TRANSACTION;

-- Document Entity
CREATE TABLE doc(
	cpart UTINYINT,
	docid UINTEGER,
	field fields,
	e UINTEGER,
	start_pos UINTEGER,
	end_pos UINTEGER,
	score DOUBLE,
	tag tags
);

--
-- Load and recode the data:
-- 
--   + Document identifiers are looked up in the data dictionary
--   + Field and tag are mapped to their ENUM types
--
INSERT INTO doc
SELECT 
  d.cpart, 
  d.docid,
  fd.field,
  ed.eid,
  docs.start_pos,
  docs.end_pos,
  docs.score,
  docs.tag
FROM dict d, 'msmarco_doc_00.parquet' docs, fielddict fd, edict ed
WHERE 
      d.identifier = docs.identifier
  AND fd.id = docs.field
  AND ed.e  = docs.text;

CREATE INDEX de_idx ON doc(e);

COMMIT;
