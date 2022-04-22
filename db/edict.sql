-- Does the progress bar work?
SET enable_progress_bar=true;

--
-- Entity dictionary
--
BEGIN TRANSACTION;

CREATE TABLE edict(eid UINTEGER, e VARCHAR, ef UINTEGER);

INSERT INTO edict
SELECT row_number() OVER (), text, ef
FROM
  (SELECT text, count(*) as ef
   FROM 'msmarco_doc_00.parquet'
   GROUP by text
   ORDER by ef DESC
  );

COMMIT;
