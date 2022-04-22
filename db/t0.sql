-- First ten rows
SELECT * -- identifier, text, tag 
FROM 'doc-ok.parquet'
WHERE field=2
LIMIT 10;
