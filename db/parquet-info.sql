-- What's in the Parquet Files?
SELECT * FROM parquet_metadata('doc-ok.parquet') WHERE row_group_id = 0;
SELECT * FROM parquet_metadata('doc-ok_field_mapping.parquet');
