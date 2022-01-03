-- What's in the Parquet Files?
SELECT path_in_schema, type, stats_min_value, stats_max_value FROM parquet_metadata('msmarco_doc_00.parquet') WHERE row_group_id = 0;
SELECT path_in_schema, type, stats_min_value, stats_max_value FROM parquet_metadata('msmarco_doc_00_field_mapping.parquet') WHERE row_group_id = 0;
