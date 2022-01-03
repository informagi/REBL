import duckdb

print(duckdb.query('''
SELECT *
FROM 'doc-ok.parquet'
''').fetchall())
