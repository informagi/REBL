import duckdb

print(duckdb.query('''
SELECT *
FROM 'doc-error.parquet'
''').fetchall())
