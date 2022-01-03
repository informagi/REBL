# DuckDB experiments

## Preliminaries

1.
DuckDB has been compiled from source and installed.

2.
Mention detection has run and created parquet files.

3.
The parquet files are stored in `/export/data2/tmp`,
or another location specified as working directory 
in `md.init`.

## CLI

### Sanity check

    duckdb md.init < t0.sql
    duckdb md.init < 

### Another sanity check

Copy output over:

    scp tusi:/scratch/ckamphuis/el-msmarcov2/msmarco_v2_md/msmarco_doc_00* /export/data2/tmp

    duckdb md.init < t2.sql

### Create database

We want a representation for querying that is less elaborate than all the string values.

    duckdb md.init < prepare-md.sql


