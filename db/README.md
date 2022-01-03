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

First, take the MD output on a single MS Marco V2 document
(`msmarco_doc_00_21381293`, more or less randomly selected,
in my tests called `doc-ok`).

Read the field metadata:

    duckdb md.init < fields.sql

Read 10 rows from the document:

    duckdb md.init < t0.sql

Query `t1.sql` gives a few rows of entity frequency information.

### Another sanity check

Now copy the batch MD output from `tusi`:

    scp tusi:/scratch/ckamphuis/el-msmarcov2/msmarco_v2_md/msmarco_doc_00* /export/data2/tmp

Test query `t2.sql` should give the same output as above,
but now reads from the Parquet file generated for the full batch (`00`).

    duckdb md.init < t2.sql

### Create database

We want a representation for querying that is less elaborate than all those string values,
pretty much preferred by our JSON scripting friends, but not ideal for SQL processing.

    duckdb md.init < prepare-md.sql

The transformation took ~3½ seconds on my home machine for part `00`.

### Test queries

Query `md-t2.sql` should give the same output as `t2.sql` and `t1.sql`.

TBC


