# REBL

REBL is an extension of the [Radboud Entity Linker](https://github.com/informagi/REL) (REL) for Batch Entity Linking.
REBL makes it easier to isolate the GPU heavy operations from the CPU heavy operations, by separating the mention detection stage from the candidate selection and entity disambiguation stages.
In order to use REBL, REL needs to be installed as well. 

## Install
    git clone git@github.com:informagi/REBL.git
    cd REBL
    pip install .
If you have to run on CPU, then you can issue:

    pip install --user -f https://download.pytorch.org/whl/torch_stable.html torch==1.10.1+cpu

Note that `torch` requires a pretty old `python` (3.8), or this command will fail.

## Mention Detection

After installing REBL, it is very easy to run Mention Detection:

    python -m rebl.md.mention_detection.py \
        --in_file /path/to/input.gz \
        --out_file /path/to/md.parquet

A file containing the fields tagged will also be created, its name will be created following the `--out_file` name.

The following command line options are available:

* `-i IN_FILE, --in_file IN_FILE` Name of the file to tag
* `-o OUT_FILE, --out_file OUT_FILE` Output file name
* `-t TAGGER, --tagger TAGGER` Tagger that is used, `default: ner-fast`
*  `-f [FIELDS [FIELDS ...]], --fields [FIELDS [FIELDS ...]]` Fields to tag `default: title headings body`
*  `-id IDENTIFIER, --identifier IDENTIFIER` field key to identify document `default: docid`
*  `-pb PREDICT_BATCH_SIZE, --predict_batch_size PREDICT_BATCH_SIZE` How many Sentences to tag at once `default: 100`
*  `-wb WRITE_BATCH_SIZE, --write_batch_size WRITE_BATCH_SIZE` Number of lines written to outfile per batch `default: 10000`
*  `-ft {jsonl,tsv}, --file_type {jsonl,tsv}` Define the input file format `default: jsonl`
*  `-c CONT, --cont CONT`  Append output file if it already exists `default: True`

## Candidate Selection + Entity Disambiguation

Running these steps is also easy: (Note that `/path/to/md.parquet` is the output file from the previous step, and `/path/to/input.gz` is the same file as the input for Mention Detection)

    python3 -m rebl.ed.entity_disambiguation \
        --md_file /path/to/md.parquet \
        --fields title headings body \
        --source_file /path/to/input.gz \
        --out_file ed.parquet \
        --base_url /path/to/REL-data \
        --wiki_version wiki_2019 \
        --identifier docid

The following command line options are available (`-f` and `-ff` are mutually exclusive):

* `-md MD_FILE, --md_file MD_FILE` Name of the md parquet file to tag
* `-ff FIELDS_FILE, --fields_file FIELDS_FILE` Name of the fields file to tag
* `-f [FIELDS [FIELDS ...]], --fields [FIELDS [FIELDS ...]]` Give the fields directly
* `--s SOURCE_FILE, --source_file SOURCE_FILE` Name of the source file that contains raw text
* `-o OUT_FILE, --out_file OUT_FILE`  Name of the file to write to
* `-b BASE_URL, --base_url BASE_URL` Location of base_url for REL ED model
* `-w WIKI_VERSION, --wiki_version WIKI_VERSION` Wikipedia version to use
* `-id IDENTIFIER, --identifier IDENTIFIER` field key to identify document
* `wb WRITE_BATCH_SIZE, --write_batch_size WRITE_BATCH_SIZE` NWrite batch size `default: 10000`

