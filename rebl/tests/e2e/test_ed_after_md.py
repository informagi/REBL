from os import path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from ...ed import EntityDisambiguation
from ...md import MentionDetection

_fields = ['title', 'headings', 'body']
_base_url = path.dirname(path.dirname(__file__)) + f'/resources/ed/'
_wiki_version = 'wiki_test'


def test_md_and_ed(tmp_path):
    in_file_md = path.dirname(path.dirname(__file__)) + f'/resources/e2e/minimal_example.txt'
    out_file_md = str(tmp_path) + '/md_output.parquet'
    md = MentionDetection(
        in_file=in_file_md,
        out_file=out_file_md
    )
    # This actually creates a file without entries as flair does not find fox
    md.write_batches_to_parquet()
    df = pd.DataFrame([["example_doc", i, "fox", 10, 13, .1, "PER"] for i in range(3)],
                      columns=['identifier', 'field', 'text', 'start_pos', 'end_pos', 'score', 'tag'])
    pq.write_table(pa.Table.from_pandas(df=df, preserve_index=False), out_file_md)
    out_file_ed = str(tmp_path) + '/ed_output.parquet'
    in_file_ed = out_file_md
    ed_f = EntityDisambiguation(md_file=in_file_ed, fields=_fields, source_file=in_file_md, out_file=out_file_ed,
                                base_url=_base_url, wiki_version=_wiki_version)
    ed_f.process()
    assert list(pq.read_table(out_file_ed).to_pandas()['entity']) == ['Fox', 'Fox', 'Fox']
