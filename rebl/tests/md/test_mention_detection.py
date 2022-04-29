from shutil import copy
from pathlib import Path
from os import path
import pyarrow.parquet as pq

from ...md import MentionDetection


def mention_detect_jsonl(file_path, file_id):
    # 2 includes one of the chars flair removes
    # 20 includes more edge cases (but takes long time)
    in_file = path.dirname(path.dirname(__file__)) + \
              f'/resources/hard_examples_with_expected_output/example_docs_{file_id}.txt.gz'
    out_file = str(file_path) + 'outfile.parquet'
    md = MentionDetection(
        in_file=in_file,
        out_file=out_file
    )
    md.write_batches_to_parquet()
    with open(out_file[:-8] + '_errors.txt', 'a') as f:
        f.write('abc\n')
    with open(out_file[:-8] + '_errors.txt', 'r') as f:
        # raises an exception if errors were found during write_batches_to_parquet
        assert f.readline().strip() == 'abc'


def mention_detect_jsonl_continue(file_path):
    # still need to add a case that generates both files on the fly
    in_file = path.dirname(path.dirname(__file__)) + f'/resources/sample_docs_first_three/msmarco_doc_00.txt'
    copy_to_out = path.dirname(path.dirname(__file__)) + \
                  f'/resources/sample_docs_first_three/msmarco_doc_00_md_unfinished.parquet'
    out_file = str(file_path) + 'outfile.parquet'
    copy(copy_to_out, out_file)
    md = MentionDetection(
        in_file=in_file,
        out_file=out_file,
        cont=True
    )
    md.write_batches_to_parquet()
    # Confirm that when using continuation works
    out_table = pq.read_table(out_file).to_pandas()
    expected_out_table = pq.read_table(Path(path.dirname(
        path.dirname(__file__)) + f'/resources/sample_docs_first_three/msmarco_doc_00_md.parquet')).to_pandas()
    for c in ['identifier', 'field', 'text', 'start_pos', 'end_pos', 'score', 'tag']:
        for a, b in zip(out_table[c], expected_out_table[c]):
            if type(a) == float:
                assert -.001 < a-b < .001  # difference should be small
            else:
                assert a == b

def test_mention_detect_2(tmp_path):
    mention_detect_jsonl(file_path=tmp_path, file_id='2')


# This function takes a long time, but has some additional edge cases
# def test_mention_detect_20(tmp_path):
#     mention_detect_jsonl(file_path=tmp_path, file_id='20')


def mention_detect_tab(file_path):
    in_file = path.dirname(path.dirname(__file__)) + '/resources/example_tab_sep.tsv'
    out_file = str(file_path) + 'outfile.parquet'
    md = MentionDetection(
        in_file=in_file,
        out_file=out_file,
        file_type='tsv'
    )
    md.write_batches_to_parquet()
    with open(out_file[:-8] + '_errors.txt', 'a') as f:
        f.write('def\n')
    with open(out_file[:-8] + '_errors.txt', 'r') as f:
        # raises an exception if errors were found during write_batches_to_parquet
        assert f.readline().strip() == 'def'


def test_mention_detect_tab_sep(tmp_path):
    mention_detect_tab(file_path=tmp_path)


def test_mention_detect_continue(tmp_path):
    mention_detect_jsonl_continue(file_path=tmp_path)
