from os import path

from ...md import MentionDetection


def mention_detect_jsonl(file_path, file_id):
    # 2 includes one of the chars flair removes
    # 20 includes more edge cases TODO
    in_file = path.dirname(path.dirname(__file__)) + f'/resources/example_docs_{file_id}.txt.gz'
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


def test_mention_detect_2(tmp_path):
    mention_detect_jsonl(file_path=tmp_path, file_id='2')


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

# def test_mention_detect_20(tmp_path):
#     mention_detect_jsonl(file_path=tmp_path, file_id='20')
