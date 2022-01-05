from os import path

from ...md import MentionDetection


def mention_detect(file_path, file_id):
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
    mention_detect(file_path=tmp_path, file_id='2')


def test_mention_detect_20(tmp_path):
    mention_detect(file_path=tmp_path, file_id='20')
