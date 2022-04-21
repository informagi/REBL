from os import path

from ...utils import ErrorFileToDocuments, input_stream_gen_lines


def test_create_error_file(tmp_path):
    in_file = path.dirname(path.dirname(__file__)) + f'/resources/fake_error_ids.txt'
    out_file = str(tmp_path) + '/outfile.txt'
    source_folder = path.dirname(path.dirname(__file__)) + f'/resources/sample_docs_first_ten'
    expected_out_file = path.dirname(path.dirname(__file__)) + f'/resources/fake_error_ids_as_source_file.txt'

    ef_t_d = ErrorFileToDocuments(
        in_file=in_file,
        out_file=out_file,
        source_files_folder=source_folder
    )
    ef_t_d.process()

    out_gen = input_stream_gen_lines(out_file)
    expected_out_gen = input_stream_gen_lines(expected_out_file)
    for out_line, expected_out_line in zip(out_gen, expected_out_gen):
        assert out_line == expected_out_line
