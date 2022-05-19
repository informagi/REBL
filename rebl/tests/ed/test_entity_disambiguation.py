from os import path

import pyarrow.parquet as pq

from ...ed import EntityDisambiguation

_md = path.dirname(path.dirname(__file__)) + \
      f'/resources/md/hard_examples_with_expected_output/example_docs_20_md.parquet'
_fields_file = path.dirname(path.dirname(__file__)) + f'/resources/ed/example_fields_file.parquet'
_fields = ['title', 'headings', 'body']
_source_file = path.dirname(path.dirname(__file__)) + \
               f'/resources/md/hard_examples_with_expected_output/example_docs_20.txt.gz'
_out_file = '/outfile.parquet'  # Use as extension on tmp path
_base_url = path.dirname(path.dirname(__file__)) + f'/resources/ed/'
_wiki_version = 'wiki_test'


def test_one_type_of_file_parameter():
    # Both fields_file and fields
    try:
        EntityDisambiguation(md_file=_md, fields_file=_fields_file, fields=_fields, source_file=_source_file,
                             out_file=_out_file, base_url=_base_url, wiki_version=_wiki_version)
        raise AssertionError("Should raise error as both fields_file and fields are provided")
    except IOError:
        pass
    # Only fields should not raise error
    EntityDisambiguation(md_file=_md, fields=_fields, source_file=_source_file, out_file=_out_file, base_url=_base_url,
                         wiki_version=_wiki_version)
    # Only fields_file should not raise error
    EntityDisambiguation(md_file=_md, fields_file=_fields_file, source_file=_source_file, out_file=_out_file,
                         base_url=_base_url, wiki_version=_wiki_version)
    # Neither
    try:
        EntityDisambiguation(md_file=_md, source_file=_source_file, out_file=_out_file, base_url=_base_url,
                             wiki_version=_wiki_version)
        raise AssertionError("Should raise error as neither fields_file or fields are provided")
    except IOError:
        pass


def test_create_fields():
    ed_ff = EntityDisambiguation(md_file=_md, fields_file=_fields_file, source_file=_source_file, out_file=_out_file,
                                 base_url=_base_url, wiki_version=_wiki_version)
    ed_f = EntityDisambiguation(md_file=_md, fields=_fields, source_file=_source_file, out_file=_out_file,
                                base_url=_base_url, wiki_version=_wiki_version)
    assert ed_ff.fields == ed_f.fields == {0: 'title', 1: 'headings', 2: 'body'}


def test_disambiguate(tmp_path):
    ed_f = EntityDisambiguation(md_file=_md, fields=_fields, source_file=_source_file, out_file=str(tmp_path)+_out_file,
                                base_url=_base_url, wiki_version=_wiki_version)
    link = ed_f.disambiguate("test", "body", [(10, 3), (17, 11)], "the brown fox jumped over the lazy dog",
                             ["PER", "PER"], [0.9, 0.8],)
    assert link == {"test+body": [(10, 3, "fox", "Fox", 0.0, "PER", 0.9)]}
