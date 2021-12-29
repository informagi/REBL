import argparse
import gzip
import json
from itertools import chain

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from flair.data import Sentence, Token
from flair.models import SequenceTagger
from syntok.segmenter import analyze


class MentionDetection:

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.out_file = self.arguments['out_file']
        self.tagger = SequenceTagger.load(self.arguments['tagger'])

    def input_stream_gen(self):
        with gzip.open(self.arguments['in_file'], 'rb') as f:
            for line in f:
                yield line

    @staticmethod
    def create_sentences(text):
        sentence_list = []
        for syntok_sentence in chain.from_iterable(analyze(text)):
            manual_sent = Sentence()
            for token in syntok_sentence:
                manual_sent.add_token(Token(token._value, start_position=token._offset))
            for token in manual_sent:
                start = token.start_pos
                end = start + len(token.text)
                try:
                    assert token.text == text[start:end]
                except AssertionError as e:
                    print(e)
                    raise AssertionError
            sentence_list.append(manual_sent)
        return sentence_list

    def jsonl_to_sentences_with_id_gen(self):
        for line in self.input_stream_gen():
            json_line = json.loads(line)
            base_id = json_line[self.arguments['identifier']]
            for field in self.arguments['fields']:
                raw_text = json_line[field]
                for sentence_object in self.create_sentences(raw_text):
                    yield sentence_object, base_id + '_' + field

    def batch_sentence_gen(self):
        batch, ids = list(), list()
        for sentence, identifier in self.jsonl_to_sentences_with_id_gen():
            batch.append(sentence)
            ids.append(identifier)
            if len(batch) == int(self.arguments['predict_batch_size']):
                yield batch, ids
                batch, ids = list(), list()
        yield batch, ids

    def mention_detect_sentence_batch_gen(self):
        for batch, ids in self.batch_sentence_gen():
            self.tagger.predict(batch)
            yield batch, ids

    def sentence_md_batches_to_sentences_gen(self):
        for sentence_batch, id_batch in self.mention_detect_sentence_batch_gen():
            for sentence, identifier in zip(sentence_batch, id_batch):
                yield sentence, identifier

    def sentence_to_mentions_gen(self):
        for sentence, identifier in self.sentence_md_batches_to_sentences_gen():
            for entity in sentence.get_spans('ner'):
                yield entity, identifier

    def batch_mentions_gen(self):
        batch = []
        for entity, identifier in self.sentence_to_mentions_gen():
            batch.append(
                [
                    identifier,
                    entity.text,
                    entity.start_pos,
                    entity.end_pos,
                    entity.score,
                    entity.tag
                ]
            )
            if len(batch) == int(self.arguments['write_batch_size']):
                yield batch
                batch = []
        yield batch

    def write_batches_to_parquet(self):
        for batch in self.batch_mentions_gen():
            df = pd.DataFrame(batch, columns=['identifier', 'text', 'start_pos', 'end_pos', 'score', 'tag'])
            table = pa.Table.from_pandas(df=df, preserve_index=False)
            with pq.ParquetWriter(self.out_file, schema=table.schema) as writer:
                writer.write_table(table)

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'in_file': None,
            'out_file': None,
            'tagger': 'ner-fast',
            'fields': ['title', 'headings', 'body'],
            'identifier': 'docid',
            'predict_batch_size': '100',
            'write_batch_size': '10000'
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['in_file'] is None:
            raise IOError('in_file needs to be provided')
        if arguments['out_file'] is None:
            raise IOError('out_file path needs to be provided')
        return arguments


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i',
        '--in_file',
        help='Name of the JSONL file to tag'
    )
    parser.add_argument(
        '-o',
        '--out_file',
        help='Output file name'
    )
    parser.add_argument(
        '-t',
        '--tagger',
        help='Tagger that is used, default is ner-fast',
        default='ner-fast'
    )
    parser.add_argument(
        '-f',
        '--fields',
        nargs='*',
        help='Fields to tag',
        default=['title', 'headings', 'body']
    )
    parser.add_argument(
        '-id',
        '--identifier',
        help='field key to identify document',
        default='docid'
    )
    parser.add_argument(
        '-pb',
        '--predict_batch_size',
        help='How many Sentences to tag at once',
        default='100'
    )
    parser.add_argument(
        '-wb',
        '--write_batch_size',
        help='How many Sentences to tag at once',
        default='10000'
    )
    md = MentionDetection(**vars(parser.parse_args()))
    md.write_batches_to_parquet()

