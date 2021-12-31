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
        self.field_mapping = {f: i for i, f in enumerate(self.arguments['fields'])}

    def input_stream_gen(self):
        try: 
            # try read first line as gzipped file
            f = gzip.open(self.arguments['in_file'], 'rt', encoding='utf-8')
            yield f.readline()
        except gzip.BadGzipFile:
            # if input is not gzipped, fallback to normal file I/O
            f = open(self.arguments['in_file'], 'rt', encoding='utf-8')
            yield f.readline()
        # Generate rest of the input
        for line in f:
            yield line

    def create_sentences(self, text, identifier):
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
                    print(token.text)
                    print(text[start:end])
                    print(e)
                    # For now ignore this document and store identifier in error file
                    with open(self.arguments['out_file'][:-8] + '_errors.txt', 'a') as f:
                        f.write(identifier)
                        f.write('\n')
                    return []
            sentence_list.append(manual_sent)
        return sentence_list

    def jsonl_to_sentences_with_id_gen(self):
        for line in self.input_stream_gen():
            json_line = json.loads(line)
            identifier = json_line[self.arguments['identifier']]
            for field in self.arguments['fields']:
                raw_text = json_line[field]
                for sentence_object in self.create_sentences(raw_text, identifier):
                    yield sentence_object, identifier, self.field_mapping[field]

    def batch_sentence_gen(self):
        batch, ids, fields = list(), list(), list()
        for sentence, identifier, field in self.jsonl_to_sentences_with_id_gen():
            batch.append(sentence)
            ids.append(identifier)
            fields.append(field)
            if len(batch) == int(self.arguments['predict_batch_size']):
                yield batch, ids, fields
                batch, ids, fields = list(), list(), list()
        yield batch, ids, fields

    def mention_detect_sentence_batch_gen(self):
        for batch, ids, fields in self.batch_sentence_gen():
            self.tagger.predict(batch)
            yield batch, ids, fields

    def sentence_md_batches_to_sentences_gen(self):
        for sentence_batch, id_batch, field_batch in self.mention_detect_sentence_batch_gen():
            for sentence, identifier, field in zip(sentence_batch, id_batch, field_batch):
                yield sentence, identifier, field

    def sentence_to_mentions_gen(self):
        for sentence, identifier, field in self.sentence_md_batches_to_sentences_gen():
            for entity in sentence.get_spans('ner'):
                yield entity, identifier, field

    def batch_mentions_gen(self):
        batch = []
        for entity, identifier, field in self.sentence_to_mentions_gen():
            batch.append(
                [
                    identifier,
                    field,
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
        fields = pd.DataFrame.from_dict({k: [v] for k, v in self.field_mapping.items()})
        table = pa.Table.from_pandas(df=fields, preserve_index=False)
        with pq.ParquetWriter(self.out_file[:-8] + '_field_mapping.parquet', schema=table.schema) as writer:
            writer.write_table(table)

        gen = self.batch_mentions_gen()
        df = pd.DataFrame(next(gen),
                          columns=['identifier', 'field', 'text', 'start_pos', 'end_pos', 'score', 'tag'])
        table = pa.Table.from_pandas(df=df, preserve_index=False)
        with pq.ParquetWriter(self.out_file, schema=table.schema) as writer:
            writer.write_table(table)
            while batch := next(gen):
                df = pd.DataFrame(batch,
                                  columns=['identifier', 'field', 'text', 'start_pos', 'end_pos', 'score', 'tag'])
                table = pa.Table.from_pandas(df=df, preserve_index=False)
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
