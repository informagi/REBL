import argparse
import json
import re
import os
from itertools import chain
from ..utils import input_stream_gen_lines

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import torch
from flair.data import Sentence, Token
from flair.models import SequenceTagger
from syntok.segmenter import analyze


class MentionDetection:

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.out_file = self.arguments['out_file']
        self.tagger = SequenceTagger.load(self.arguments['tagger'])
        self.field_mapping = {f: i for i, f in enumerate(self.arguments['fields'])}
        self.chars_removed_by_flair = re.compile("([\u200c\ufe0f\ufeff])")
        self.skip_to = 0

    def create_sentences(self, text, identifier):
        sentence_list = []
        for syntok_sentence in chain.from_iterable(analyze(text)):
            remove_char_counts = []
            manual_sent = Sentence()
            for token in syntok_sentence:
                # Find how much we need to increase the offset by the characters that are removed by flair
                if len(self.chars_removed_by_flair.sub('', token.value)) > 0:  # Zero length tokens are not added
                    remove_char_counts.append(len(self.chars_removed_by_flair.findall(token.value)))
                manual_sent.add_token(Token(token.value, start_position=token.offset))
            try:
                assert len(remove_char_counts) == len(manual_sent)  # Ensure offset list is of equal length
            except AssertionError as e:
                print("Length offset list: " + str(len(remove_char_counts)))
                print("Length token list: " + str(len(manual_sent)))
                print("AssertionError: " + str(e))
                with open(self.arguments['out_file'][:-8] + '_errors.txt', 'a') as f:
                    f.write(identifier)
                    f.write('\n')

            for i, token in enumerate(manual_sent):
                start = token.start_pos
                end = start + len(token.text) + remove_char_counts[i]
                raw_token_cleaned = self.chars_removed_by_flair.sub('', text[start:end])
                token.end_pos = end
                try:  # Especially important that the offset is still correct, the rest can be reconstructed
                    assert raw_token_cleaned == token.text
                except AssertionError as e:
                    print("Token text" + token.text)
                    print("Raw text" + raw_token_cleaned)
                    print("Context: " + text[start - 10:end + 10])
                    print("Remove char count: " + str(remove_char_counts[i]))
                    print("AssertionError: " + str(e))
                    # For now ignore this document and store identifier in error file
                    with open(self.arguments['out_file'][:-8] + '_errors.txt', 'a') as f:
                        f.write(identifier)
                        f.write('\n')
                    return []
            sentence_list.append(manual_sent)
        return sentence_list

    def jsonl_to_sentences_with_id_gen(self):
        for line in input_stream_gen_lines(self.arguments['in_file'], skip_to=self.skip_to):
            json_line = json.loads(line)
            identifier = json_line[self.arguments['identifier']]
            for field in self.arguments['fields']:
                raw_text = json_line[field]
                for sentence_object in self.create_sentences(raw_text, identifier):
                    yield sentence_object, identifier, self.field_mapping[field]

    def tab_sep_to_sentences_with_id_gen(self):
        for line in input_stream_gen_lines(self.arguments['in_file']):
            identifier, text = line.strip().split('\t')
            for sentence_object in self.create_sentences(text, identifier):
                yield sentence_object, identifier, 'body'

    def batch_sentence_gen(self):
        batch, ids, fields = list(), list(), list()
        if self.arguments['file_type'] == 'jsonl':
            doc_gen = self.jsonl_to_sentences_with_id_gen()
        else:
            doc_gen = self.tab_sep_to_sentences_with_id_gen()
        for sentence, identifier, field in doc_gen:
            batch.append(sentence)
            ids.append(identifier)
            fields.append(field)
            if len(batch) == int(self.arguments['predict_batch_size']):
                yield batch, ids, fields
                batch, ids, fields = list(), list(), list()
        yield batch, ids, fields

    def mention_detect_sentence_batch_gen(self):
        for batch, ids, fields in self.batch_sentence_gen():
            try:
                self.tagger.predict(batch)
                yield batch, ids, fields
            except RuntimeError:
                try:
                    torch.cuda.empty_cache()
                    self.tagger.predict(batch)
                    yield batch, ids, fields
                except RuntimeError:
                    torch.cuda.empty_cache()
                    fine = []
                    for i, b in enumerate(batch):
                        try:
                            torch.cuda.empty_cache()
                            self.tagger.predict(b)
                            fine.append(i)
                        except RuntimeError:  # Single sentence is too big for gpu...
                            with open(self.arguments['out_file'][:-8] + '_memory_problem.txt', 'a') as f:
                                json.dump({"id": ids[i],
                                           "start_pos": b[0].start_pos,
                                           "end_pos": b[-1].end_pos},
                                          f)
                                f.write('\n')
                        yield [batch[i] for i in fine], [ids[i] for i in fine], [fields[i] for i in fine]
                    torch.cuda.empty_cache()

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

        if not self.arguments['cont'] or not os.path.isfile(self.out_file[:-8] + '_field_mapping.parquet'):
            with pq.ParquetWriter(self.out_file[:-8] + '_field_mapping.parquet', schema=table.schema) as writer:
                writer.write_table(table)

        if self.arguments['cont'] and os.path.isfile(self.out_file):
            already_tagged = pq.read_table(self.out_file)
            last_identifier = max({int(str(e).split('_')[-1]) for e in already_tagged['identifier']})
            file_identifier = str(already_tagged['identifier'][0]).split('_')[-2]
            last_file_identifier = f'msmarco_doc_{file_identifier}_{last_identifier}'
            self.skip_to = last_identifier
            df = already_tagged.to_pandas()
            df = df[df['identifier'] != last_file_identifier]
            gen = self.batch_mentions_gen()
        else:
            gen = self.batch_mentions_gen()
            df = pd.DataFrame(next(gen),
                              columns=['identifier', 'field', 'text', 'start_pos', 'end_pos', 'score', 'tag'])
        table = pa.Table.from_pandas(df=df, preserve_index=False)
        with pq.ParquetWriter(self.out_file, schema=table.schema) as writer:
            writer.write_table(table)
            while True:
                try:
                    batch = next(gen)
                except StopIteration:
                    break
                df = pd.DataFrame(batch,
                                  columns=['identifier', 'field', 'text', 'start_pos', 'end_pos', 'score', 'tag'])
                table = pa.Table.from_pandas(df=df, preserve_index=False)
                writer.write_table(table)
        pq.write_table(pq.read_table(self.out_file).combine_chunks(), self.out_file)

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'in_file': None,
            'out_file': None,
            'tagger': 'ner-fast',
            'fields': ['title', 'headings', 'body'],
            'identifier': 'docid',
            'predict_batch_size': '100',
            'write_batch_size': '10000',
            'file_type': 'jsonl',
            'cont': True
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
    parser.add_argument(
        '-ft',
        '--file_type',
        choices=['jsonl', 'tsv'],
        help='What is the input file format',
        default='jsonl'
    )
    parser.add_argument(
        '-c',
        '--cont',
        help='Append output file if it already exists',
        default=True
    )
    md = MentionDetection(**vars(parser.parse_args()))
    md.write_batches_to_parquet()
