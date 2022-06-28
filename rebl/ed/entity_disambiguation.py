import argparse
import json
import time
import gc
from itertools import chain

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from REL.entity_disambiguation import EntityDisambiguation as RelED
from REL.mention_detection import MentionDetection
from REL.utils import process_results

from ..utils import input_stream_gen_lines, stream_parquet_file_per_entry


class EntityDisambiguation:

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.config = {
            "mode": "eval",
            "model_path": "{}/{}/generated/model".format(self.arguments['base_url'], self.arguments['wiki_version'])
        }
        self.out_file = self.arguments['out_file']
        self.fields = self.create_fields()
        self.fields_inverted = {value: key for key, value in self.fields.items()}
        self.stream_parquet_md_file = stream_parquet_file_per_entry(self.arguments['md_file'])
        self.stream_raw_source_file = input_stream_gen_lines(self.arguments['source_file'])
        self.mention_detection = MentionDetection(self.arguments['base_url'], self.arguments['wiki_version'])
        self.model = RelED(self.arguments['base_url'], self.arguments['wiki_version'], self.config,
                           reset_embeddings=True)
        self.docs_done = 0

    def create_fields(self):
        if self.arguments['fields']:
            return {i: f for i, f in enumerate(self.arguments['fields'])}
        return {v[0]: k for k, v in pd.read_parquet(self.arguments['fields_file']).to_dict().items()}

    def stream_doc_with_spans(self):
        for i, raw_data in enumerate(self.stream_raw_source_file):
            json_content = json.loads(raw_data)
            for field_key in range(len(self.fields)):
                field = self.fields[field_key]
                current_text = json_content[field]
                spans, tags, scores = [], [], []
                data = next(self.stream_parquet_md_file)
                while data[1]['field'] == field_key and \
                        data[1]['identifier'] == json_content[self.arguments['identifier']]:
                    spans.append((data[1]['start_pos'], data[1]['end_pos'] - data[1]['start_pos']))
                    tags.append(data[1]['tag'])
                    scores.append(data[1]['score'])
                    try:
                        data = next(self.stream_parquet_md_file)
                    except StopIteration:
                        yield json_content[self.arguments['identifier']], field, spans, current_text, tags, scores
                        return
                yield json_content[self.arguments['identifier']], field, spans, current_text, tags, scores
                self.stream_parquet_md_file = chain(iter([data]), self.stream_parquet_md_file)
            self.docs_done = i + 1
            if self.docs_done % 100 == 0:
                gc.collect()
            if self.docs_done == 20000:
                import sys
                sys.exit(0)

    def disambiguate(self, identifier, field, spans, text, tags, scores):
        unique_id = f'{identifier}+{field}'
        processed = {unique_id: [text, spans]}
        mentions_dataset, total_ment = self.mention_detection.format_spans(
            processed
        )
        predictions, timing = self.model.predict(mentions_dataset)
        ts = [(tag, score) for tag, score, pred in
              zip(tags, scores, predictions[unique_id]) if pred['prediction'] != 'NIL']
        results = process_results(
            mentions_dataset,
            predictions,
            processed,
            True
        )
        for i, (tag, score) in enumerate(ts):
            results[unique_id][i] = list(results[unique_id][i])
            results[unique_id][i][5] = tag
            results[unique_id][i][6] = score
            results[unique_id][i] = tuple(results[unique_id][i])
        return results

    def stream_disambiguate_file(self):
        for identifier, field, spans, text, tags, scores in self.stream_doc_with_spans():
            if len(spans) == 0:
                continue
            yield f'{identifier}+{field}', self.disambiguate(identifier, field, spans, text, tags, scores)

    def create_disambiguate_batches(self):
        batch = []
        for identifier, result in self.stream_disambiguate_file():
            if len(batch) >= self.arguments['write_batch_size']:
                yield batch
                batch = []
            for start_pos, span, text, entity, ed_score, tag, md_score in result[identifier]:
                doc_id, field = identifier.split('+')
                field = self.fields_inverted[field]
                batch.append([doc_id, field, start_pos, start_pos + span, entity, ed_score, tag, md_score])
        yield batch

    def process(self):
        gen = self.create_disambiguate_batches()
        df = pd.DataFrame(next(gen),
                          columns=['doc_id', 'field', 'start_pos', 'end_pos', 'entity', 'ed_score', 'tag', 'md_score'])
        table = pa.Table.from_pandas(df=df, preserve_index=False)
        t = time.time()
        with pq.ParquetWriter(self.out_file, schema=table.schema) as writer:
            writer.write_table(table)
            while True:
                try:
                    batch = next(gen)
                except StopIteration:
                    break
                df = pd.DataFrame(batch,
                                  columns=['doc_id', 'field', 'start_pos', 'end_pos', 'entity', 'ed_score', 'tag',
                                           'md_score'])
                table = pa.Table.from_pandas(df=df, preserve_index=False)
                writer.write_table(table)
                batch_time = time.time() - t
                print(f'Documents finished: {self.docs_done}; Batch time: {batch_time:.2f} seconds', flush=True)
                t = time.time()
        pq.write_table(pq.read_table(self.out_file).combine_chunks(), self.out_file)

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'md_file': None,
            'fields_file': None,
            'fields': None,
            'source_file': None,
            'out_file': None,
            'base_url': None,
            'wiki_version': None,
            'identifier': 'docid',
            'write_batch_size': 10000
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['fields'] and arguments['fields_file'] or \
                arguments['fields'] is None and arguments['fields_file'] is None:
            raise IOError('Either fields or fields_file needs to be provided')
        for key in ['md_file', 'source_file', 'out_file', 'base_url', 'wiki_version']:
            if arguments[key] is None:
                raise IOError(f'Argument {key} needs to be provided')
        return arguments


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-md',
        '--md_file',
        required=True,
        help='Name of the md parquet file to tag'
    )
    fields_me = parser.add_mutually_exclusive_group()
    fields_me.add_argument(
        '-ff',
        '--fields_file',
        help='Name of the fields file to tag'
    )
    fields_me.add_argument(
        '-f',
        '--fields',
        nargs='*',
        help='Give the fields directly',
    )
    parser.add_argument(
        '-s',
        '--source_file',
        required=True,
        help='Name of the source file that contains raw text'
    )
    parser.add_argument(
        '-o',
        '--out_file',
        required=True,
        help='Name of the file to write to'
    )
    parser.add_argument(
        '-b',
        '--base_url',
        required=True,
        help='Location of base_url for REL ED model'
    )
    parser.add_argument(
        '-w',
        '--wiki_version',
        required=True,
        help='Wikipedia version to use'
    )
    parser.add_argument(
        '-id',
        '--identifier',
        help='field key to identify document',
        default='docid'
    )
    parser.add_argument(
        '-wb',
        '--write_batch_size',
        help='Write batch size',
        default=10000
    )
    ed = EntityDisambiguation(**vars(parser.parse_args()))
    ed.process()
