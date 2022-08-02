import argparse
import json
import gzip
import tqdm
import requests
import xml.etree.ElementTree as ET
import pyarrow.parquet as pq
import html


class EntityParquetToJSON:

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.ids = self.load_ids()
        self.entity_id_map = self.load_entity_id_map()
        self.data = self.load_data()
        self.field_mapping = {0: 'title', 1: 'headings', 2: 'body'}
        self.out = dict()

    def load_ids(self):
        print("Start Loading ids", flush=True)
        ids = []
        with gzip.open(self.arguments['source_file'], 'r') as source:
            for i, line in tqdm.tqdm(enumerate(source)):
                docid = json.loads(line)['docid']
                ids.append(docid)
                self.out[docid] = {'title': [], 'headings': [], 'body': [], 'docid': docid}
        return ids

    def load_entity_id_map(self):
        print("Start Loading entity_id_map", flush=True)
        entity_id_map = dict()
        for i, batch in tqdm.tqdm(enumerate(pq.ParquetFile(self.arguments['entity_maps']).iter_batches())):
            print(f"Finished batch {i}")
            df = batch.to_pandas()
            for entry in df.iterrows():
                e, identifier = entry[1]['entity'], entry[1]['id']
                if e in entity_id_map.keys():
                    response = requests.get(
                        f"https://en.wikipedia.org/w/api.php?format=xml&action=parse&prop=sections&page={e}")
                    page_id = int(ET.fromstring(response.text)[0].attrib['pageid'])
                    entity_id_map[e] = page_id
                else:
                    entity_id_map[e] = identifier
        return entity_id_map

    def load_data(self):
        print("Start Loading link_data", flush=True)
        data = []
        for i, batch in tqdm.tqdm(enumerate(pq.ParquetFile(self.arguments['in_file']).iter_batches())):
            print(f"Finished batch {i}")
            df = batch.to_pandas()
            data = data + [d for d in df.iterrows()]
        data = [d[1] for d in data]
        data = [[e['doc_id'], e['field'], e['start_pos'], e['end_pos'], e['entity'], e['tag'], e['md_score']] for e in
                data]
        data = sorted(data, key=lambda a: (int(a[0].split('_')[-1]), a[1], a[2]))
        return data

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'in_file': None,
            'out_file': None,
            'source_file': None,
            'entity_maps': None,
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        for key in arguments.keys():
            if arguments[key] is None:
                raise IOError(f'Argument {key} needs to be provided')
        return arguments

    def run(self):
        print("Start creating JSON file", flush=True)
        current_doc = None
        for i, (docid, field, start_pos, end_pos, entity, tag, md_score) in enumerate(self.data):
            if i % 100000 == 0:
                print(f"Finished {i} documents")
            e_text = entity.replace('_', ' ')
            e_text = html.unescape(e_text)
            self.out[docid][self.field_mapping[field]].append({
                "entity_id": self.entity_id_map[e_text],
                "start_pos": start_pos,
                "end_pos": end_pos,
                "entity": e_text,
                "details": {
                    "tag": tag,
                    "md_score": md_score
                }
            })
        with gzip.open(self.arguments['out_file'], 'wt') as f:
            for docid in self.ids:
                f.write(self.out[docid])
                f.write('\n')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i',
        '--in_file',
        help='Name of file to change to JSON (gzipped)'
    )
    parser.add_argument(
        '-o',
        '--out_file',
        help='Name of output file'
    )
    parser.add_argument(
        '-s',
        '--source_file',
        help='Name of the original data file'
    )
    parser.add_argument(
        '-em',
        '--entity_maps',
        help='Name of the file containing the entities, and identifiers'
    )
    ef_t_d = EntityParquetToJSON(**vars(parser.parse_args()))
    ef_t_d.run()
