import argparse
import gzip

from ..utils import input_stream_gen_lines


class ErrorFileToDocuments:

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.out_file = self.arguments['out_file']
        self.source_folder = self.arguments['source_files_folder']
        if self.source_folder[-1] != '/':
            self.source_folder += '/'
        self.in_file_gen = input_stream_gen_lines(self.arguments['in_file'])

    def process(self):
        current_file_id = '-1'
        current_source_file = None
        output = ''
        for filename in self.in_file_gen:
            _, __, file_id, offset = filename.strip().split('_')  # Strip the newline
            offset = int(offset)
            if file_id != current_file_id:
                current_file_id = file_id
                try:
                    current_source_file = gzip.open(self.source_folder + f'msmarco_doc_{current_file_id}.gz',
                                                    'rt',
                                                    encoding='utf-8')
                    current_source_file.seek(offset)
                except (gzip.BadGzipFile, FileNotFoundError):
                    current_source_file = open(self.source_folder + f'msmarco_doc_{current_file_id}.txt',
                                               'rt',
                                               encoding='utf-8')
                    current_source_file.seek(offset)
            else:
                current_source_file.seek(offset)
            output += current_source_file.readline()
        with open(self.out_file, 'wt') as out:
            out.write(output)

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'in_file': None,
            'out_file': None,
            'source_files_folder': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['in_file'] is None:
            raise IOError('in_file containing errors needs to be provided')
        if arguments['source_files_folder'] is None:
            raise IOError('folder containing the original documents needs to be provided')
        if arguments['out_file'] is None:
            raise IOError('out_file path needs to be provided')
        return arguments


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-i',
        '--in_file',
        help='Name of the error file to process to a new document file'
    )
    parser.add_argument(
        '-s',
        '--source_files_folder',
        help='Name of the folder that contains the original source files'
    )
    parser.add_argument(
        '-o',
        '--out_file',
        help='Output file name'
    )
    ef_t_d = ErrorFileToDocuments(**vars(parser.parse_args()))
    ef_t_d.process()
