import gzip


def input_stream_gen_lines(filename):
    try:
        # try read first line as gzipped file
        f = gzip.open(filename, 'rt', encoding='utf-8')
        yield f.readline()
    except gzip.BadGzipFile:
        # if input is not gzipped, fallback to normal file I/O
        f = open(filename, 'rt', encoding='utf-8')
        yield f.readline()
    # Generate rest of the input
    for line in f:
        yield line
