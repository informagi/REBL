import gzip
import pyarrow.parquet as pq


def input_stream_gen_lines(filename, skip_to=0):
    try:
        # Try read first line as gzipped file
        f = gzip.open(filename, 'rt', encoding='utf-8')
        f.seek(1)  # To confirm the data is gzipped
        f.seek(0)  # Resets to zero, because the default seek behaviour is to start from start of file
    except Exception:
        # If input is not gzipped, fallback to normal file I/O
        f = open(filename, 'rt', encoding='utf-8')
    # Generate rest of the input
    f.seek(skip_to)
    return (line for line in f.readlines())
    # for line in f:
    #     yield line


def stream_parquet_file_per_entry(filename):
    for batch in pq.ParquetFile(filename).iter_batches():
        df = batch.to_pandas()
        lines = [line for line in df.iterrows()]

        for line in lines:
            yield line
        # for line in df.iterrows():
        #     yield line
