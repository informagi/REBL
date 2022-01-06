# REBL

Batch entity linking of a JSON lines collection

## Install

    pip install --user -r requirements.txt

If you have to run on CPU, then you can issue:

    pip install --user -f https://download.pytorch.org/whl/torch_stable.html torch==1.10.1+cpu

Note that `torch` requires a pretty old `python` (3.8), or this command will fail.

## Run

    python rebl/md/mention_detection.py -i /data/doc.txt -o /data/doc.parquet -pb 10

