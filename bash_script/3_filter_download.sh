#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=2
#PJM -L elapse=99:00:00
#PJM -j


source $HOME/venv/c4/bin/activate

cd $SSD
cd c4-dataset-script/c4_dataset_script

cat 2_download/download-docs_${PJM_BULKNUM}/*/part-* | \
    python3.11 Chinese/filter_out_bad_lines.py \
        --badwords_filepath ./badwords/zh \
         > 3_filter_output/clean_docs_${PJM_BULKNUM}.jsonl




