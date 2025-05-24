#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=1
#PJM -L elapse=99:00:00
#PJM -j

source $HOME/venv/c4/bin/activate

cd $SSD
cd c4-dataset-script/c4_dataset_script


cat 7_filter_cantonese/output/* > 7_filter_cantonese/cantonese.jsonl


python3.11 Chinese/clean_cantonese.py \
    --input_file 7_filter_cantonese/cantonese.jsonl \
    --output_file 8_clean_cantonese/cantonese_cleaned.jsonl \