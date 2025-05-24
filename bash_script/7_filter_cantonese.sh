#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j


source $HOME/venv/c4/bin/activate

cd $SSD
cd c4-dataset-script/c4_dataset_script


python3.11 Chinese/filter_cantonese.py \
    --input 6_remove_sc/removed_sc_${PJM_BULKNUM}.jsonl \
    --output 7_filter_cantonese/cantonese_${PJM_BULKNUM}/ \
    --include_quotes --split \

cat 7_filter_cantonese/cantonese_${PJM_BULKNUM}/clean_docs/* > 7_filter_cantonese/output/cantonese_${PJM_BULKNUM}.jsonl
rm -rf 7_filter_cantonese/cantonese_${PJM_BULKNUM}