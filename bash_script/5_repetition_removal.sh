#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=32
#PJM -L elapse=99:00:00
#PJM -j
#PJM -S


source $HOME/venv/c4/bin/activate


cd $SSD
cd c4-dataset-script/c4_dataset_script


spark-submit --master local[64]  \
    Chinese/repetition_removal.py \
        --input 4_remove_duplicate/deduplicated_text_${PJM_BULKNUM}/clean_docs.jsonl \
        --output 5_repetition_removal/removed_repetition_${PJM_BULKNUM}/ \


cat 5_repetition_removal/removed_repetition_${PJM_BULKNUM}/clean_docs/* > 5_repetition_removal/removed_repetition_${PJM_BULKNUM}/clean_docs.jsonl