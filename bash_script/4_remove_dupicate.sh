#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=120
#PJM -L elapse=99:00:00
#PJM -j
#PJM -S

cd $SSD
cd c4-dataset-script/c4_dataset_script


spark-submit --master local[20]  \
    Chinese/remove_duplicate_text.py \
        --input 3_filter_output/clean_docs_${PJM_BULKNUM}.jsonl \
        --output 4_remove_duplicate/deduplicated_text_${PJM_BULKNUM}

cat 4_remove_duplicate/deduplicated_text_${PJM_BULKNUM}/clean_docs/* > 4_remove_duplicate/deduplicated_text_${PJM_BULKNUM}/clean_docs.jsonl