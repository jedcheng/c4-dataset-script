#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=32
#PJM -L elapse=99:00:00
#PJM -j


source $HOME/venv/c4/bin/activate

cd $SSD
cd c4-dataset-script/c4_dataset_script



python3.11 Chinese/filter_out_SC_spark.py \
    --input 5_repetition_removal/removed_repetition_${PJM_BULKNUM}/clean_docs.jsonl \
    --output 6_remove_sc/removed_sc_${PJM_BULKNUM}/ \
        --SC_words_filepath ./badwords/SC_list.txt \

cat 6_remove_sc/removed_sc_${PJM_BULKNUM}/clean_docs/* > 6_remove_sc/removed_sc_${PJM_BULKNUM}.jsonl

rm -rf 6_remove_sc/removed_sc_${PJM_BULKNUM}