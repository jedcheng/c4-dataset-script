#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=4
#PJM -L elapse=99:00:00
#PJM -j
##PJM -S


source $HOME/venv/c4/bin/activate


cd $SSD
cd c4-dataset-script/c4_dataset_script

export CC_ID="2025_13"

spark-submit --master local[32]  \
    Chinese/download_web_docs.py \
        --wet-paths wet.paths.gz \
        --output 1_download_${CC_ID}/download-docs \
        --array_index $PJM_BULKNUM \
        --badwords_filepath ./badwords/zh \
        --simplified_chinese_filtering \
        --SC_words_filepath ./badwords/SC_list.txt \


cat 1_download_${CC_ID}/download-docs_${PJM_BULKNUM}/* > 1_download_${CC_ID}/clean_docs_${PJM_BULKNUM}.jsonl
