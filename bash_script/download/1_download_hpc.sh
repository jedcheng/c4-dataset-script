#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j
##PJM -S


source $HOME/venv/c4/bin/activate


cd $SSD/c4-dataset-script/c4_dataset_script

export CC_ID="2023_40"

mkdir -p 1_download_${CC_ID}



spark-submit --master local[32]  \
    Chinese/download_web_docs.py \
        --wet-paths wet.paths.gz \
        --output 1_download_${CC_ID} \
        --array_index $PJM_BULKNUM \
        --badwords_filepath ./badwords/zh \
        --simplified_chinese_filtering \
        --SC_words_filepath ./badwords/SC_list.txt \
