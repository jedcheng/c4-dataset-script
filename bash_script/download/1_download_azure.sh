#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=4
#PJM -L elapse=99:00:00
#PJM -j
##PJM -S



cd $HOME/c4-dataset-script

cd ${WKDIR}/c4-dataset-script

source venv/bin/activate

cd c4_dataset_script


export CC_ID="2023_23"

rm -rf 1_download_${CC_ID}/
mkdir -p 1_download_${CC_ID}



for PJM_BULKNUM in $(seq 0 17); do
    spark-submit --master local[16]  \
        Chinese/download_web_docs.py \
            --wet-paths wet.paths.gz \
            --output 1_download_${CC_ID} \
            --array_index $PJM_BULKNUM \
            --badwords_filepath ./badwords/zh \
            --simplified_chinese_filtering \
            --SC_words_filepath ./badwords/SC_list.txt \

done