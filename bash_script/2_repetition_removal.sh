#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=120
#PJM -L elapse=99:00:00
#PJM -j
##PJM -S


source $HOME/venv/c4/bin/activate


cd $SSD
cd c4-dataset-script/c4_dataset_script

export CC_ID="2024_33"

spark-submit --master local[120] --driver-memory 12g \
    --conf spark.local.dir=$SSD/spark_tmp \
    Chinese/repetition_removal.py \
    --input 1_download_${CC_ID}/clean_docs_${PJM_BULKNUM}.jsonl \
    --output 2_repetition_removal/repetition_removed_${PJM_BULKNUM}  \

