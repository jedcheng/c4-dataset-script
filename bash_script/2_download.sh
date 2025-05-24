#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=32
#PJM -L elapse=99:00:00
#PJM -j
##PJM -S


source $HOME/venv/c4/bin/activate


cd $SSD
cd c4-dataset-script/c4_dataset_script


spark-submit --master local[64]  \
    Chinese/download_web_docs.py \
        --wet-paths wet.paths.gz \
        --output 2_download/download-docs \
        --array_index $PJM_BULKNUM


