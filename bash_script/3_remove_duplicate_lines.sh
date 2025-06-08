#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=120
#PJM -L elapse=99:00:00
#PJM -j
#PJM -S


source $HOME/venv/c4/bin/activate
 

cd $SSD
cd c4-dataset-script/c4_dataset_script



spark-submit --driver-memory 32g --master local[120] --executor-memory 4G \
    --conf spark.local.dir=$SSD/spark_tmp \
    Chinese/remove_duplicate_text.py \
        --input 2_repetition_removal \
        --output 3_remove_duplicate_lines \



