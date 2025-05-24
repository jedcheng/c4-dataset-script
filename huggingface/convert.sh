#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j


source $HOME/venv/c4/bin/activate



python3.11 convert.py  \
    --input_file ${SSD}/c4-dataset-script/c4_dataset_script/5_repetition_removal/removed_repetition_${PJM_BULKNUM}/clean_docs.jsonl \
    --output_path ${SSD}/c4-dataset-script/huggingface/zh_cc \
    --output_file C4_Chinese \
    --number ${PJM_BULKNUM} \
    --total_no_files 20 
