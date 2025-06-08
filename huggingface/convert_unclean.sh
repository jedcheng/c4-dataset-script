#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j
#PJM -S

source $HOME/venv/c4/bin/activate



export CC_ID="2024_33"
mkdir -p ${SSD}/c4-dataset-script/huggingface/zh_uncleaned/${CC_ID}


python3.11 convert.py  \
    --input_file ${SSD}/c4-dataset-script/c4_dataset_script/1_download_${CC_ID}/clean_docs_${PJM_BULKNUM}.jsonl \
    --output_path ${SSD}/c4-dataset-script/huggingface/zh_uncleaned/${CC_ID} \
    --output_file ${CC_ID}_CC_zh_not_cleaned \
    --number ${PJM_BULKNUM} \
    --total_no_files 18
