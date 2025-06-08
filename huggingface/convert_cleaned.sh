#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j
#PJM -S

source $HOME/venv/c4/bin/activate



# python3.11 convert.py  \
#     --input_file ${SSD}/c4-dataset-script/c4_dataset_script/4_repetition_removal/clean_docs_${PJM_BULKNUM}.jsonl \
#     --output_path ${SSD}/c4-dataset-script/huggingface/zh_cc \
#     --output_file C4_Chinese \
#     --number ${PJM_BULKNUM} \
#     --total_no_files 8


export CC_ID="2025_05"
mkdir -p zh_cleaned/${CC_ID}


python3.11 convert_from_part.py \
    --input_path ${SSD}/c4-dataset-script/c4_dataset_script/4_remove_duplicate_docs/unique_docs \
    --output_path zh_cleaned/${CC_ID} \
    --num_parquet 8 \
    --output_file ${CC_ID}_C4_Traditional_Chinese \