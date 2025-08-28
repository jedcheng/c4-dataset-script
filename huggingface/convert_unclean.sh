#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=4
#PJM -L elapse=99:00:00
#PJM -j
##PJM -S

source $HOME/venv/c4/bin/activate



# export CC_ID="2021_49"
# export total_no_files=19

# mkdir -p ${SSD}/c4-dataset-script/huggingface/zh_uncleaned/${CC_ID}




# for ((PJM_BULKNUM=0; PJM_BULKNUM<=total_no_files; PJM_BULKNUM++))
# do
#     python3.11 convert.py  \
#         --input_file ${SSD}/c4-dataset-script/c4_dataset_script/1_download_${CC_ID}/clean_docs_${PJM_BULKNUM}.jsonl \
#         --output_path ${SSD}/c4-dataset-script/huggingface/zh_uncleaned/${CC_ID} \
#         --output_file ${CC_ID}_CC_zh_not_cleaned \
#         --number ${PJM_BULKNUM} \
#         --total_no_files $((total_no_files + 1)) &
# done

# wait


python3.11 automate_conversion.py \
    --year 2018 \
    --base_path ${SSD}/c4-dataset-script/c4_dataset_script \
    --output_base ${SSD}/c4-dataset-script/huggingface \


# Usage examples for the automated conversion script

# Example 1: Generate bash scripts for all dumps in 2021
# echo "=== Generating bash scripts for all 2021 dumps ==="
# python3 automate_conversion.py --year 2021

# # Example 2: Process all dumps sequentially with direct Python execution
# echo "=== Processing all 2021 dumps sequentially ==="
# python3 automate_conversion.py --year 2021 --sequential --max_workers 4

# # Example 3: Process only a specific dump
# echo "=== Processing only dump 2021_49 ==="
# python3 automate_conversion.py --year 2021 --dump_id 2021_49 --sequential

# # Example 4: Generate script for a specific dump
# echo "=== Generating script for dump 2021_49 ==="
# python3 automate_conversion.py --year 2021 --dump_id 2021_49

# # Example 5: Custom paths
# echo "=== Using custom paths ==="
# python3 automate_conversion.py --year 2021 \
#     --base_path /custom/path/to/c4-dataset-script \
#     --output_base /custom/output/path \
#     --venv_path /custom/venv/path \
#     --sequential
