#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=120
#PJM -L elapse=99:00:00
#PJM -j
#PJM -S


source $HOME/venv/c4/bin/activate


## export SSD="" # replace with your working directory
export CC_ID="2025_13"

cd $SSD 
cd c4-dataset-script/c4_dataset_script


# rm -rf 2_repetition_removal
# mkdir -p 2_repetition_removal

rm -rf 3_remove_duplicate_lines
mkdir -p 3_remove_duplicate_lines

rm -rf 4_remove_duplicate_docs
mkdir -p 4_remove_duplicate_docs

# for i in {0..17}
# do
#    echo "Processing chunk $i"

#    spark-submit --master local[120] --driver-memory 12g \
#        --conf spark.local.dir=$SSD/spark_tmp \
#        Chinese/repetition_removal.py \
#        --input 1_download_${CC_ID}/clean_docs_${i}.jsonl \
#        --output 2_repetition_removal/repetition_removed_${i}  

#    echo "Finished processing chunk $i"
# done


spark-submit --driver-memory 64g --master local[120] --executor-memory 16G \
    --conf spark.local.dir=$SSD/spark_tmp \
    Chinese/remove_duplicate_text.py \
        --input 2_repetition_removal \
        --output 3_remove_duplicate_lines \
        --array_index_upper_bound 17



spark-submit --driver-memory 96g --master local[120] --executor-memory 32G \
    --conf spark.local.dir=$SSD/spark_tmp \
    Chinese/remove_duplicate_minhash.py \
        --input 3_remove_duplicate_lines/clean_docs \
        --output 4_remove_duplicate_docs \

mkdir -p 6_filter_cantonese/${CC_ID}

spark-submit  --driver-memory 32g --master local[120] --executor-memory 8G \
    --conf spark.local.dir=$SSD/spark_tmp/ \
    Chinese/filter_cantonese.py \
    --input 4_remove_duplicate_docs/unique_docs \
    --output 5_filter_cantonese/${CC_ID} \
    --split


cd ${SSD}/c4-dataset-script/huggingface


mkdir -p cantonese/${CC_ID}
cp ${SSD}/c4-dataset-script/c4_dataset_script/5_filter_cantonese/${CC_ID}/*.parquet cantonese/${CC_ID}/

mkdir -p zh_cleaned/${CC_ID}

python3.11 convert_from_part.py \
    --input_path ${SSD}/c4-dataset-script/c4_dataset_script/4_remove_duplicate_docs/unique_docs \
    --output_path zh_cleaned/${CC_ID} \
    --num_parquet 8 \
    --output_file ${CC_ID}_C4_Traditional_Chinese \



