#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=120
#PJM -L elapse=99:00:00
#PJM -j
#PJM -S

# Clean one Common Crawl dump: stages 2-5, then the Hugging Face parquet export.
# Stage 1 (download + filtering) is done by distributed/wet_worker.py.
#
#   pjsub -x CC_ID=2016_30 bash_script/all_cleaning_pipeline.sh
#
# Run dumps one at a time: stage 4b reads and updates a shared index.

set -euo pipefail

: "${CC_ID:?set CC_ID, e.g. pjsub -x CC_ID=2016_30}"
: "${SSD:?set SSD to the working directory that contains c4-dataset-script}"
DOWNLOAD_DIR=${DOWNLOAD_DIR:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl}
CORES=${CORES:-120}

source $HOME/venv/c4/bin/activate

REPO=$SSD/c4-dataset-script
WORK=$REPO/c4_dataset_script/work/${CC_ID}
INDEX_DIR=$REPO/c4_dataset_script/cross_dump_index
SPARK_TMP=$SSD/spark_tmp/${CC_ID}

cd $REPO/c4_dataset_script
rm -rf $WORK $SPARK_TMP
mkdir -p $WORK $SPARK_TMP

# Stage 2: Gopher repetition removal
spark-submit --master local[$CORES] --driver-memory 32g \
    --conf spark.local.dir=$SPARK_TMP \
    Chinese/repetition_removal.py \
        --input "$DOWNLOAD_DIR/1_download_${CC_ID}/chunk_*.jsonl.gz" \
        --output $WORK/2_repetition_removal

# Stage 3: C4 line deduplication within the dump
spark-submit --master local[$CORES] --driver-memory 64g --executor-memory 16G \
    --conf spark.local.dir=$SPARK_TMP \
    Chinese/remove_duplicate_text.py \
        --input $WORK/2_repetition_removal/clean_docs \
        --output $WORK/3_remove_duplicate_lines

# Stage 4: MinHash near-duplicate removal within the dump
spark-submit --master local[$CORES] --driver-memory 96g --executor-memory 32G \
    --conf spark.local.dir=$SPARK_TMP \
    Chinese/remove_duplicate_minhash.py \
        --input $WORK/3_remove_duplicate_lines/clean_docs \
        --output $WORK/4_remove_duplicate_docs

# Stage 4b: exact-duplicate removal against all previously processed dumps
python3 Chinese/remove_cross_dump_duplicates.py \
    --input $WORK/4_remove_duplicate_docs/unique_docs \
    --output $WORK/4b_cross_dump_dedup \
    --index_dir $INDEX_DIR \
    --cc_id $CC_ID

# Stage 5: Cantonese subset
mkdir -p $WORK/5_filter_cantonese
spark-submit --master local[$CORES] --driver-memory 32g --executor-memory 8G \
    --conf spark.local.dir=$SPARK_TMP \
    Chinese/filter_cantonese.py \
        --input $WORK/4b_cross_dump_dedup \
        --output $WORK/5_filter_cantonese \
        --split

# Hugging Face export
cd $REPO/huggingface

mkdir -p cantonese/${CC_ID}
cp $WORK/5_filter_cantonese/*.parquet cantonese/${CC_ID}/

mkdir -p zh_cleaned/${CC_ID}
python3 convert_from_part.py \
    --input_path $WORK/4b_cross_dump_dedup \
    --output_path zh_cleaned/${CC_ID} \
    --num_parquet 8 \
    --output_file ${CC_ID}_C4_Traditional_Chinese

rm -rf $SPARK_TMP
echo "Done: ${CC_ID}"
