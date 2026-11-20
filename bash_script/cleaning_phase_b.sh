#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=16
#PJM -L elapse=04:00:00
#PJM -j
#
# Phase B, one dump per subjob: drop texts that also appear in a later dump
# (stage 4b), extract the Cantonese subset (stage 5), write parquet.
# Needs phase A finished for every dump in DUMPS_FILE.
#
# SCOPE names a separate output set, deduplicated only among the dumps in
# DUMPS_FILE (both default to all 126 dumps):
#
#   pjsub --bulk --sparam 1-126 bash_script/cleaning_phase_b.sh
#   pjsub --bulk --sparam 1-22 -x SCOPE=2025_2026,DUMPS_FILE=/fast/pj24001684/c4-dataset-script/bash_script/dumps_2025_2026.txt \
#       bash_script/cleaning_phase_b.sh
#
# Output: $OUT_ROOT/<SCOPE>/{zh_cleaned,cantonese}/<CC_ID>/
# Log: $OUT_ROOT/logs/<CC_ID>.b.<SCOPE>.log

source ${REPO:-/fast/pj24001684/c4-dataset-script}/bash_script/cleaning_env.sh
SCOPE=${SCOPE:-all}
OUT=$OUT_ROOT/$SCOPE
DONE=$OUT_ROOT/done/${CC_ID}.b.${SCOPE}
# Skip before the log redirect, so a finished dump's log is not overwritten.
if [ -f $DONE ]; then echo "${CC_ID}: phase B ($SCOPE) already done"; exit 0; fi
exec > >(tee $OUT_ROOT/logs/${CC_ID}.b.${SCOPE}.log) 2>&1

echo "${CC_ID}: phase B ($SCOPE, dumps from $DUMPS_FILE) on $(hostname), $CORES cores, $(date)"

# Stage 4b: exact-duplicate removal; the latest dump containing a text keeps it
python3 Chinese/remove_cross_dump_duplicates.py filter \
    --input $OUT_ROOT/4_remove_duplicate_docs/${CC_ID}/unique_docs \
    --output $LOCAL/4b_cross_dump_dedup \
    --hash_dir $OUT_ROOT/hashes \
    --dumps_file $DUMPS_FILE \
    --cc_id ${CC_ID}

# Stage 5: Cantonese subset
mkdir -p $LOCAL/5_filter_cantonese
spark-submit $SPARK_OPTS --driver-memory 32g \
    Chinese/filter_cantonese.py \
        --input $LOCAL/4b_cross_dump_dedup \
        --output $LOCAL/5_filter_cantonese \
        --split

rm -rf $OUT/cantonese/${CC_ID} $OUT/zh_cleaned/${CC_ID}
mkdir -p $OUT/cantonese/${CC_ID} $OUT/zh_cleaned/${CC_ID}
cp $LOCAL/5_filter_cantonese/*.parquet $OUT/cantonese/${CC_ID}/

python3 $REPO/huggingface/convert_from_part.py \
    --input_path $LOCAL/4b_cross_dump_dedup \
    --output_path $OUT/zh_cleaned/${CC_ID} \
    --num_parquet 8 \
    --output_file ${CC_ID}_C4_Traditional_Chinese

rm -rf $LOCAL
touch $DONE
echo "${CC_ID}: phase B done $(date)"
