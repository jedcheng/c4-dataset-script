#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=16
#PJM -L elapse=02:00:00
#PJM -j
#
# Cantonese subset (stage 5) of each dump WITHOUT cross-dump dedup: runs on the
# phase A output, like the jed351/Cantonese_Common_Crawl_Filtered repo.
#
#   pjsub --bulk --sparam 1-126 -x SCRATCH_ROOT=... bash_script/cantonese_per_dump.sh
#
# Output: $OUT_ROOT/per_dump/cantonese/<CC_ID>/

source ${REPO:-/fast/pj24001684/c4-dataset-script}/bash_script/cleaning_env.sh
DONE=$OUT_ROOT/done/${CC_ID}.c
if [ -f $DONE ]; then echo "${CC_ID}: per-dump Cantonese already done"; exit 0; fi
exec > >(tee $OUT_ROOT/logs/${CC_ID}.c.log) 2>&1
[ -f $OUT_ROOT/done/${CC_ID}.a ] || { echo "${CC_ID}: phase A not finished"; exit 1; }

OUT=$OUT_ROOT/per_dump/cantonese/${CC_ID}
rm -rf $OUT; mkdir -p $OUT
spark-submit $SPARK_OPTS --driver-memory 32g \
    Chinese/filter_cantonese.py \
        --input $OUT_ROOT/4_remove_duplicate_docs/${CC_ID}/unique_docs \
        --output $OUT \
        --split
rm -f $OUT/*.jsonl
rm -rf $LOCAL
touch $DONE
echo "${CC_ID}: per-dump Cantonese done $(date)"
