#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L node=1
#PJM -L elapse=08:00:00
#PJM -j
#
# Phase A, one dump per subjob: stages 2-4, then the text hashes for stage 4b.
#
#   pjsub --bulk --sparam 1-126 bash_script/cleaning_phase_a.sh
#
# Dumps that already finished are skipped, so resubmitting the same range only
# reruns failed or unfinished ones. Log: $OUT_ROOT/logs/<CC_ID>.a.log

source ${REPO:-/fast/pj24001684/c4-dataset-script}/bash_script/cleaning_env.sh
exec > >(tee $OUT_ROOT/logs/${CC_ID}.a.log) 2>&1

if [ -f $OUT_ROOT/done/${CC_ID}.a ]; then echo "${CC_ID}: phase A already done"; exit 0; fi
STAGE4=$OUT_ROOT/4_remove_duplicate_docs/${CC_ID}
rm -rf $STAGE4
echo "${CC_ID}: phase A on $(hostname), $CORES cores, $(date)"

# Stage 2: Gopher repetition removal
spark-submit $SPARK_OPTS --driver-memory 32g \
    Chinese/repetition_removal.py \
        --input "$DOWNLOAD_DIR/1_download_${CC_ID}/chunk_*.jsonl.gz" \
        --output $LOCAL/2_repetition_removal

# Stage 3: C4 line deduplication within the dump
spark-submit $SPARK_OPTS --driver-memory 160g \
    Chinese/remove_duplicate_text.py \
        --input $LOCAL/2_repetition_removal/clean_docs \
        --output $LOCAL/3_remove_duplicate_lines
rm -rf $LOCAL/2_repetition_removal

# Stage 4: MinHash near-duplicate removal within the dump
spark-submit $SPARK_OPTS --driver-memory 160g \
    Chinese/remove_duplicate_minhash.py \
        --input $LOCAL/3_remove_duplicate_lines/clean_docs \
        --output $STAGE4
rm -rf $LOCAL

# Text hashes, used by phase B to drop texts that reappear in later dumps
python3 Chinese/remove_cross_dump_duplicates.py hash \
    --input $STAGE4/unique_docs \
    --hash_dir $OUT_ROOT/hashes \
    --cc_id ${CC_ID}

touch $OUT_ROOT/done/${CC_ID}.a
echo "${CC_ID}: phase A done $(date)"
