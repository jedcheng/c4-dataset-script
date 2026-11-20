#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j
# Upload <INPUT_DIR>/<CC_ID>/*.parquet to a HF dataset repo, one commit per dump,
# rewritten with the common schema (url, timestamp, content_language, content_type, text).
#
#   pjsub -x REPO_ID=jed351/Cantonese_Common_Crawl_Filtered,INPUT_DIR=.../per_dump/cantonese,STAGING=... \
#       -o $STAGING/job.%j.out bash_script/upload_parquet.sh
# Resumable: dumps with $STAGING/done/<CC_ID> are skipped.
: "${REPO_ID:?}" "${INPUT_DIR:?}" "${STAGING:?}"
MSG="{cc}: 2026 download and cleaning"
mkdir -p $STAGING
source $HOME/venv/c4/bin/activate
cd ${REPO:-/fast/pj24001684/c4-dataset-script}
python huggingface/upload_not_cleaned.py --source parquet --repo_id $REPO_ID --input_dir $INPUT_DIR \
    --staging $STAGING --workers 8 --commit_message "$MSG" >> $STAGING/upload.log 2>&1
