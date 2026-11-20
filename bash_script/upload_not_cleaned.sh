#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j
#PJM -o /home/pj24001684/ku40000295/jc/projects/CommonCrawl_hf_staging/job.%j.out
# Convert + upload the uncleaned dumps to HF (resumable; see huggingface/upload_not_cleaned.py).
# Re-downloads (v2):
#   pjsub -x STAGING=.../CommonCrawl_hf_staging_v2,DOWNLOAD_DIR=.../CommonCrawl_v2,DUMPS_FILE=<list> bash_script/upload_not_cleaned.sh
MSG="{cc}: replace with 2026 download"
S=${STAGING:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl_hf_staging}
source $HOME/venv/c4/bin/activate
cd /fast/pj24001684/c4-dataset-script
python huggingface/upload_not_cleaned.py --workers 8 --staging $S \
    --download_dir ${DOWNLOAD_DIR:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl} \
    --dumps_file ${DUMPS_FILE:-bash_script/dumps.txt} \
    --commit_message "${COMMIT_MESSAGE:-$MSG}" >> $S/upload.log 2>&1
