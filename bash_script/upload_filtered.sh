#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=8
#PJM -L elapse=99:00:00
#PJM -j
#PJM -o /home/pj24001684/ku40000295/jc/projects/CommonCrawl_hf_staging_filtered/job.%j.out
# Upload phase A output (stage 4) to the Filtered repo. Dumps whose phase A is
# still running are retried every 10 minutes until every dump is uploaded.
S=/home/pj24001684/ku40000295/jc/projects/CommonCrawl_hf_staging_filtered
DUMPS=/fast/pj24001684/c4-dataset-script/bash_script/dumps.txt
source $HOME/venv/c4/bin/activate
cd /fast/pj24001684/c4-dataset-script
while :; do
    python huggingface/upload_not_cleaned.py --source phase_a --staging $S --workers 8 \
        --commit_message "{cc}: replace with 2026 download, cleaned with stages 2-4" >> $S/upload.log 2>&1
    left=$(for d in $(cat $DUMPS); do [ -e $S/done/$d ] || echo $d; done | wc -l)
    [ $left -eq 0 ] && break
    echo "$(date): $left dumps not uploaded yet, retrying in 10 min" >> $S/upload.log
    sleep 600
done
