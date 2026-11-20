#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=60
#PJM -L elapse=04:00:00
#PJM -j
#
# MinHash near-duplicate removal ACROSS dumps for a phase B output set
# (Chinese/global_minhash.py). Three steps, each submitted after the previous one:
#
#   S="-x STEP=sign"   ; pjsub --bulk --sparam 1-22 $S bash_script/global_minhash.sh
#   S="-x STEP=edges"  ; pjsub --bulk --sparam 1-1  $S bash_script/global_minhash.sh
#   S="-x STEP=filter" ; pjsub --bulk --sparam 1-22 $S bash_script/global_minhash.sh
#
# Defaults: the 2025_2026 set. Output: $OUT_ROOT/$SCOPE/zh_cleaned_minhash/<CC_ID>/
set -euo pipefail
REPO=${REPO:-/fast/pj24001684/c4-dataset-script}
OUT_ROOT=${OUT_ROOT:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl_cleaned}
SCOPE=${SCOPE:-2025_2026}
DUMPS_FILE=${DUMPS_FILE:-$REPO/bash_script/dumps_2025_2026.txt}
IN=$OUT_ROOT/$SCOPE/zh_cleaned
WORK=$OUT_ROOT/$SCOPE/global_minhash
OUT=$OUT_ROOT/$SCOPE/zh_cleaned_minhash
CC_ID=$(sed -n "${PJM_BULKNUM}p" $DUMPS_FILE)
mkdir -p $WORK/logs

source $HOME/venv/c4/bin/activate
cd $REPO/c4_dataset_script
case ${STEP:?sign, edges or filter} in
    sign)   [ -e $WORK/sig/$CC_ID.npy ] && exit 0
            python Chinese/global_minhash.py sign --input_dir $IN --work $WORK --cc_id $CC_ID \
                --workers $(nproc) > $WORK/logs/sign.$CC_ID.log 2>&1 ;;
    edges)  python Chinese/global_minhash.py edges --work $WORK --dumps_file $DUMPS_FILE \
                > $WORK/logs/edges.log 2>&1 ;;
    filter) python Chinese/global_minhash.py filter --input_dir $IN --work $WORK --cc_id $CC_ID \
                --output_dir $OUT > $WORK/logs/filter.$CC_ID.log 2>&1 ;;
esac
