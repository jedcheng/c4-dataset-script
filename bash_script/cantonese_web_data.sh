#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=60
#PJM -L elapse=12:00:00
#PJM -j
#
# Cantonese-Web-Data: the per-dump Cantonese subsets (cantonese_per_dump.sh) of all
# dumps, text-cleaned and MinHash-deduplicated (threshold 0.3) across dumps.
#   subjob 1: no_quotes_domain_cleaned,  subjob 2: with_quotes_domain_cleaned
#
#   pjsub --bulk --sparam 1-2 -o $WORK/job.%j.%b.out bash_script/cantonese_web_data.sh
#
# KEEP=oldest keeps the earliest crawl of each group of near-duplicates instead of the latest
# (e.g. for studying language change over time): -x KEEP=oldest,WORK=<another dir>
#
# Output: $WORK/hf/{no_quotes,with_quotes}/2013_2026_common_crawl.parquet

set -euo pipefail
REPO=${REPO:-/fast/pj24001684/c4-dataset-script}
INPUT=${INPUT:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl_cleaned/per_dump/cantonese}
WORK=${WORK:-/home/pj24001684/ku40000295/pj24001684/c4_cantonese/web_data_2026}
DUMPS_FILE=${DUMPS_FILE:-$REPO/bash_script/dumps.txt}
NAME=${NAME:-2013_2026_common_crawl}
KEEP=${KEEP:-newest}

SUBSET=$(sed -n "${PJM_BULKNUM}p" <<< $'no_quotes_domain_cleaned\nwith_quotes_domain_cleaned')
VARIANT=${SUBSET%_domain_cleaned}
W=$WORK/$SUBSET
OUT=$WORK/hf/$VARIANT/$NAME.parquet
CORES=$(nproc)

source $HOME/venv/c4/bin/activate
cd $REPO/c4_dataset_script
rm -rf $W; mkdir -p $W/spark

python Chinese/cantonese_web_data.py prep --input $INPUT --subset $SUBSET \
    --dumps_file $DUMPS_FILE --output $W/input --keep $KEEP
spark-submit --master local[$CORES] --driver-memory $((CORES * 3))g \
    --conf spark.local.dir=$W/spark --conf spark.driver.maxResultSize=0 \
    Chinese/remove_duplicate_minhash.py --input $W/input --output $W/minhash --threshold 0.3
python Chinese/cantonese_web_data.py finalize --input $W/minhash/unique_docs --output $OUT
rm -rf $W/spark
echo "$SUBSET done $(date)"
