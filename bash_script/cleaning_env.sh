# Shared settings for cleaning_phase_a.sh / cleaning_phase_b.sh (sourced, not run).
#
# Each bulk subjob handles one dump: line $PJM_BULKNUM of dumps.txt (126 dumps,
# chronological). Large intermediates live on the node-local disk ($TMPDIR,
# ~118 GB, wiped at job end); only final outputs are written to OUT_ROOT.

set -euo pipefail

REPO=${REPO:-/fast/pj24001684/c4-dataset-script}
DOWNLOAD_DIR=${DOWNLOAD_DIR:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl}
OUT_ROOT=${OUT_ROOT:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl_cleaned}
DUMPS_FILE=${DUMPS_FILE:-$REPO/bash_script/dumps.txt}

CC_ID=${CC_ID:-$(sed -n "${PJM_BULKNUM:?run as a bulk job or set CC_ID}p" $DUMPS_FILE)}
: "${CC_ID:?no dump on line $PJM_BULKNUM of $DUMPS_FILE}"

# Prefer a finished re-download (fixed download filters) over the original one.
# Only for 2026 dumps this run; earlier dumps keep the original download.
DOWNLOAD_V2_DIR=${DOWNLOAD_V2_DIR:-/home/pj24001684/ku40000295/jc/projects/CommonCrawl_v2}
if [[ $CC_ID == 2026_* ]] && [ -e $DOWNLOAD_V2_DIR/1_download_${CC_ID}/_COMPLETE ]; then
    DOWNLOAD_DIR=$DOWNLOAD_V2_DIR
fi

CORES=$(nproc)
# Partial-node jobs have a small hidden quota on /tmp: set SCRATCH_ROOT (e.g. on
# /home) to put the local scratch there instead.
if [ -n "${SCRATCH_ROOT:-}" ]; then
    TMPDIR=$SCRATCH_ROOT/$(echo ${PJM_JOBID:-$$}_${PJM_BULKNUM:-0} | tr -d '[]')
    mkdir -p $TMPDIR
fi
# Bulk subjobs get TMPDIR=/tmp/<jobid>[<n>]_..., and Spark reads "[n]" in a path
# as a glob. Work through a bracket-free symlink to it instead.
LOCAL_LINK=/tmp/clean_${USER}_${CC_ID}_$$
ln -sfn ${TMPDIR:-/tmp} $LOCAL_LINK
trap 'rm -f $LOCAL_LINK' EXIT
LOCAL=$LOCAL_LINK/clean_${CC_ID}
SPARK_OPTS="--master local[$CORES] --conf spark.local.dir=$LOCAL/spark"

mkdir -p $OUT_ROOT/{logs,done,hashes,4_remove_duplicate_docs}
rm -rf $LOCAL
mkdir -p $LOCAL/spark

source $HOME/venv/c4/bin/activate
cd $REPO/c4_dataset_script
