#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=60
#PJM -L elapse=08:00:00
#PJM -j
#
# cleaning_phase_a.sh on half a node, for when no whole node is free. Partial
# nodes have a small hidden /tmp quota, so Spark scratch goes to $SCRATCH_ROOT
# (/home: HDD, slower, but /fast has no room for this).
#
#   pjsub --bulk --sparam 104-104 bash_script/cleaning_phase_a_shared.sh

SCRATCH_ROOT=${SCRATCH_ROOT:-/home/pj24001684/ku40000295/jc/projects/clean_scratch}
export TMPDIR=$SCRATCH_ROOT/$(echo ${PJM_JOBID}_${PJM_BULKNUM} | tr -d "[]")
mkdir -p $TMPDIR
trap 'rm -rf $TMPDIR' EXIT
bash ${REPO:-/fast/pj24001684/c4-dataset-script}/bash_script/cleaning_phase_a.sh
