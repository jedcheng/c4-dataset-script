#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L node=2
#PJM -L elapse=02:00:00
#PJM -j
#PJM -S

### Examples to create a spark cluster in an MPI cluster


module load intel
module load impi

source $HOME/venv/c4/bin/activate



##MASTER_ADDR=$(head -n 1 "$PJM_O_NODEINF")
##cat $MASTER_ADDR


mpirun bash $SSD/spark-3.5.5-bin-hadoop3/sbin/start-worker.sh spark://172.16.10.45:7077 --cores 16 --work-dir $SSD/spark_tmp 

mpirun sleep 2h