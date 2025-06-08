#!/bin/bash
#PJM -L rscgrp=a-batch
#PJM -L vnode-core=4
#PJM -L elapse=99:00:00
#PJM -j

### Examples to create a spark cluster in an MPI cluster

source $HOME/venv/c4/bin/activate



MASTER_ADDR=$(head -n 1 "$PJM_O_NODEINF")
echo $MASTER_ADDR

bash $SSD/spark-3.5.5-bin-hadoop3/sbin/start-master.sh --host $MASTER_ADDR 

sleep 90h